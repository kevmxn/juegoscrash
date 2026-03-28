#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para CRASH con servidor HTTP y WebSocket
- Polling a la API de Stake Crash con headers sin Brotli
- Envía historial a clientes WebSocket en fragmentos (500 por lote)
- Broadcast de nuevos eventos de Crash en lotes (máx 300)
- Backoff exponencial y circuit breaker
- Auto‑ping cada 10 minutos para evitar que Render suspenda el servicio
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import random
import logging
import os
from datetime import datetime
from typing import Set, Dict, Any, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURACIÓN CRASH
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.76',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0',
    'Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TIME = 300

crash_ids: Set[str] = set()
crash_status = {'consecutive_errors': 0, 'next_allowed_time': 0, 'blocked_until': 0}
crash_history: List[Dict] = []
MAX_HISTORY = 15000

connected_clients: Set[web.WebSocketResponse] = set()
client_semaphore = asyncio.Semaphore(10)  # Max 10 concurrent clients

# Batching settings for new events
BATCH_SIZE = 300
BATCH_FLUSH_INTERVAL = 2.0
event_batch: List[Dict] = []
batch_lock = asyncio.Lock()

# History chunking settings
HISTORY_CHUNK_SIZE = 500

# ============================================
# AUTO‑PING PARA MANTENER EL SERVICIO ACTIVO
# ============================================
async def self_ping():
    """Hace una petición a /health cada 10 minutos para evitar que Render suspenda el servicio."""
    port = int(os.environ.get('PORT', 10000))
    url = f"http://localhost:{port}/health"
    while True:
        await asyncio.sleep(600)  # 10 minutos
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=5) as resp:
                    if resp.status == 200:
                        logger.info("[PING] Auto‑ping exitoso, servicio activo")
                    else:
                        logger.warning(f"[PING] Auto‑ping falló con código {resp.status}")
        except Exception as e:
            logger.error(f"[PING] Error en auto‑ping: {e}")

# ============================================
# FUNCIONES CRASH
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_crash(session: aiohttp.ClientSession) -> dict | None:
    now = time.time()
    if now < crash_status['blocked_until']:
        wait = crash_status['blocked_until'] - now
        logger.info(f"[CRASH] 🚫 Bloqueado por {wait:.1f}s")
        await asyncio.sleep(wait)
        return None
    if now < crash_status['next_allowed_time']:
        wait = crash_status['next_allowed_time'] - now
        logger.info(f"[CRASH] ⏳ Backoff {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    try:
        async with session.get(API_CRASH, headers=headers, timeout=10) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                crash_status['next_allowed_time'] = time.time() + retry_after
                crash_status['consecutive_errors'] += 1
                logger.warning(f"[CRASH] ⚠️ Esperar {retry_after}s (Retry-After)")
                return None
            if resp.status == 200:
                crash_status['consecutive_errors'] = 0
                return await resp.json()
            elif resp.status == 403:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                logger.warning(f"[CRASH] 🚫 403 Forbidden - backoff {backoff:.1f}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.error(f"[CRASH] 🔒 Bloqueado {BLOCK_TIME}s")
                return None
            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + retry_after
                crash_status['consecutive_errors'] += 1
                logger.warning(f"[CRASH] ⚠️ Rate limit, esperar {retry_after}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
            elif 500 <= resp.status < 600:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                logger.error(f"[CRASH] ❌ Error {resp.status}, backoff {backoff:.1f}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
            else:
                logger.warning(f"[CRASH] ⚠️ Código inesperado: {resp.status}")
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
    except asyncio.TimeoutError:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[CRASH] ⏰ Timeout, backoff {backoff:.1f}s")
        if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            crash_status['blocked_until'] = time.time() + BLOCK_TIME
        return None
    except Exception as e:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[CRASH] 💥 Excepción: {e}")
        if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            crash_status['blocked_until'] = time.time() + BLOCK_TIME
        return None

async def procesar_crash(data: dict):
    global crash_history
    event_id = data.get('id')
    if not event_id or event_id in crash_ids:
        return
    crash_ids.add(event_id)
    data_inner = data.get('data', {})
    result = data_inner.get('result', {})
    max_mult = result.get('maxMultiplier')
    round_dur = result.get('roundDuration')
    started_at = data_inner.get('startedAt')
    if max_mult is not None and max_mult > 0:
        evento = {
            'event_id': event_id,
            'maxMultiplier': max_mult,
            'roundDuration': round_dur,
            'startedAt': started_at,
            'timestamp_recepcion': datetime.now().isoformat()
        }
        crash_history.insert(0, evento)
        if len(crash_history) > MAX_HISTORY:
            crash_history.pop()
        logger.info(f"[CRASH] ✅ NUEVO: ID={event_id} | {max_mult}x | Duración={round_dur}s | Inicio={started_at}")

        # Add to batch instead of broadcasting immediately
        await add_to_batch({
            'tipo': 'crash',
            'id': event_id,
            'maxMultiplier': max_mult,
            'roundDuration': round_dur,
            'startedAt': started_at,
            'timestamp_recepcion': evento['timestamp_recepcion']
        })
    else:
        logger.warning(f"[CRASH] ⚠️ ID {event_id} mult inválido: {max_mult}")

async def monitor_crash():
    logger.info("[CRASH] 🚀 Iniciando monitor")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_crash(session)
            if data:
                await procesar_crash(data)
            await asyncio.sleep(random.uniform(0.5, 1.5))

# ============================================
# BATCH MANAGEMENT (for new events)
# ============================================
async def add_to_batch(event: Dict[str, Any]):
    global event_batch
    async with batch_lock:
        event_batch.append(event)
        if len(event_batch) >= BATCH_SIZE:
            await flush_batch()

async def flush_batch():
    global event_batch
    async with batch_lock:
        if not event_batch:
            return
        batch_copy = event_batch[:]
        event_batch.clear()
    if not connected_clients:
        return
    message = json.dumps({
        'tipo': 'batch',
        'api': 'crash',
        'eventos': batch_copy
    }, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )
    logger.debug(f"Batch flusheado: {len(batch_copy)} eventos")

async def batch_flusher():
    while True:
        await asyncio.sleep(BATCH_FLUSH_INTERVAL)
        await flush_batch()

# ============================================
# HISTORY CHUNKING (for initial client connection)
# ============================================
async def send_history_in_chunks(ws: web.WebSocketResponse, history: List[Dict]):
    """Send the full history in chunks."""
    total = len(history)
    if total == 0:
        await ws.send_json({'tipo': 'historial_start', 'total': 0})
        await ws.send_json({'tipo': 'historial_end'})
        return

    await ws.send_json({'tipo': 'historial_start', 'total': total})

    for i in range(0, total, HISTORY_CHUNK_SIZE):
        chunk = history[i:i + HISTORY_CHUNK_SIZE]
        await ws.send_json({
            'tipo': 'historial_chunk',
            'offset': i,
            'chunk': chunk
        })
        # Small delay to avoid flooding the connection
        await asyncio.sleep(0.01)

    await ws.send_json({'tipo': 'historial_end'})

# ============================================
# SERVIDOR HTTP + WEBSOCKET (para clientes)
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    # Limit concurrent clients
    async with client_semaphore:
        connected_clients.add(ws)
        try:
            # Send history in chunks
            await send_history_in_chunks(ws, crash_history)
            logger.info("Cliente Crash conectado, historial enviado en fragmentos")

            async for msg in ws:
                if msg.type == web.WSMsgType.CLOSE:
                    break
        finally:
            connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(text="Servidor Crash activo. Use /ws para WebSocket o /health para health check.", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/', root_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"✅ Servidor Crash escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor exclusivo de CRASH con WebSocket, batching y fragmentación de historial")
    logger.info("=" * 60)
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_crash()),
        asyncio.create_task(self_ping()),
        asyncio.create_task(batch_flusher()),
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("\n⏹ Deteniendo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
