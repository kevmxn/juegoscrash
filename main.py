#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para CRASH con servidor HTTP y WebSocket
- Polling a la API de Stake Crash con cloudscraper (bypass Cloudflare)
- Almacena hasta 100,000 eventos en SQLite
- Envía solo los últimos 100 eventos al conectar
- Eventos en lotes de hasta 20 cada 1 segundo
- Tabla de niveles enviada cada 60-120 segundos (aleatorio)
- Backoff exponencial y circuit breaker
- Auto‑ping cada 10 minutos
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
from typing import Set, List
from collections import defaultdict
import aiosqlite

# ✅ cloudscraper maneja cookies y JS challenges de Cloudflare
# pip install cloudscraper
try:
    import cloudscraper
    _scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False
        }
    )
    USE_CLOUDSCRAPER = True
except ImportError:
    USE_CLOUDSCRAPER = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

if not USE_CLOUDSCRAPER:
    logger.warning("⚠️  cloudscraper no instalado. Instalar: pip install cloudscraper")

# ============================================
# CONFIGURACIÓN CRASH
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
DB_PATH = "crash_data.db"

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TIME = 300

crash_ids: Set[str] = set()
crash_status = {'consecutive_errors': 0, 'next_allowed_time': 0}
crash_history: list = []
MAX_HISTORY = 100
MAX_STORAGE = 100000

current_level = 0
level_counts = defaultdict(lambda: {'3-4.99': 0, '5-9.99': 0, '10+': 0})

connected_clients: Set[web.WebSocketResponse] = set()

event_queue = asyncio.Queue()
BATCH_SIZE = 20
BATCH_TIMEOUT = 1.0

TABLE_UPDATE_MIN = 60
TABLE_UPDATE_MAX = 120

# ============================================
# BASE DE DATOS
# ============================================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                maxMultiplier REAL,
                roundDuration REAL,
                startedAt TEXT,
                timestamp_recepcion TEXT,
                nivel INTEGER
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS counts (
                level INTEGER,
                range TEXT,
                count INTEGER,
                PRIMARY KEY (level, range)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await db.commit()

async def load_from_db():
    global crash_history, crash_ids, level_counts, current_level
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            'SELECT id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion, nivel '
            'FROM events ORDER BY timestamp_recepcion DESC LIMIT ?', (MAX_HISTORY,)
        ) as cursor:
            rows = await cursor.fetchall()
            crash_history = []
            crash_ids.clear()
            for row in rows:
                crash_history.append({
                    'event_id': row[0], 'maxMultiplier': row[1],
                    'roundDuration': row[2], 'startedAt': row[3],
                    'timestamp_recepcion': row[4], 'nivel': row[5]
                })
                crash_ids.add(row[0])
        async with db.execute('SELECT level, range, count FROM counts') as cursor:
            rows = await cursor.fetchall()
            level_counts.clear()
            for level, rng, cnt in rows:
                level_counts[level][rng] = cnt
        async with db.execute('SELECT value FROM state WHERE key = "current_level"') as cursor:
            row = await cursor.fetchone()
            if row:
                current_level = int(row[0])
            else:
                current_level = 0
                await db.execute('INSERT OR IGNORE INTO state VALUES (?, ?)', ('current_level', '0'))
                await db.commit()

async def save_event(event: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT OR REPLACE INTO events (id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion, nivel)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            event['event_id'], event['maxMultiplier'],
            event.get('roundDuration'), event.get('startedAt'),
            event['timestamp_recepcion'], event['nivel']
        ))
        await db.execute('''
            DELETE FROM events WHERE id NOT IN (
                SELECT id FROM events ORDER BY timestamp_recepcion DESC LIMIT ?
            )
        ''', (MAX_STORAGE,))
        await db.commit()

async def update_count(level: int, range_key: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO counts (level, range, count) VALUES (?, ?, 1)
            ON CONFLICT(level, range) DO UPDATE SET count = count + 1
        ''', (level, range_key))
        await db.commit()

async def update_current_level(level: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO state VALUES (?, ?)', ('current_level', str(level)))
        await db.commit()

# ============================================
# AUTO‑PING
# ============================================
async def self_ping():
    port = int(os.environ.get('PORT', 10000))
    url = f"http://localhost:{port}/health"
    while True:
        await asyncio.sleep(600)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        logger.info("[PING] Auto‑ping exitoso")
                    else:
                        logger.warning(f"[PING] Falló con código {resp.status}")
        except Exception as e:
            logger.error(f"[PING] Error: {e}")

# ============================================
# BATCH SENDER
# ============================================
async def batch_sender():
    pending_events = []
    while True:
        try:
            event = await asyncio.wait_for(event_queue.get(), timeout=BATCH_TIMEOUT)
            pending_events.append(event)
            if len(pending_events) >= BATCH_SIZE:
                await send_batch(pending_events.copy())
                pending_events.clear()
        except asyncio.TimeoutError:
            if pending_events:
                await send_batch(pending_events.copy())
                pending_events.clear()
        except Exception as e:
            logger.error(f"Error en batch_sender: {e}")

async def send_batch(events_list: List[dict]):
    if not connected_clients:
        return
    message = json.dumps({'tipo': 'batch', 'eventos': events_list}, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )
    logger.info(f"Enviado lote de {len(events_list)} eventos")

# ============================================
# PERIODIC TABLE SENDER
# ============================================
async def periodic_table_sender():
    while True:
        interval = random.uniform(TABLE_UPDATE_MIN, TABLE_UPDATE_MAX)
        await asyncio.sleep(interval)
        if not connected_clients:
            continue
        message = json.dumps({
            'tipo': 'nivel_counts',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()}
        }, default=str)
        await asyncio.gather(
            *[client.send_str(message) for client in connected_clients],
            return_exceptions=True
        )
        logger.info(f"Tabla de niveles enviada (intervalo {interval:.1f}s)")

# ============================================
# CONSULTA CRASH
# ============================================
def _registrar_error(descripcion: str):
    crash_status['consecutive_errors'] += 1
    n = crash_status['consecutive_errors']
    backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** n))
    if n >= MAX_CONSECUTIVE_ERRORS:
        crash_status['next_allowed_time'] = time.time() + BLOCK_TIME
        logger.error(f"[CRASH] 🔒 Bloqueado {BLOCK_TIME}s ({n} errores) - {descripcion}")
    else:
        crash_status['next_allowed_time'] = time.time() + backoff
        logger.warning(f"[CRASH] {descripcion} - Backoff {backoff:.1f}s ({n} errores consecutivos)")

def _get_proxies() -> dict | None:
    """
    Lee la URL del proxy desde la variable de entorno PROXY_URL.
    Formato: http://usuario:password@host:puerto
    Ejemplo Webshare: http://user-abc:pass123@proxy.webshare.io:80
    Retorna None si no está configurado (sin proxy).
    """
    proxy_url = os.environ.get('PROXY_URL')
    if proxy_url:
        return {'http': proxy_url, 'https': proxy_url}
    return None

def _hacer_request_sync() -> tuple[int, dict | None]:
    """
    Ejecuta la petición HTTP de forma síncrona con cloudscraper.
    Usa proxy residencial si PROXY_URL está configurado.
    Retorna (status_code, json_data_o_None).
    """
    proxies = _get_proxies()
    try:
        resp = _scraper.get(
            API_CRASH,
            headers={
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://stake.com',
                'Referer': 'https://stake.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'cross-site',
            },
            proxies=proxies,
            timeout=15
        )
        if resp.status_code == 200:
            return 200, resp.json()
        return resp.status_code, None
    except Exception as e:
        logger.debug(f"[CRASH] Error en request: {e}")
        return -1, None

async def _hacer_request_aiohttp(session: aiohttp.ClientSession) -> tuple[int, dict | None]:
    """Fallback con aiohttp si cloudscraper no está disponible."""
    try:
        async with session.get(
            API_CRASH,
            headers={
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://stake.com',
                'Referer': 'https://stake.com/',
            },
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status == 200:
                return 200, await resp.json()
            return resp.status, None
    except Exception:
        return -1, None

async def consultar_crash(aiohttp_session: aiohttp.ClientSession) -> dict | None:
    now = time.time()
    if now < crash_status['next_allowed_time']:
        wait = crash_status['next_allowed_time'] - now
        if wait > 0.5:
            logger.debug(f"[CRASH] ⏳ En backoff, esperando {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    # cloudscraper es síncrono → ejecutar en thread pool para no bloquear el event loop
    if USE_CLOUDSCRAPER:
        loop = asyncio.get_event_loop()
        status, data = await loop.run_in_executor(None, _hacer_request_sync)
    else:
        status, data = await _hacer_request_aiohttp(aiohttp_session)

    if status == 200:
        crash_status['consecutive_errors'] = 0
        return data
    elif status == 403:
        _registrar_error("🚫 403 Forbidden")
    elif status == 429:
        _registrar_error("⚠️ 429 Rate Limit")
    elif 500 <= status < 600:
        _registrar_error(f"❌ Error {status}")
    elif status == -1:
        _registrar_error("💥 Excepción / Timeout")
    else:
        logger.warning(f"[CRASH] ⚠️ Código inesperado: {status}")
    return None

# ============================================
# PROCESAMIENTO
# ============================================
async def procesar_crash(data: dict):
    global current_level, crash_history, level_counts
    event_id = data.get('id')
    if not event_id or event_id in crash_ids:
        return
    crash_ids.add(event_id)
    data_inner = data.get('data', {})
    result = data_inner.get('result', {})
    max_mult = result.get('maxMultiplier')
    round_dur = result.get('roundDuration')
    started_at = data_inner.get('startedAt')

    if max_mult is None or max_mult <= 0:
        logger.warning(f"[CRASH] ⚠️ ID {event_id} mult inválido: {max_mult}")
        return

    current_level += 1 if max_mult >= 2.00 else -1

    range_key = None
    if 3.00 <= max_mult <= 4.99:
        range_key = '3-4.99'
    elif 5.00 <= max_mult <= 9.99:
        range_key = '5-9.99'
    elif max_mult >= 10.00:
        range_key = '10+'

    evento = {
        'tipo': 'crash',
        'event_id': event_id,
        'maxMultiplier': max_mult,
        'roundDuration': round_dur,
        'startedAt': started_at,
        'timestamp_recepcion': datetime.now().isoformat(),
        'nivel': current_level
    }

    crash_history.insert(0, evento)
    if len(crash_history) > MAX_HISTORY:
        crash_history.pop()
    if range_key:
        level_counts[current_level][range_key] += 1

    await save_event(evento)
    if range_key:
        await update_count(current_level, range_key)
    await update_current_level(current_level)

    logger.info(f"[CRASH] ✅ NUEVO: ID={event_id} | {max_mult}x | Duración={round_dur}s | Nivel={current_level}")
    await event_queue.put(evento)

async def monitor_crash():
    modo = 'cloudscraper ✅' if USE_CLOUDSCRAPER else 'aiohttp ⚠️ (instalar cloudscraper)'
    logger.info(f"[CRASH] 🚀 Iniciando monitor | Modo: {modo}")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_crash(session)
            if data:
                await procesar_crash(data)
                await asyncio.sleep(random.uniform(1.5, 2.5))
            else:
                await asyncio.sleep(1)

# ============================================
# SERVIDOR HTTP + WEBSOCKET
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        if crash_history:
            await ws.send_json({'tipo': 'historial', 'api': 'crash', 'eventos': crash_history})
        await ws.send_json({
            'tipo': 'nivel_counts',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()}
        })
        logger.info("Cliente Crash conectado, historial y tabla de niveles enviados")
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.discard(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(
        text="Servidor Crash activo. Use /ws para WebSocket o /health para health check.",
        status=200
    )

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
    logger.info("🚀 Monitor Crash - 100k eventos, envío últimos 100")
    logger.info(f"   CF Bypass: {'cloudscraper ✅' if USE_CLOUDSCRAPER else 'aiohttp ⚠️  → pip install cloudscraper'}")
    proxy_url = os.environ.get('PROXY_URL')
    if proxy_url:
        safe = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
        logger.info(f"   Proxy: ✅ {safe}")
    else:
        logger.warning("   Proxy: ⚠️  No configurado (PROXY_URL vacío) — IP datacenter puede ser bloqueada")
    logger.info("=" * 60)
    await init_db()
    await load_from_db()
    asyncio.create_task(batch_sender())
    asyncio.create_task(periodic_table_sender())
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_crash()),
        asyncio.create_task(self_ping()),
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
