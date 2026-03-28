#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor unificado CRASH + SPACEMAN con servidor HTTP para Render
Incluye logs detallados para depuración.
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import random
import os
import logging
from typing import Set

# ============================================
# CONFIGURACIÓN DE LOGS
# ============================================
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
BLOCK_TIME = 300   # segundos

crash_ids: Set[str] = set()
crash_status = {'consecutive_errors': 0, 'next_allowed_time': 0, 'blocked_until': 0}

# ============================================
# CONFIGURACIÓN SPACEMAN
# ============================================
SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301

BASE_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0

spaceman_last_multiplier: float = None
spaceman_events_seen: Set[str] = set()

# ============================================
# BASE DE DATOS (SQLite)
# ============================================
DB_FILE = 'data/eventos.db'

def init_db():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    import sqlite3
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS eventos
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  api TEXT,
                  event_id TEXT,
                  maxMultiplier REAL,
                  roundDuration REAL,
                  startedAt TEXT,
                  timestamp_recepcion TEXT)''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_api ON eventos (api)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON eventos (timestamp_recepcion)')
    conn.commit()
    conn.close()
    logger.info("Base de datos inicializada en %s", DB_FILE)

def guardar_evento_sync(api: str, event_id: str, maxMultiplier: float, roundDuration: float, startedAt: str) -> str:
    import sqlite3
    from datetime import datetime
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute('''INSERT INTO eventos (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp))
    conn.commit()
    # Mantener solo los últimos MAX_HISTORY (15k)
    c.execute('''DELETE FROM eventos WHERE id IN (
                    SELECT id FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT -1 OFFSET 15000
                )''', (api,))
    conn.commit()
    conn.close()
    return timestamp

async def guardar_evento(api: str, event_id: str, maxMultiplier: float, roundDuration: float, startedAt: str) -> str:
    return await asyncio.to_thread(guardar_evento_sync, api, event_id, maxMultiplier, roundDuration, startedAt)

def obtener_ultimos_eventos_sync(api: str, limite: int = 15000) -> list:
    import sqlite3
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''SELECT api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion
                 FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT ?''', (api, limite))
    filas = c.fetchall()
    conn.close()
    eventos = []
    for fila in filas:
        eventos.append({
            'api': fila[0],
            'event_id': fila[1],
            'maxMultiplier': fila[2],
            'roundDuration': fila[3],
            'startedAt': fila[4],
            'timestamp_recepcion': fila[5]
        })
    return eventos

async def obtener_ultimos_eventos(api: str, limite: int = 15000) -> list:
    return await asyncio.to_thread(obtener_ultimos_eventos_sync, api, limite)

# ============================================
# FUNCIONES CRASH
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_crash(session: aiohttp.ClientSession) -> dict | None:
    now = time.time()

    if now < crash_status['blocked_until']:
        wait = crash_status['blocked_until'] - now
        logger.warning("[CRASH] 🚫 Bloqueado por %.1f s", wait)
        await asyncio.sleep(wait)
        return None

    if now < crash_status['next_allowed_time']:
        wait = crash_status['next_allowed_time'] - now
        logger.info("[CRASH] ⏳ Backoff %.1f s", wait)
        await asyncio.sleep(wait)
        return None

    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }

    try:
        async with session.get(API_CRASH, headers=headers, timeout=10) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                crash_status['next_allowed_time'] = time.time() + retry_after
                crash_status['consecutive_errors'] += 1
                logger.warning("[CRASH] ⚠️ Servidor pide esperar %d s (Retry-After)", retry_after)
                return None

            if resp.status == 200:
                crash_status['consecutive_errors'] = 0
                logger.debug("[CRASH] ✅ Respuesta 200 OK")
                return await resp.json()

            if resp.status == 403:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                logger.error("[CRASH] 🚫 403 Forbidden - backoff %.1f s (error #%d)", backoff, crash_status['consecutive_errors'])
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.critical("[CRASH] 🔒 Bloqueado por %d s por exceso de errores", BLOCK_TIME)
                return None

            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + retry_after
                crash_status['consecutive_errors'] += 1
                logger.warning("[CRASH] ⚠️ Rate limit, esperar %d s (error #%d)", retry_after, crash_status['consecutive_errors'])
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.critical("[CRASH] 🔒 Bloqueado por %d s por exceso de errores", BLOCK_TIME)
                return None

            if 500 <= resp.status < 600:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                logger.error("[CRASH] ❌ Error %d, backoff %.1f s (error #%d)", resp.status, backoff, crash_status['consecutive_errors'])
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                return None

            logger.warning("[CRASH] ⚠️ Código inesperado: %d", resp.status)
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
        logger.error("[CRASH] ⏰ Timeout, backoff %.1f s (error #%d)", backoff, crash_status['consecutive_errors'])
        if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            crash_status['blocked_until'] = time.time() + BLOCK_TIME
        return None
    except Exception as e:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff
        logger.exception("[CRASH] 💥 Excepción: %s", e)
        if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            crash_status['blocked_until'] = time.time() + BLOCK_TIME
        return None

async def procesar_crash(data: dict):
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
        timestamp = await guardar_evento('crash', event_id, max_mult, round_dur, started_at)
        logger.info("[CRASH] ✅ NUEVO: ID=%s | %.2fx | Duración=%s | Inicio=%s", event_id, max_mult, round_dur, started_at)
        # Broadcast a clientes WebSocket (se implementa más abajo)
        await broadcast({
            'tipo': 'crash',
            'id': event_id,
            'maxMultiplier': max_mult,
            'roundDuration': round_dur,
            'startedAt': started_at,
            'timestamp_recepcion': timestamp
        })
    else:
        logger.warning("[CRASH] ⚠️ ID %s con multiplicador inválido: %s", event_id, max_mult)

async def monitor_crash():
    logger.info("[CRASH] 🚀 Iniciando monitor")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_crash(session)
            if data:
                await procesar_crash(data)
            await asyncio.sleep(random.uniform(0.5, 1.5))

# ============================================
# FUNCIONES SPACEMAN
# ============================================
async def monitor_spaceman():
    global spaceman_last_multiplier
    reconnect_delay = BASE_RECONNECT_DELAY
    logger.info("[SPACEMAN] 🚀 Iniciando monitor")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(SPACEMAN_WS) as ws:
                    logger.info("[SPACEMAN] ✅ WebSocket conectado")
                    subscribe_msg = {
                        "type": "subscribe",
                        "casinoId": SPACEMAN_CASINO_ID,
                        "currency": SPACEMAN_CURRENCY,
                        "key": [SPACEMAN_GAME_ID]
                    }
                    await ws.send_json(subscribe_msg)
                    logger.info("[SPACEMAN] 📡 Suscripción enviada")
                    reconnect_delay = BASE_RECONNECT_DELAY

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = msg.json()
                                if "gameResult" in data and data["gameResult"]:
                                    result_str = data["gameResult"][0].get("result")
                                    if result_str:
                                        multiplier = float(result_str)
                                        if multiplier >= 1.00 and multiplier != spaceman_last_multiplier:
                                            spaceman_last_multiplier = multiplier
                                            game_id = data.get("gameId", "unknown")
                                            if game_id not in spaceman_events_seen:
                                                spaceman_events_seen.add(game_id)
                                                timestamp = await guardar_evento('spaceman', game_id, multiplier, None, datetime.now().isoformat())
                                                logger.info("[SPACEMAN] 🚀 NUEVO: GameID=%s | %.2fx", game_id, multiplier)
                                                await broadcast({
                                                    'tipo': 'spaceman',
                                                    'id': game_id,
                                                    'maxMultiplier': multiplier,
                                                    'roundDuration': None,
                                                    'startedAt': datetime.now().isoformat(),
                                                    'timestamp_recepcion': timestamp
                                                })
                                            else:
                                                logger.debug("[SPACEMAN] ⚠️ Duplicado: %s | %.2fx", game_id, multiplier)
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError) as e:
                                logger.debug("[SPACEMAN] Ignorando mensaje no relevante: %s", e)
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            logger.warning("[SPACEMAN] 🔌 Conexión cerrada por el servidor")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error("[SPACEMAN] ❌ Error en WebSocket: %s", ws.exception())
                            break
        except Exception as e:
            logger.exception("[SPACEMAN] 💥 Error, reconexión en %.1f s", reconnect_delay)
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_RECONNECT_DELAY, reconnect_delay * 2)

# ============================================
# SERVIDOR HTTP + WEBSOCKET (aiohttp)
# ============================================
connected_clients: Set[web.WebSocketResponse] = set()

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    client_ip = request.remote
    logger.info("Cliente WebSocket conectado desde %s", client_ip)
    try:
        # Enviar historial al conectar
        for api in ['crash', 'spaceman']:
            eventos = await obtener_ultimos_eventos(api, 15000)
            if eventos:
                await ws.send_json({
                    'tipo': 'historial',
                    'api': api,
                    'eventos': eventos
                })
                logger.debug("Enviado historial de %s (%d eventos) a %s", api, len(eventos), client_ip)
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    except Exception as e:
        logger.exception("Error en websocket_handler: %s", e)
    finally:
        connected_clients.remove(ws)
        logger.info("Cliente WebSocket desconectado %s", client_ip)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def start_http_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/', health_handler)
    port = int(os.environ.get('PORT', 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info("✅ Servidor HTTP/WebSocket escuchando en puerto %d", port)
    await asyncio.Future()

async def broadcast(event_data: dict):
    if not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor unificado CRASH + SPACEMAN con servidor HTTP")
    logger.info("=" * 60)

    await asyncio.to_thread(init_db)

    tasks = [
        asyncio.create_task(start_http_server(), name="HTTP"),
        asyncio.create_task(monitor_crash(), name="Crash"),
        asyncio.create_task(monitor_spaceman(), name="Spaceman"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("⏹ Deteniendo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("✅ Detenido")

if __name__ == "__main__":
    from datetime import datetime
    asyncio.run(main())
