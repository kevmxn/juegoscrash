#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para CRASH con servidor HTTP y WebSocket
- Polling a la API de Stake Crash
- Almacena hasta 100,000 eventos en SQLite
- Envía solo los últimos 100 eventos al conectar
- Eventos en lotes de hasta 20 cada 1 segundo
- Tabla de niveles enviada cada 60-120 segundos (aleatorio)
- Persistencia con SQLite
- Backoff exponencial y circuit breaker
- Auto‑ping cada 10 minutos
- TLS fingerprint real de Chrome via curl_cffi (evita Cloudflare 403)
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

# ✅ curl_cffi impersona el TLS fingerprint de un browser real
# pip install curl_cffi
try:
    from curl_cffi.requests import AsyncSession as CurlAsyncSession
    USE_CURL_CFFI = True
except ImportError:
    USE_CURL_CFFI = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

if not USE_CURL_CFFI:
    logger.warning("⚠️  curl_cffi no instalado. Usando aiohttp (puede seguir dando 403). Instalar: pip install curl_cffi")

# ============================================
# CONFIGURACIÓN CRASH
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
DB_PATH = "crash_data.db"

# User Agents de Chrome (más creíbles para curl_cffi que también impersona Chrome)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

# Impersonaciones disponibles en curl_cffi
CURL_IMPERSONATE = [
    "chrome110", "chrome107", "chrome104", "chrome101",
    "chrome100", "chrome99", "firefox102", "firefox100",
]

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
# FUNCIONES DE BASE DE DATOS
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
                event = {
                    'event_id': row[0],
                    'maxMultiplier': row[1],
                    'roundDuration': row[2],
                    'startedAt': row[3],
                    'timestamp_recepcion': row[4],
                    'nivel': row[5]
                }
                crash_history.append(event)
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
                await db.execute('INSERT OR IGNORE INTO state (key, value) VALUES (?, ?)', ('current_level', '0'))
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
        await db.execute(
            'INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)',
            ('current_level', str(level))
        )
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
                        logger.info("[PING] Auto‑ping exitoso, servicio activo")
                    else:
                        logger.warning(f"[PING] Auto‑ping falló con código {resp.status}")
        except Exception as e:
            logger.error(f"[PING] Error en auto‑ping: {e}")

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
# FUNCIONES CRASH
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

def _build_headers() -> dict:
    """Headers que imitan un browser real haciendo un fetch de API."""
    return {
        'User-Agent': get_random_user_agent(),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Origin': 'https://stake.com',
        'Referer': 'https://stake.com/',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'cross-site',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

def _registrar_error(descripcion: str):
    """Incrementa errores consecutivos y aplica backoff."""
    crash_status['consecutive_errors'] += 1
    n = crash_status['consecutive_errors']
    backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** n))
    if n >= MAX_CONSECUTIVE_ERRORS:
        crash_status['next_allowed_time'] = time.time() + BLOCK_TIME
        logger.error(f"[CRASH] 🔒 Bloqueado {BLOCK_TIME}s ({n} errores consecutivos) - {descripcion}")
    else:
        crash_status['next_allowed_time'] = time.time() + backoff
        logger.warning(f"[CRASH] {descripcion} - Backoff {backoff:.1f}s ({n} errores consecutivos)")

# -----------------------------------------------
# CONSULTA con curl_cffi (impersonación TLS real)
# -----------------------------------------------
async def _consultar_con_curl() -> dict | None:
    impersonate = random.choice(CURL_IMPERSONATE)
    async with CurlAsyncSession(impersonate=impersonate) as session:
        resp = await session.get(API_CRASH, headers=_build_headers(), timeout=10)
        if resp.status_code == 200:
            crash_status['consecutive_errors'] = 0
            return resp.json()
        elif resp.status_code == 403:
            _registrar_error(f"🚫 403 Forbidden [impersonate={impersonate}]")
            return None
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', 2 ** crash_status['consecutive_errors']))
            crash_status['consecutive_errors'] += 1
            crash_status['next_allowed_time'] = time.time() + retry_after
            logger.warning(f"[CRASH] ⚠️ Rate limit, esperar {retry_after}s ({crash_status['consecutive_errors']} errores)")
            return None
        elif 500 <= resp.status_code < 600:
            _registrar_error(f"❌ Error {resp.status_code}")
            return None
        else:
            logger.warning(f"[CRASH] ⚠️ Código inesperado: {resp.status_code}")
            return None

# -----------------------------------------------
# CONSULTA fallback con aiohttp
# -----------------------------------------------
async def _consultar_con_aiohttp(session: aiohttp.ClientSession) -> dict | None:
    async with session.get(
        API_CRASH, headers=_build_headers(), timeout=aiohttp.ClientTimeout(total=10)
    ) as resp:
        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            crash_status['consecutive_errors'] += 1
            crash_status['next_allowed_time'] = time.time() + retry_after
            logger.warning(f"[CRASH] ⚠️ Retry-After {retry_after}s ({crash_status['consecutive_errors']} errores)")
            return None
        if resp.status == 200:
            crash_status['consecutive_errors'] = 0
            return await resp.json()
        elif resp.status == 403:
            _registrar_error("🚫 403 Forbidden [aiohttp]")
            return None
        elif resp.status == 429:
            retry_after = int(resp.headers.get('Retry-After', 2 ** crash_status['consecutive_errors']))
            crash_status['consecutive_errors'] += 1
            crash_status['next_allowed_time'] = time.time() + retry_after
            logger.warning(f"[CRASH] ⚠️ Rate limit, esperar {retry_after}s")
            return None
        elif 500 <= resp.status < 600:
            _registrar_error(f"❌ Error {resp.status}")
            return None
        else:
            logger.warning(f"[CRASH] ⚠️ Código inesperado: {resp.status}")
            return None

# -----------------------------------------------
# CONSULTA principal
# -----------------------------------------------
async def consultar_crash(aiohttp_session: aiohttp.ClientSession) -> dict | None:
    now = time.time()
    if now < crash_status['next_allowed_time']:
        wait = crash_status['next_allowed_time'] - now
        if wait > 0.5:
            logger.debug(f"[CRASH] ⏳ En backoff, esperando {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    try:
        if USE_CURL_CFFI:
            return await _consultar_con_curl()
        else:
            return await _consultar_con_aiohttp(aiohttp_session)
    except asyncio.TimeoutError:
        _registrar_error("⏰ Timeout")
        return None
    except Exception as e:
        _registrar_error(f"💥 Excepción: {e}")
        return None

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

    if max_mult is not None and max_mult > 0:
        if max_mult < 2.00:
            current_level -= 1
        else:
            current_level += 1

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

        logger.info(f"[CRASH] ✅ NUEVO: ID={event_id} | {max_mult}x | Duración={round_dur}s | Inicio={started_at} | Nivel={current_level}")
        await event_queue.put(evento)
    else:
        logger.warning(f"[CRASH] ⚠️ ID {event_id} mult inválido: {max_mult}")

async def monitor_crash():
    logger.info("[CRASH] 🚀 Iniciando monitor (intervalo ~2s con jitter)")
    logger.info(f"[CRASH] Modo TLS: {'curl_cffi ✅' if USE_CURL_CFFI else 'aiohttp ⚠️'}")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_crash(session)
            if data:
                await procesar_crash(data)
                sleep_time = random.uniform(1.5, 2.5)
                await asyncio.sleep(sleep_time)
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
            await ws.send_json({
                'tipo': 'historial',
                'api': 'crash',
                'eventos': crash_history
            })
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
    logger.info(f"   TLS Mode: {'curl_cffi ✅' if USE_CURL_CFFI else 'aiohttp ⚠️  → instalar: pip install curl_cffi'}")
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
