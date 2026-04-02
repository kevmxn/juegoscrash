#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para Crash con servidor HTTP y WebSocket
- Polling a la API de Stake Crash
- Almacena hasta 100,000 eventos en SQLite
- Envía solo los últimos 100 eventos al conectar
- Eventos en lotes de hasta 20 cada 1 segundo
- Tabla de niveles enviada cada 60-120 segundos (aleatorio)
- Persistencia con SQLite
- Backoff exponencial y circuit breaker (estilo apis.py)
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
from typing import Set, Dict, Any, List
from collections import defaultdict
import aiosqlite

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURACIÓN Crash
# ============================================
API_Crash = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
DB_PATH = "crash_data.db"

# User Agents (misma lista extensa que en crashstake.py y apis.py)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:45.0) Gecko/20100101 Firefox/45.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko/20100101 Firefox/11.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0",
    "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0 Waterfox/109.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:60.9) Gecko/20100101 Goanna/4.9 Firefox/60.9 PaleMoon/28.9.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Goanna/5.0 Firefox/78.0 PaleMoon/29.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:52.9) Gecko/20100101 Goanna/4.0 Firefox/52.9 Basilisk/2019.10.29",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0 SeaMonkey/2.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0 SeaMonkey/2.35",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.28) Gecko/20120306 Firefox/3.6.28 (K-Meleon 1.5.4)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
    "Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux i686; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (X11; Linux i686; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0",
    "Mozilla/5.0 (X11; Linux i686; rv:52.0) Gecko/20100101 Firefox/52.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:31.0) Gecko/20100101 Firefox/31.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:60.9) Gecko/20100101 Goanna/4.9 Firefox/60.9 PaleMoon/28.9.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:52.9) Gecko/20100101 Goanna/4.0 Firefox/52.9 Basilisk/2019.10.29",
    "Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0 SeaMonkey/2.35",
    "Mozilla/5.0 (X11; Linux i686; rv:2.0) Gecko/20100101 Firefox/4.0 SeaMonkey/2.1",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2.28) Gecko/20120306 Firefox/3.6.28 (K-Meleon 1.5.4)",
]

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TIME = 300

Crash_ids: Set[str] = set()
Crash_status = {'consecutive_errors': 0, 'next_allowed_time': 0}
Crash_history: list = []
MAX_HISTORY = 100          # Solo se envían los últimos 100 al cliente
MAX_STORAGE = 100000       # Se almacenan hasta 100,000 eventos en BD

current_level = 0
level_counts = defaultdict(lambda: {'3-4.99': 0, '5-9.99': 0, '10+': 0})

connected_clients: Set[web.WebSocketResponse] = set()

# Batching
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
    global Crash_history, Crash_ids, level_counts, current_level
    async with aiosqlite.connect(DB_PATH) as db:
        # Cargar últimos 100 eventos en memoria
        async with db.execute('SELECT id, maxMultiplier, startedAt, timestamp_recepcion, nivel FROM events ORDER BY timestamp_recepcion DESC LIMIT ?', (MAX_HISTORY,)) as cursor:
            rows = await cursor.fetchall()
            Crash_history = []
            Crash_ids.clear()
            for row in rows:
                event = {
                    'event_id': row[0],
                    'maxMultiplier': row[1],
                    'startedAt': row[2],
                    'timestamp_recepcion': row[3],
                    'nivel': row[4]
                }
                Crash_history.append(event)
                Crash_ids.add(row[0])
        # Cargar contadores y estado
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
            INSERT OR REPLACE INTO events (id, maxMultiplier, startedAt, timestamp_recepcion, nivel)
            VALUES (?, ?, ?, ?, ?)
        ''', (event['event_id'], event['maxMultiplier'], event.get('startedAt'), event['timestamp_recepcion'], event['nivel']))
        # Mantener solo los últimos MAX_STORAGE eventos en BD
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
        await db.execute('''
            INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)
        ''', ('current_level', str(level)))
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
                async with session.get(url, timeout=5) as resp:
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
    batch_msg = {
        'tipo': 'batch',
        'eventos': events_list
    }
    message = json.dumps(batch_msg, default=str)
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
        table_msg = {
            'tipo': 'nivel_counts',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()}
        }
        message = json.dumps(table_msg, default=str)
        await asyncio.gather(
            *[client.send_str(message) for client in connected_clients],
            return_exceptions=True
        )
        logger.info(f"Tabla de niveles enviada (intervalo {interval:.1f}s)")

# ============================================
# FUNCIONES Crash (con backoff estilo apis.py)
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_Crash(session: aiohttp.ClientSession) -> dict | None:
    now = time.time()
    if now < Crash_status['next_allowed_time']:
        wait = Crash_status['next_allowed_time'] - now
        if wait > 0.5:
            logger.debug(f"[Crash] ⏳ Backoff {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    headers = {'User-Agent': get_random_user_agent()}
    try:
        async with session.get(API_Crash, headers=headers, timeout=10) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                Crash_status['next_allowed_time'] = time.time() + retry_after
                Crash_status['consecutive_errors'] += 1
                logger.warning(f"[Crash] ⚠️ Retry-After {retry_after}s")
                return None

            if resp.status == 200:
                Crash_status['consecutive_errors'] = 0
                return await resp.json()

            elif resp.status == 403:
                Crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** Crash_status['consecutive_errors']))
                Crash_status['next_allowed_time'] = time.time() + backoff
                logger.warning(f"[Crash] 🚫 403 Forbidden - backoff {backoff:.1f}s")
                if Crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    Crash_status['next_allowed_time'] = time.time() + BLOCK_TIME
                    logger.error(f"[Crash] 🔒 Bloqueado {BLOCK_TIME}s")
                return None

            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** Crash_status['consecutive_errors']))
                Crash_status['next_allowed_time'] = time.time() + retry_after
                Crash_status['consecutive_errors'] += 1
                logger.warning(f"[Crash] ⚠️ Rate limit, esperar {retry_after}s")
                return None

            elif 500 <= resp.status < 600:
                Crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** Crash_status['consecutive_errors']))
                Crash_status['next_allowed_time'] = time.time() + backoff
                logger.error(f"[Crash] ❌ Error {resp.status}, backoff {backoff:.1f}s")
                return None

            else:
                logger.warning(f"[Crash] ⚠️ Código inesperado: {resp.status}")
                return None

    except asyncio.TimeoutError:
        Crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** Crash_status['consecutive_errors']))
        Crash_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[Crash] ⏰ Timeout, backoff {backoff:.1f}s")
        return None
    except Exception as e:
        Crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** Crash_status['consecutive_errors']))
        Crash_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[Crash] 💥 Excepción: {e}")
        return None

async def procesar_Crash(data: dict):
    global current_level, Crash_history, level_counts
    event_id = data.get('id')
    if not event_id or event_id in Crash_ids:
        return
    Crash_ids.add(event_id)
    data_inner = data.get('data', {})
    result = data_inner.get('result', {})
    max_mult = result.get('maxMultiplier')
    started_at = data_inner.get('startedAt')
    if max_mult is not None and max_mult > 0:
        # Actualizar nivel
        if max_mult < 2.00:
            current_level -= 1
        else:
            current_level += 1

        # Determinar rango
        range_key = None
        if 3.00 <= max_mult <= 4.99:
            range_key = '3-4.99'
        elif 5.00 <= max_mult <= 9.99:
            range_key = '5-9.99'
        elif max_mult >= 10.00:
            range_key = '10+'

        evento = {
            'tipo': 'Crash',
            'event_id': event_id,
            'maxMultiplier': max_mult,
            'startedAt': started_at,
            'timestamp_recepcion': datetime.now().isoformat(),
            'nivel': current_level
        }

        # Actualizar memoria (últimos 100 eventos)
        Crash_history.insert(0, evento)
        if len(Crash_history) > MAX_HISTORY:
            Crash_history.pop()
        if range_key:
            level_counts[current_level][range_key] += 1

        # Guardar en BD (almacena hasta MAX_STORAGE)
        await save_event(evento)
        if range_key:
            await update_count(current_level, range_key)
        await update_current_level(current_level)

        logger.info(f"[Crash] ✅ NUEVO: ID={event_id} | {max_mult}x | Inicio={started_at} | Nivel={current_level}")
        await event_queue.put(evento)
    else:
        logger.warning(f"[Crash] ⚠️ ID {event_id} mult inválido: {max_mult}")

async def monitor_Crash():
    logger.info("[Crash] 🚀 Iniciando monitor (intervalo ~2s con jitter)")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_Crash(session)
            if data:
                await procesar_Crash(data)
                # Éxito: espera entre 1.5 y 2.5 segundos (jitter)
                sleep_time = random.uniform(1.5, 2.5)
                await asyncio.sleep(sleep_time)
            else:
                # Si falló, el backoff ya esperó, añadimos 1s extra para no saturar
                await asyncio.sleep(1)

# ============================================
# SERVIDOR HTTP + WEBSOCKET
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        if Crash_history:
            await ws.send_json({
                'tipo': 'historial',
                'api': 'Crash',
                'eventos': Crash_history
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
        connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(text="Servidor crash activo. Use /ws para WebSocket o /health para health check.", status=200)

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
    logger.info(f"✅ Servidor crash escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor crash con almacenamiento 100k eventos, envío últimos 100")
    logger.info("=" * 60)
    await init_db()
    await load_from_db()
    asyncio.create_task(batch_sender())
    asyncio.create_task(periodic_table_sender())
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_Crash()),
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
