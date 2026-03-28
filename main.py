#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
import aiohttp.web as web
import json
import sqlite3
import time
import random
import os
from datetime import datetime
from typing import Set, Dict, Any

# ============================================
# CONFIGURACIÓN
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301

DB_FILE = 'data/eventos.db'
MAX_HISTORY = 15000          # Ahora 15.000 eventos por API

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
]

BASE_SLEEP = 1.0
MAX_SLEEP = 20.0

crash_ids: Set[str] = set()
slide_ids: Set[str] = set()
spaceman_last_multiplier: float = None

api_status = {
    'crash': {'consecutive_errors': 0, 'next_allowed_time': 0},
    'slide': {'consecutive_errors': 0, 'next_allowed_time': 0}
}

# Clientes WebSocket conectados (objetos WebSocketResponse de aiohttp)
connected_clients: Set[web.WebSocketResponse] = set()

# ============================================
# BASE DE DATOS
# ============================================
def init_db():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
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

def guardar_evento_sync(api: str, event_id: str, maxMultiplier: float, roundDuration: float, startedAt: str) -> str:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute('''INSERT INTO eventos (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp))
    conn.commit()
    # Mantener solo los últimos MAX_HISTORY eventos por API
    c.execute('''DELETE FROM eventos WHERE id IN (
                    SELECT id FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT -1 OFFSET ?
                )''', (api, MAX_HISTORY))
    conn.commit()
    conn.close()
    return timestamp

def obtener_ultimos_eventos_sync(api: str, limite: int = MAX_HISTORY) -> list:
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

async def guardar_evento(api: str, event_id: str, maxMultiplier: float, roundDuration: float, startedAt: str) -> str:
    return await asyncio.to_thread(guardar_evento_sync, api, event_id, maxMultiplier, roundDuration, startedAt)

async def obtener_ultimos_eventos(api: str, limite: int = MAX_HISTORY) -> list:
    return await asyncio.to_thread(obtener_ultimos_eventos_sync, api, limite)

# ============================================
# SERVIDOR HTTP + WEBSOCKET (aiohttp)
# ============================================
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        # Enviar historial al conectar
        for api in ['crash', 'slide', 'spaceman']:
            eventos = await obtener_ultimos_eventos(api, MAX_HISTORY)
            if eventos:
                await ws.send_json({
                    'tipo': 'historial',
                    'api': api,
                    'eventos': eventos
                })
        # Mantener conexión abierta
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def broadcast(event_data: Dict[str, Any]):
    if not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )

async def start_web_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)   # Conexión WebSocket
    app.router.add_get('/health', health_handler)  # Endpoint de salud
    # Opcional: también responder en la raíz con un mensaje simple
    async def root_handler(request):
        return web.Response(text="Servidor activo. Use /ws para WebSocket o /health para health check.", status=200)
    app.router.add_get('/', root_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"✅ Servidor HTTP/WebSocket escuchando en puerto {port}")
    await asyncio.Future()  # run forever

# ============================================
# FUNCIONES DE MONITOREO (sin cambios, solo ajuste de broadcast)
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_con_backoff(session: aiohttp.ClientSession, url: str, api_nombre: str) -> dict | None:
    status = api_status[api_nombre]
    now = time.time()

    if now < status['next_allowed_time']:
        wait = status['next_allowed_time'] - now
        print(f"⏳ {api_nombre} en espera por {wait:.1f}s (backoff)")
        await asyncio.sleep(wait)
        return None

    headers = {'User-Agent': get_random_user_agent()}
    try:
        async with session.get(url, headers=headers, timeout=5) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                status['next_allowed_time'] = time.time() + retry_after
                status['consecutive_errors'] += 1
                print(f"⚠️ {api_nombre} pide esperar {retry_after}s")
                return None

            if resp.status == 200:
                status['consecutive_errors'] = 0
                return await resp.json()
            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** status['consecutive_errors']))
                status['next_allowed_time'] = time.time() + retry_after
                status['consecutive_errors'] += 1
                print(f"⚠️ {api_nombre} rate limited. Esperando {retry_after}s")
                return None
            elif 500 <= resp.status < 600:
                status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
                status['next_allowed_time'] = time.time() + backoff
                print(f"❌ {api_nombre} error {resp.status}. Backoff {backoff:.1f}s")
                return None
            else:
                print(f"⚠️ {api_nombre} código no esperado: {resp.status}")
                return None
    except asyncio.TimeoutError:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"⏰ {api_nombre} timeout. Backoff {backoff:.1f}s")
        return None
    except Exception as e:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"💥 {api_nombre} error: {e}. Backoff {backoff:.1f}s")
        return None

async def monitor_crash():
    global crash_ids
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_con_backoff(session, API_CRASH, 'crash')
            if data:
                api_id = data.get('id')
                if api_id and api_id not in crash_ids:
                    crash_ids.add(api_id)
                    data_inner = data.get('data', {})
                    result = data_inner.get('result', {})
                    max_mult = result.get('maxMultiplier')
                    round_dur = result.get('roundDuration')
                    started_at = data_inner.get('startedAt')

                    if max_mult is not None and max_mult > 0:
                        timestamp = await guardar_evento('crash', api_id, max_mult, round_dur, started_at)
                        await broadcast({
                            'tipo': 'crash',
                            'id': api_id,
                            'maxMultiplier': max_mult,
                            'roundDuration': round_dur,
                            'startedAt': started_at,
                            'timestamp_recepcion': timestamp
                        })
                        print(f"✅ Crash nuevo: ID={api_id} maxMult={max_mult}")
                    else:
                        print(f"⚠️ Crash ID {api_id} con multiplicador inválido: {max_mult}")

            await asyncio.sleep(0.5)

async def monitor_slide():
    global slide_ids
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_con_backoff(session, API_SLIDE, 'slide')
            if data:
                api_id = data.get('id')
                if api_id and api_id not in slide_ids:
                    slide_ids.add(api_id)
                    data_inner = data.get('data', {})
                    result = data_inner.get('result', {})
                    max_mult = result.get('maxMultiplier')
                    started_at = data_inner.get('startedAt')

                    if max_mult is not None and max_mult > 0:
                        timestamp = await guardar_evento('slide', api_id, max_mult, None, started_at)
                        await broadcast({
                            'tipo': 'slide',
                            'id': api_id,
                            'maxMultiplier': max_mult,
                            'roundDuration': None,
                            'startedAt': started_at,
                            'timestamp_recepcion': timestamp
                        })
                        print(f"✅ Slide nuevo: ID={api_id} maxMult={max_mult}")
                    else:
                        print(f"⚠️ Slide ID {api_id} con multiplicador inválido: {max_mult}")

            await asyncio.sleep(0.5)

async def monitor_spaceman():
    global spaceman_last_multiplier
    reconnect_delay = BASE_SLEEP
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                # Usamos aiohttp para WebSocket también
                async with session.ws_connect(SPACEMAN_WS) as ws:
                    print("✅ Spaceman WebSocket conectado")
                    subscribe_msg = {
                        "type": "subscribe",
                        "casinoId": SPACEMAN_CASINO_ID,
                        "currency": SPACEMAN_CURRENCY,
                        "key": [SPACEMAN_GAME_ID]
                    }
                    await ws.send_json(subscribe_msg)
                    print("📡 Suscripción Spaceman enviada")

                    reconnect_delay = BASE_SLEEP

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
                                            event_id = data.get("gameId") or f"spaceman_{int(time.time())}"
                                            started_at = datetime.now().isoformat()
                                            timestamp = await guardar_evento('spaceman', event_id, multiplier, None, started_at)
                                            await broadcast({
                                                'tipo': 'spaceman',
                                                'id': event_id,
                                                'maxMultiplier': multiplier,
                                                'roundDuration': None,
                                                'startedAt': started_at,
                                                'timestamp_recepcion': timestamp
                                            })
                                            print(f"🚀 Spaceman nuevo: {multiplier:.2f}x")
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError) as e:
                                pass
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            break
        except Exception as e:
            print(f"🔴 Spaceman error: {e}. Reintentando en {reconnect_delay:.1f}s")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_SLEEP, reconnect_delay * 2)

# ============================================
# MAIN
# ============================================
async def main():
    await asyncio.to_thread(init_db)

    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_crash()),
        asyncio.create_task(monitor_slide()),
        asyncio.create_task(monitor_spaceman()),
    ]

    print("🚀 Monitoreo unificado iniciado (Crash, Slide, Spaceman)")
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n⏹ Deteniendo monitoreo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
