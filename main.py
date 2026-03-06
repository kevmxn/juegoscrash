#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import aiohttp
import websockets
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
# APIs HTTP (Crash y Slide)
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

# WebSocket Spaceman
SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'   # Cambiar si es necesario
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301

# Base de datos
DB_FILE = 'eventos.db'
MAX_HISTORY = 600          # Eventos máximos por API

# User-Agents rotativos para HTTP
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    # ... (puedes añadir más)
]

# Configuración de backoff
BASE_SLEEP = 1.0
MAX_SLEEP = 60.0

# Control de IDs vistos para evitar duplicados (por API)
crash_ids: Set[str] = set()
slide_ids: Set[str] = set()
spaceman_last_multiplier: float = None   # Último multiplicador Spaceman para evitar duplicados

# Estado de backoff por API (para HTTP)
api_status = {
    'crash': {'consecutive_errors': 0, 'next_allowed_time': 0},
    'slide': {'consecutive_errors': 0, 'next_allowed_time': 0}
}

# ============================================
# BASE DE DATOS (funciones síncronas, ejecutadas en threads)
# ============================================
def init_db():
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
    """Inserta un evento y retorna timestamp de recepción."""
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
# SERVIDOR WEBSOCKET (broadcast a clientes)
# ============================================
connected_clients: Set[websockets.WebSocketServerProtocol] = set()
websocket_server_task = None

async def websocket_handler(websocket):
    connected_clients.add(websocket)
    try:
        # Enviar historial al conectar (para todas las APIs)
        for api in ['crash', 'slide', 'spaceman']:
            eventos = await obtener_ultimos_eventos(api, MAX_HISTORY)
            if eventos:
                await websocket.send(json.dumps({
                    'tipo': 'historial',
                    'api': api,
                    'eventos': eventos
                }, default=str))
        await websocket.wait_closed()
    finally:
        connected_clients.remove(websocket)

async def broadcast(event_data: Dict[str, Any]):
    if not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    await asyncio.gather(
        *[client.send(message) for client in connected_clients],
        return_exceptions=True
    )

async def start_websocket_server():
    port = int(os.environ.get('PORT', 8080))
    async with websockets.serve(websocket_handler, "0.0.0.0", port):
        print(f"✅ Servidor WebSocket escuchando en puerto {port}")
        await asyncio.Future()  # Corre para siempre

# ============================================
# FUNCIONES DE AYUDA PARA HTTP CON BACKOFF
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

# ============================================
# MONITOREO CRASH (HTTP)
# ============================================
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

            # Esperar un poco antes de la siguiente iteración (manejado por backoff)
            await asyncio.sleep(0.5)

# ============================================
# MONITOREO SLIDE (HTTP)
# ============================================
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

# ============================================
# MONITOREO SPACEMAN (WEBSOCKET)
# ============================================
async def monitor_spaceman():
    global spaceman_last_multiplier
    reconnect_delay = BASE_SLEEP
    while True:
        try:
            async with websockets.connect(SPACEMAN_WS) as ws:
                print("✅ Spaceman WebSocket conectado")
                # Enviar suscripción
                subscribe_msg = {
                    "type": "subscribe",
                    "casinoId": SPACEMAN_CASINO_ID,
                    "currency": SPACEMAN_CURRENCY,
                    "key": [SPACEMAN_GAME_ID]
                }
                await ws.send(json.dumps(subscribe_msg))
                print("📡 Suscripción Spaceman enviada")

                # Resetear backoff al conectar exitosamente
                reconnect_delay = BASE_SLEEP

                async for message in ws:
                    try:
                        data = json.loads(message)
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
                        # Ignorar mensajes no relevantes
                        pass

        except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError) as e:
            print(f"🔴 Spaceman conexión cerrada: {e}. Reintentando en {reconnect_delay:.1f}s")
        except Exception as e:
            print(f"💥 Spaceman error inesperado: {e}. Reintentando en {reconnect_delay:.1f}s")

        # Backoff exponencial para reconexión
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_SLEEP, reconnect_delay * 2)

# ============================================
# MAIN
# ============================================
async def main():
    # Inicializar base de datos
    await asyncio.to_thread(init_db)

    # Lanzar tareas concurrentes
    tasks = [
        asyncio.create_task(start_websocket_server(), name="WebSocket"),
        asyncio.create_task(monitor_crash(), name="Crash"),
        asyncio.create_task(monitor_slide(), name="Slide"),
        asyncio.create_task(monitor_spaceman(), name="Spaceman"),
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
