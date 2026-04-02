#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import sqlite3
import threading
import asyncio
import websockets
import json
import os
import random
import logging
import sys
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================
# CONFIGURACIÓN DE LOGGING PARA RENDER
# ============================================
# Configurar logging para que sea visible en la consola de Render
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURACIÓN
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'

# 30+ user‑agents rotativos
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (Windows NT 10.0; rv:119.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.230 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.163 Mobile Safari/537.36',
    'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'Mozilla/5.0 (compatible; Bingbot/2.0; +http://www.bing.com/bingbot.htm)',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

DB_FILE = 'eventos.db'
MAX_HISTORY = 600

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0

# ============================================
# BASE DE DATOS
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
    logger.info("✅ Base de datos inicializada correctamente")

init_db()

def guardar_evento(api, event_id, maxMultiplier, roundDuration, startedAt):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    c.execute('''INSERT INTO eventos (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (api, event_id, maxMultiplier, roundDuration, startedAt, timestamp))
    conn.commit()
    c.execute('''DELETE FROM eventos WHERE id IN (
                    SELECT id FROM eventos WHERE api = ? ORDER BY timestamp_recepcion DESC LIMIT -1 OFFSET ?
                )''', (api, MAX_HISTORY))
    conn.commit()
    conn.close()
    return timestamp

def obtener_ultimos_eventos(api, limite=MAX_HISTORY):
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

def obtener_estadisticas_giros():
    """Obtiene estadísticas de los últimos giros para logging"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Total de giros
    c.execute("SELECT COUNT(*) FROM eventos WHERE api = 'crash'")
    total_giros = c.fetchone()[0]

    # Promedio de multiplicadores
    c.execute("SELECT AVG(maxMultiplier) FROM eventos WHERE api = 'crash'")
    avg_multiplier = c.fetchone()[0] or 0

    # Último giro
    c.execute("SELECT maxMultiplier, startedAt FROM eventos WHERE api = 'crash' ORDER BY timestamp_recepcion DESC LIMIT 1")
    ultimo = c.fetchone()

    # Giros en la última hora
    c.execute("SELECT COUNT(*) FROM eventos WHERE api = 'crash' AND timestamp_recepcion > datetime('now', '-1 hour')")
    giros_ultima_hora = c.fetchone()[0]

    conn.close()

    return {
        'total_giros': total_giros,
        'avg_multiplier': round(avg_multiplier, 2),
        'ultimo_multiplicador': round(ultimo[0], 2) if ultimo else None,
        'ultimo_startedAt': ultimo[1] if ultimo else None,
        'giros_ultima_hora': giros_ultima_hora
    }

# ============================================
# CONFIGURACIÓN DE SESIÓN HTTP
# ============================================
def crear_sesion():
    sesion = requests.Session()
    retry = Retry(
        total=2,
        backoff_factor=0.1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET']
    )
    adapter = HTTPAdapter(max_retries=retry)
    sesion.mount('http://', adapter)
    sesion.mount('https://', adapter)
    return sesion

sesion_global = crear_sesion()

# ============================================
# FUNCIÓN DE CONSULTA CON BACKOFF
# ============================================
api_status = {
    'consecutive_errors': 0,
    'next_allowed_time': 0,
    'total_requests': 0,
    'total_giros_detectados': 0
}

def get_random_user_agent():
    return random.choice(USER_AGENTS)

def consultar_con_backoff(url):
    now = time.time()
    api_status['total_requests'] += 1

    if now < api_status['next_allowed_time']:
        wait = api_status['next_allowed_time'] - now
        logger.info(f"⏳ API en espera por {wait:.1f}s (backoff)")
        time.sleep(wait)
        return None

    headers = {'User-Agent': get_random_user_agent()}
    try:
        resp = sesion_global.get(url, headers=headers, timeout=5)

        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            api_status['next_allowed_time'] = time.time() + retry_after
            api_status['consecutive_errors'] += 1
            logger.warning(f"⚠️ API pide esperar {retry_after}s (Retry-After header)")
            return None

        if resp.status_code == 200:
            api_status['consecutive_errors'] = 0
            return resp.json()
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', 2 ** api_status['consecutive_errors']))
            api_status['next_allowed_time'] = time.time() + retry_after
            api_status['consecutive_errors'] += 1
            logger.warning(f"⚠️ Rate limited (429). Esperando {retry_after}s")
            return None
        elif 500 <= resp.status_code < 600:
            api_status['consecutive_errors'] += 1
            backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** api_status['consecutive_errors']))
            api_status['next_allowed_time'] = time.time() + backoff
            logger.error(f"❌ Error servidor {resp.status_code}. Backoff {backoff:.1f}s")
            return None
        else:
            logger.warning(f"⚠️ Código no esperado: {resp.status_code}")
            return None

    except requests.exceptions.Timeout:
        api_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** api_status['consecutive_errors']))
        api_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"⏰ Timeout. Backoff {backoff:.1f}s")
        return None
    except Exception as e:
        api_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** api_status['consecutive_errors']))
        api_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"💥 Error: {e}. Backoff {backoff:.1f}s")
        return None

# ============================================
# SERVIDOR WEBSOCKET
# ============================================
connected_clients = set()
websocket_loop = None
stop_websocket = threading.Event()

async def websocket_handler(websocket):
    client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}" if websocket.remote_address else "unknown"
    connected_clients.add(websocket)
    logger.info(f"🔌 Cliente WebSocket conectado: {client_info} (Total: {len(connected_clients)})")

    try:
        eventos = obtener_ultimos_eventos('crash', MAX_HISTORY)
        if eventos:
            await websocket.send(json.dumps({
                'tipo': 'historial',
                'api': 'crash',
                'eventos': eventos
            }, default=str))
            logger.info(f"📤 Historial enviado a {client_info} ({len(eventos)} eventos)")
        await websocket.wait_closed()
    except websockets.exceptions.ConnectionClosed:
        logger.info(f"🔌 Cliente desconectado: {client_info}")
    except Exception as e:
        logger.error(f"❌ Error en WebSocket handler: {e}")
    finally:
        connected_clients.remove(websocket)
        logger.info(f"🔌 Cliente eliminado. Total conectados: {len(connected_clients)}")

async def websocket_server():
    global websocket_loop
    port = int(os.environ.get('PORT', 8080))
    async with websockets.serve(websocket_handler, "0.0.0.0", port):
        logger.info(f"✅ Servidor WebSocket iniciado en puerto {port}")
        websocket_loop = asyncio.get_running_loop()
        while not stop_websocket.is_set():
            await asyncio.sleep(1)

def start_websocket_server():
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(websocket_server())
    except Exception as e:
        logger.error(f"Error fatal en WebSocket server: {e}")

threading.Thread(target=start_websocket_server, daemon=True).start()

async def _async_broadcast(message):
    if connected_clients:
        await asyncio.gather(
            *[client.send(message) for client in connected_clients],
            return_exceptions=True
        )

def broadcast(event_data):
    if websocket_loop is None or not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    asyncio.run_coroutine_threadsafe(_async_broadcast(message), websocket_loop)

# ============================================
# FUNCIÓN PARA LOGGEAR ESTADO DE GIROS
# ============================================
last_log_time = 0
LOG_INTERVAL = 30  # Loggear estadísticas cada 30 segundos

def log_estado_giros(forzar=False):
    """Loggea el estado actual de los giros"""
    global last_log_time

    now = time.time()
    if not forzar and now - last_log_time < LOG_INTERVAL:
        return

    last_log_time = now
    stats = obtener_estadisticas_giros()

    logger.info("=" * 60)
    logger.info("🎰 ESTADÍSTICAS DE GIROS CRASH")
    logger.info("=" * 60)
    logger.info(f"📊 Total de giros registrados: {stats['total_giros']}")
    logger.info(f"🎯 Promedio de multiplicadores: {stats['avg_multiplier']}x")
    logger.info(f"⚡ Giros en última hora: {stats['giros_ultima_hora']}")
    logger.info(f"🔢 Requests totales a API: {api_status['total_requests']}")
    logger.info(f"🆕 Giros nuevos detectados: {api_status['total_giros_detectados']}")
    logger.info(f"👥 Clientes WebSocket conectados: {len(connected_clients)}")

    if stats['ultimo_multiplicador']:
        logger.info(f"🎲 Último giro: {stats['ultimo_multiplicador']}x @ {stats['ultimo_startedAt']}")

    logger.info("=" * 60)

# ============================================
# BUCLE PRINCIPAL (Crash únicamente)
# ============================================
logger.info("🚀 Iniciando monitoreo de Crash...")
logger.info("📡 Conectando a API: " + API_CRASH)

crash_ids = set()
giro_counter = 0

# Loggear estado inicial
log_estado_giros(forzar=True)

try:
    while True:
        now = time.time()

        # Loggear estado periódicamente
        log_estado_giros()

        if now >= api_status['next_allowed_time']:
            crash_data = consultar_con_backoff(API_CRASH)
            if crash_data:
                api_id = crash_data.get('id')
                if api_id and api_id not in crash_ids:
                    crash_ids.add(api_id)
                    api_status['total_giros_detectados'] += 1
                    giro_counter += 1

                    data_inner = crash_data.get('data', {})
                    result = data_inner.get('result', {})
                    max_mult = result.get('maxMultiplier')
                    round_dur = result.get('roundDuration')
                    started_at = data_inner.get('startedAt')

                    if max_mult is not None and max_mult > 0:
                        timestamp = guardar_evento('crash', api_id, max_mult, round_dur, started_at)

                        # Log detallado del giro
                        logger.info("-" * 50)
                        logger.info(f"🎰 NUEVO GIRO DETECTADO (#{giro_counter})")
                        logger.info(f"   📋 ID: {api_id}")
                        logger.info(f"   💥 Multiplicador: {max_mult}x")
                        logger.info(f"   ⏱️ Duración: {round_dur}s")
                        logger.info(f"   🕐 Iniciado: {started_at}")
                        logger.info(f"   💾 Guardado en DB: {timestamp}")
                        logger.info("-" * 50)

                        broadcast({
                            'tipo': 'crash',
                            'id': api_id,
                            'maxMultiplier': max_mult,
                            'roundDuration': round_dur,
                            'startedAt': started_at,
                            'timestamp_recepcion': timestamp,
                            'giro_numero': giro_counter
                        })
                    else:
                        logger.warning(f"⚠️ Giro ID {api_id} con multiplicador inválido: {max_mult}")

        wait = max(0, api_status['next_allowed_time'] - time.time())
        if wait > 0:
            jitter = random.uniform(0.9, 1.1)
            sleep_time = wait * jitter
            logger.debug(f"⏱️  Esperando {sleep_time:.2f}s (con jitter)")
            time.sleep(sleep_time)
        else:
            time.sleep(0.5)

except KeyboardInterrupt:
    logger.info("
⏹ Monitoreo detenido por el usuario.")
    log_estado_giros(forzar=True)
    stop_websocket.set()
    time.sleep(1)
except Exception as e:
    logger.exception(f"❌ Error fatal en bucle principal: {e}")
    log_estado_giros(forzar=True)
    stop_websocket.set()
    raise
