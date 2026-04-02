#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor simple para Crash - Compatible con Render
- Polling a la API de Stake Crash con requests
- Almacena eventos en SQLite
- WebSocket para clientes
- Auto-ping cada 10 minutos para mantener vivo el servidor en Render
"""

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
from datetime import datetime
from typing import Set
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURACIÓN
# ============================================
API_CRASH = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest'
DB_PATH = "crash_data.db"

# User Agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0

MAX_HISTORY = 100          # Últimos 100 eventos en memoria
MAX_STORAGE = 100000       # Máximo en BD

crash_ids: Set[str] = set()
crash_history: list = []
current_level = 0
level_counts = defaultdict(lambda: {'3-4.99': 0, '5-9.99': 0, '10+': 0})

# Estado de la API
api_status = {
    'consecutive_errors': 0,
    'next_allowed_time': 0
}

# WebSocket
connected_clients: Set[websockets.WebSocketServerProtocol] = set()
websocket_loop = None
stop_websocket = threading.Event()

# ============================================
# BASE DE DATOS
# ============================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            maxMultiplier REAL,
            startedAt TEXT,
            timestamp_recepcion TEXT,
            nivel INTEGER
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS counts (
            level INTEGER,
            range TEXT,
            count INTEGER,
            PRIMARY KEY (level, range)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()
    conn.close()

def load_from_db():
    global crash_history, crash_ids, level_counts, current_level
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Cargar últimos 100 eventos
    c.execute('SELECT id, maxMultiplier, startedAt, timestamp_recepcion, nivel FROM events ORDER BY timestamp_recepcion DESC LIMIT ?', (MAX_HISTORY,))
    rows = c.fetchall()
    crash_history = []
    crash_ids.clear()
    for row in rows:
        event = {
            'event_id': row[0],
            'maxMultiplier': row[1],
            'startedAt': row[2],
            'timestamp_recepcion': row[3],
            'nivel': row[4]
        }
        crash_history.append(event)
        crash_ids.add(row[0])
    
    # Cargar contadores
    c.execute('SELECT level, range, count FROM counts')
    rows = c.fetchall()
    level_counts.clear()
    for level, rng, cnt in rows:
        level_counts[level][rng] = cnt
    
    # Cargar nivel actual
    c.execute('SELECT value FROM state WHERE key = "current_level"')
    row = c.fetchone()
    if row:
        current_level = int(row[0])
    else:
        c.execute('INSERT OR IGNORE INTO state (key, value) VALUES (?, ?)', ('current_level', '0'))
        conn.commit()
    
    conn.close()

def save_event(event: dict):
    """Guarda evento y resetea la BD si se alcanza el límite de 100,000"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Verificar cuántos eventos hay actualmente
    c.execute('SELECT COUNT(*) FROM events')
    count = c.fetchone()[0]
    
    # Si llegamos al límite, resetear la base manteniendo solo los últimos 100
    if count >= MAX_STORAGE:
        logger.warning(f"[BD] Límite de {MAX_STORAGE} alcanzado. Resetenado base de datos...")
        
        # Obtener los últimos 100 eventos
        c.execute('''
            SELECT id, maxMultiplier, startedAt, timestamp_recepcion, nivel 
            FROM events 
            ORDER BY timestamp_recepcion DESC 
            LIMIT ?
        ''', (MAX_HISTORY,))
        last_100 = c.fetchall()
        
        # Cerrar conexión
        conn.close()
        
        # Eliminar archivo de base de datos
        try:
            os.remove(DB_PATH)
            logger.info(f"[BD] Archivo {DB_PATH} eliminado")
        except Exception as e:
            logger.error(f"[BD] Error al eliminar archivo: {e}")
        
        # Recrear base de datos
        init_db()
        
        # Reconectar y guardar los últimos 100
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        for row in reversed(last_100):  # Revertir para mantener orden cronológico
            c.execute('''
                INSERT INTO events (id, maxMultiplier, startedAt, timestamp_recepcion, nivel)
                VALUES (?, ?, ?, ?, ?)
            ''', row)
        
        conn.commit()
        logger.info(f"[BD] Base de datos recreada con {len(last_100)} eventos")
    
    # Insertar el nuevo evento
    c.execute('''
        INSERT OR REPLACE INTO events (id, maxMultiplier, startedAt, timestamp_recepcion, nivel)
        VALUES (?, ?, ?, ?, ?)
    ''', (event['event_id'], event['maxMultiplier'], event.get('startedAt'), 
          event['timestamp_recepcion'], event['nivel']))
    
    conn.commit()
    conn.close()

def update_count(level: int, range_key: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT INTO counts (level, range, count) VALUES (?, ?, 1)
        ON CONFLICT(level, range) DO UPDATE SET count = count + 1
    ''', (level, range_key))
    conn.commit()
    conn.close()

def update_current_level(level: int):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)
    ''', ('current_level', str(level)))
    conn.commit()
    conn.close()

# ============================================
# AUTO-PING PARA RENDER
# ============================================
def self_ping():
    """Auto-ping cada 10 minutos para mantener el servidor activo en Render"""
    port = int(os.environ.get('PORT', 10000))
    url = f"http://localhost:{port}/health"
    while not stop_websocket.is_set():
        time.sleep(600)  # 10 minutos
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                logger.info("[PING] Auto-ping exitoso, servicio activo")
            else:
                logger.warning(f"[PING] Auto-ping falló con código {response.status_code}")
        except Exception as e:
            logger.error(f"[PING] Error en auto-ping: {e}")

# ============================================
# FUNCIONES DE CONSULTA A LA API
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

def consultar_crash():
    """Consulta la API de Crash con backoff exponencial"""
    global api_status
    now = time.time()
    
    if now < api_status['next_allowed_time']:
        wait = api_status['next_allowed_time'] - now
        if wait > 0.5:
            logger.debug(f"[Crash] ⏳ Backoff {wait:.1f}s")
        time.sleep(wait)
        return None
    
    headers = {'User-Agent': get_random_user_agent()}
    try:
        resp = requests.get(API_CRASH, headers=headers, timeout=10)
        
        if 'Retry-After' in resp.headers:
            retry_after = int(resp.headers['Retry-After'])
            api_status['next_allowed_time'] = time.time() + retry_after
            api_status['consecutive_errors'] += 1
            logger.warning(f"[Crash] ⚠️ Retry-After {retry_after}s")
            return None
        
        if resp.status_code == 200:
            api_status['consecutive_errors'] = 0
            return resp.json()
        
        elif resp.status_code == 403:
            api_status['consecutive_errors'] += 1
            backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** api_status['consecutive_errors']))
            api_status['next_allowed_time'] = time.time() + backoff
            logger.warning(f"[Crash] 🚫 403 Forbidden - backoff {backoff:.1f}s")
            return None
        
        elif resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', 2 ** api_status['consecutive_errors']))
            api_status['next_allowed_time'] = time.time() + retry_after
            api_status['consecutive_errors'] += 1
            logger.warning(f"[Crash] ⚠️ Rate limit, esperar {retry_after}s")
            return None
        
        elif 500 <= resp.status < 600:
            api_status['consecutive_errors'] += 1
            backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** api_status['consecutive_errors']))
            api_status['next_allowed_time'] = time.time() + backoff
            logger.error(f"[Crash] ❌ Error {resp.status_code}, backoff {backoff:.1f}s")
            return None
        
        else:
            logger.warning(f"[Crash] ⚠️ Código inesperado: {resp.status_code}")
            return None
    
    except requests.exceptions.Timeout:
        api_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** api_status['consecutive_errors']))
        api_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[Crash] ⏰ Timeout, backoff {backoff:.1f}s")
        return None
    except Exception as e:
        api_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** api_status['consecutive_errors']))
        api_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[Crash] 💥 Excepción: {e}")
        return None

def procesar_crash(data: dict):
    """Procesa un evento de Crash"""
    global current_level, crash_history, level_counts
    
    event_id = data.get('id')
    if not event_id or event_id in crash_ids:
        return None
    
    crash_ids.add(event_id)
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
            'tipo': 'crash',
            'event_id': event_id,
            'maxMultiplier': max_mult,
            'startedAt': started_at,
            'timestamp_recepcion': datetime.now().isoformat(),
            'nivel': current_level
        }
        
        # Actualizar memoria (últimos 100 eventos)
        crash_history.insert(0, evento)
        if len(crash_history) > MAX_HISTORY:
            crash_history.pop()
        
        if range_key:
            level_counts[current_level][range_key] += 1
        
        # Guardar en BD
        save_event(evento)
        if range_key:
            update_count(current_level, range_key)
        update_current_level(current_level)
        
        logger.info(f"[Crash] ✅ NUEVO: ID={event_id} | {max_mult}x | Nivel={current_level}")
        return evento
    else:
        logger.warning(f"[Crash] ⚠️ ID {event_id} mult inválido: {max_mult}")
        return None

def monitor_crash():
    """Bucle principal de monitoreo"""
    logger.info("[Crash] 🚀 Iniciando monitor (intervalo ~2s con jitter)")
    
    while not stop_websocket.is_set():
        data = consultar_crash()
        if data:
            evento = procesar_crash(data)
            if evento:
                # Enviar a WebSocket
                broadcast({
                    'tipo': 'nuevo_evento',
                    'evento': evento
                })
            # Éxito: espera entre 1.5 y 2.5 segundos
            sleep_time = random.uniform(1.5, 2.5)
        else:
            # Si falló, el backoff ya esperó, añadimos 1s extra
            sleep_time = 1.0
        
        # Verificar si hay que detener
        for _ in range(int(sleep_time * 10)):
            if stop_websocket.is_set():
                break
            time.sleep(0.1)

# ============================================
# SERVIDOR WEBSOCKET
# ============================================
async def websocket_handler(websocket, path):
    """Maneja conexiones WebSocket"""
    connected_clients.add(websocket)
    try:
        # Enviar historial al conectar
        if crash_history:
            await websocket.send(json.dumps({
                'tipo': 'historial',
                'api': 'crash',
                'eventos': crash_history
            }, default=str))
        
        # Enviar tabla de niveles
        await websocket.send(json.dumps({
            'tipo': 'nivel_counts',
            'nivel_actual': current_level,
            'conteos': {k: dict(v) for k, v in level_counts.items()}
        }, default=str))
        
        logger.info("Cliente conectado, historial y tabla enviados")
        
        # Mantener conexión abierta
        while not stop_websocket.is_set():
            try:
                msg = await asyncio.wait_for(websocket.recv(), timeout=30)
                # Procesar mensajes del cliente si es necesario
            except asyncio.TimeoutError:
                # Enviar ping para mantener conexión
                try:
                    await websocket.ping()
                except:
                    break
            except websockets.exceptions.ConnectionClosed:
                break
    except Exception as e:
        logger.error(f"Error en WebSocket: {e}")
    finally:
        connected_clients.remove(websocket)
        logger.info("Cliente desconectado")

async def broadcast_async(message: dict):
    """Envía mensaje a todos los clientes conectados"""
    if not connected_clients:
        return
    
    message_str = json.dumps(message, default=str)
    await asyncio.gather(
        *[client.send(message_str) for client in connected_clients],
        return_exceptions=True
    )

def broadcast(message: dict):
    """Función sincrónica para enviar mensajes desde el hilo de monitoreo"""
    if websocket_loop is None or not connected_clients:
        return
    
    asyncio.run_coroutine_threadsafe(broadcast_async(message), websocket_loop)

async def websocket_server():
    """Inicia el servidor WebSocket"""
    global websocket_loop
    port = int(os.environ.get('PORT', 10000))
    
    async with websockets.serve(websocket_handler, "0.0.0.0", port):
        logger.info(f"✅ Servidor WebSocket escuchando en puerto {port}")
        websocket_loop = asyncio.get_running_loop()
        
        # Mantener servidor activo
        while not stop_websocket.is_set():
            await asyncio.sleep(1)

def start_websocket_server():
    """Inicia el servidor WebSocket en un hilo separado"""
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(websocket_server())
    except Exception as e:
        logger.error(f"Error en WebSocket server: {e}")

# ============================================
# MAIN
# ============================================
def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor Crash - Sistema simple con auto-ping")
    logger.info("=" * 60)
    
    # Inicializar base de datos
    init_db()
    load_from_db()
    
    # Iniciar servidor WebSocket en hilo separado
    ws_thread = threading.Thread(target=start_websocket_server, daemon=True)
    ws_thread.start()
    
    # Esperar a que el servidor WebSocket esté listo
    time.sleep(2)
    
    # Iniciar auto-ping en hilo separado
    ping_thread = threading.Thread(target=self_ping, daemon=True)
    ping_thread.start()
    
    # Iniciar monitoreo en el hilo principal
    try:
        monitor_crash()
    except KeyboardInterrupt:
        logger.info("\n⏹ Deteniendo...")
        stop_websocket.set()
        time.sleep(1)

if __name__ == "__main__":
    main()
