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
- Backoff exponencial y circuit breaker (estilo apis.py)
- Auto‑ping cada 10 minutos
- ANTI-BLOQUEO: Proxies rotativos, headers realistas, warmup, backoff inteligente
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
from typing import Set, Dict, Any, List, Optional
from collections import defaultdict
import aiosqlite

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
DB_PATH = "crash_data.db"

# ============================================
# CONFIGURACIÓN ANTI-BLOQUEO
# ============================================

# PROXIES ROTATIVOS
# Agregar tus proxies aquí en formato: "http://user:pass@ip:port"
PROXIES = [
    # "http://user:pass@ip1:port",
    # "http://user:pass@ip2:port",
]

# Estado de proxies: contador de fallos y tiempo de bloqueo
proxy_status: Dict[str, Dict[str, Any]] = {
    p: {"fails": 0, "blocked_until": 0, "successes": 0}
    for p in PROXIES
}

# Configuración de bloqueo de proxies
MAX_PROXY_FAILS = 3          # Fallos antes de bloquear
PROXY_BLOCK_DURATION = 600   # 10 minutos de bloqueo
PROXY_RECOVERY_RATE = 0.3    # 30% de probabilidad de probar proxy bloqueado

def get_healthy_proxy() -> Optional[str]:
    """Obtiene un proxy saludable (no bloqueado)"""
    now = time.time()
    valid = [p for p, s in proxy_status.items() if s["blocked_until"] < now]

    if not valid and PROXIES:
        # Si todos los proxies están bloqueados, probar uno al azar con baja probabilidad
        if random.random() < PROXY_RECOVERY_RATE:
            logger.warning("[PROXY] Todos los proxies bloqueados, probando uno al azar")
            return random.choice(PROXIES)
        return None

    return random.choice(valid) if valid else None

def mark_proxy_success(proxy: str):
    """Marca un proxy como exitoso, resetea contador de errores"""
    if proxy in proxy_status:
        proxy_status[proxy]["fails"] = 0
        proxy_status[proxy]["successes"] += 1
        logger.debug(f"[PROXY] ✅ {proxy} - Éxitos: {proxy_status[proxy]['successes']}")

def mark_proxy_failed(proxy: str):
    """Marca un proxy como fallido, bloquea si supera el límite"""
    if proxy not in proxy_status:
        return

    proxy_status[proxy]["fails"] += 1
    fails = proxy_status[proxy]["fails"]

    if fails >= MAX_PROXY_FAILS:
        proxy_status[proxy]["blocked_until"] = time.time() + PROXY_BLOCK_DURATION
        logger.warning(f"[PROXY] 🚫 {proxy} bloqueado por {PROXY_BLOCK_DURATION}s ({fails} fallos)")
    else:
        logger.warning(f"[PROXY] ⚠️ {proxy} - Fallo {fails}/{MAX_PROXY_FAILS}")

def get_proxy_stats() -> Dict[str, Any]:
    """Obtiene estadísticas de uso de proxies"""
    return {
        proxy: {
            "fails": status["fails"],
            "successes": status["successes"],
            "blocked": status["blocked_until"] > time.time(),
            "blocked_until": status["blocked_until"] if status["blocked_until"] > time.time() else 0
        }
        for proxy, status in proxy_status.items()
    }

# HEADER PROFILES - Headers realistas y rotativos
HEADER_PROFILES = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://stake.com/",
        "Origin": "https://stake.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
        "Accept": "*/*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://stake.com/casino/games/crash",
        "Origin": "https://stake.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://stake.com/",
        "Origin": "https://stake.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://stake.com/casino/games/crash",
        "Origin": "https://stake.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-GB,en;q=0.9,es;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://stake.com/",
        "Origin": "https://stake.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "DNT": "1",
    },
]

def get_random_headers() -> Dict[str, str]:
    """Obtiene headers rotativos realistas"""
    headers = random.choice(HEADER_PROFILES).copy()
    # Rotar User-Agent independientemente para másvariedad
    headers["User-Agent"] = random.choice(USER_AGENTS)
    return headers

# User Agents (predominantemente Windows)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
]

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TIME = 300

# Configuración de warmup
WARMUP_ENABLED = True
WARMUP_INTERVAL = 30  # Hacer warmup cada N запросов
WARMUP_URL = "https://stake.com"

crash_ids: Set[str] = set()
crash_status = {'consecutive_errors': 0, 'next_allowed_time': 0, 'request_count': 0}
crash_history: list = []
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

# Cookies compartidas para simular sesión real
shared_cookies: Dict[str, str] = {}

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
        # Cargar últimos 100 eventos en memoria
        async with db.execute('SELECT id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion, nivel FROM events ORDER BY timestamp_recepcion DESC LIMIT ?', (MAX_HISTORY,)) as cursor:
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
            INSERT OR REPLACE INTO events (id, maxMultiplier, roundDuration, startedAt, timestamp_recepcion, nivel)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (event['event_id'], event['maxMultiplier'], event.get('roundDuration'), event.get('startedAt'), event['timestamp_recepcion'], event['nivel']))
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
# ANTI-BLOQUEO: WARMUP DE SESIÓN
# ============================================
async def warmup_session(session: aiohttp.ClientSession, proxy: Optional[str] = None):
    """Realiza warmup de la sesión para establecer cookies y parecer un cliente real"""
    if not WARMUP_ENABLED:
        return True

    try:
        headers = get_random_headers()
        async with session.get(
            WARMUP_URL,
            proxy=proxy,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10),
            allow_redirects=True
        ) as resp:
            # Guardar cookies para futuras solicitudes
            for cookie in resp.cookies.values():
                shared_cookies[cookie.key] = cookie.value
            logger.debug(f"[WARMUP] ✅ Warmup completado (status: {resp.status})")
            return resp.status == 200
    except Exception as e:
        logger.debug(f"[WARMUP] ⚠️ Warmup falló: {e}")
        return False

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
# FUNCIONES CRASH (con ANTI-BLOQUEO completo)
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

def calculate_backoff(consecutive_errors: int, is_403: bool = False) -> float:
    """Calcula backoff inteligente basado en el tipo de error"""
    if is_403:
        # Backoff más largo para 403
        if consecutive_errors <= 2:
            return random.uniform(10, 30)  # 10-30s para primeros errores
        else:
            return random.uniform(600, 1200)  # 10-20 min para errores persistentes
    else:
        # Backoff exponencial normal
        return min(MAX_SLEEP, BASE_SLEEP * (2 ** consecutive_errors))

async def consultar_crash(session: aiohttp.ClientSession) -> dict | None:
    """
    Versión mejorada de consultar_crash con ANTI-BLOQUEO completo:
    - Proxies rotativos
    - Headers realistas rotativos
    - Warmup ocasional
    - Bloqueo inteligente de proxies
    - Manejo específico de 403 con pausas largas
    """
    global shared_cookies
    now = time.time()

    # Verificar si hay bloqueo global
    if now < crash_status['next_allowed_time']:
        wait = crash_status['next_allowed_time'] - now
        if wait > 0.5:
            logger.debug(f"[CRASH] ⏳ Backoff global {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    # Verificar bloqueo global por muchos errores consecutivos
    if crash_status['consecutive_errors'] > 5:
        block_duration = random.uniform(600, 1200)  # 10-20 minutos
        crash_status['next_allowed_time'] = time.time() + block_duration
        logger.error(f"[CRASH] 🔒 Bloqueo global tras {crash_status['consecutive_errors']} errores - esperando {block_duration:.0f}s")
        await asyncio.sleep(block_duration)
        return None

    # Obtener proxy saludable
    proxy = get_healthy_proxy() if PROXIES else None

    # Obtener headers rotativos
    headers = get_random_headers()

    # Añadir cookies compartidas si existen
    if shared_cookies:
        cookie_str = "; ".join([f"{k}={v}" for k, v in shared_cookies.items()])
        headers["Cookie"] = cookie_str

    try:
        async with session.get(
            API_CRASH,
            headers=headers,
            proxy=proxy,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:

            # Guardar cookies de respuesta
            for cookie in resp.cookies.values():
                shared_cookies[cookie.key] = cookie.value

            # ===== ÉXITO =====
            if resp.status == 200:
                crash_status['consecutive_errors'] = 0
                crash_status['request_count'] += 1

                # Hacer warmup periódico si está habilitado
                if WARMUP_ENABLED and crash_status['request_count'] % WARMUP_INTERVAL == 0:
                    asyncio.create_task(warmup_session(session, proxy))

                if proxy:
                    mark_proxy_success(proxy)

                data = await resp.json()
                logger.debug(f"[CRASH] ✅ Datos recibidos (proxy: {proxy or 'none'})")
                return data

            # ===== 403 FORBIDDEN - MANEJO ESPECIAL =====
            elif resp.status == 403:
                crash_status['consecutive_errors'] += 1

                if proxy:
                    mark_proxy_failed(proxy)

                is_403 = True
                backoff = calculate_backoff(crash_status['consecutive_errors'], is_403)
                crash_status['next_allowed_time'] = time.time() + backoff

                logger.warning(f"[CRASH] 🚫 403 Forbidden - Backoff {backoff:.1f}s ({crash_status['consecutive_errors']} errores consecutivos)")

                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    long_block = random.uniform(600, 1200)
                    crash_status['next_allowed_time'] = time.time() + long_block
                    logger.error(f"[CRASH] 🔒 Bloqueo prolongado {long_block:.0f}s tras {MAX_CONSECUTIVE_ERRORS} errores")

                return None

            # ===== RATE LIMIT - USAR RETRY-AFTER =====
            elif resp.status == 429:
                crash_status['consecutive_errors'] += 1

                if 'Retry-After' in resp.headers:
                    retry_after = int(resp.headers['Retry-After'])
                else:
                    retry_after = int(resp.headers.get('retry-after', 60))

                crash_status['next_allowed_time'] = time.time() + retry_after
                logger.warning(f"[CRASH] ⚠️ Rate limit 429 - esperando {retry_after}s")

                if proxy:
                    mark_proxy_failed(proxy)

                return None

            # ===== ERRORES 5XX =====
            elif 500 <= resp.status < 600:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff

                if proxy:
                    mark_proxy_failed(proxy)

                logger.error(f"[CRASH] ❌ Error {resp.status} - backoff {backoff:.1f}s")
                return None

            # ===== CÓDIGO INESPERADO =====
            else:
                logger.warning(f"[CRASH] ⚠️ Código inesperado: {resp.status}")
                return None

    # ===== TIMEOUT =====
    except asyncio.TimeoutError:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff

        if proxy:
            mark_proxy_failed(proxy)

        logger.error(f"[CRASH] ⏰ Timeout - backoff {backoff:.1f}s")
        return None

    # ===== ERROR DE CONEXIÓN =====
    except aiohttp.ClientError as e:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff

        if proxy:
            mark_proxy_failed(proxy)

        logger.error(f"[CRASH] 💥 Error de conexión: {e}")
        return None

    # ===== CUALQUIER OTRA EXCEPCIÓN =====
    except Exception as e:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff

        logger.error(f"[CRASH] 💥 Excepción: {e}")
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
            'roundDuration': round_dur,
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

        # Guardar en BD (almacena hasta MAX_STORAGE)
        await save_event(evento)
        if range_key:
            await update_count(current_level, range_key)
        await update_current_level(current_level)

        logger.info(f"[CRASH] ✅ NUEVO: ID={event_id} | {max_mult}x | Duración={round_dur}s | Inicio={started_at} | Nivel={current_level}")
        await event_queue.put(evento)
    else:
        logger.warning(f"[CRASH] ⚠️ ID {event_id} mult inválido: {max_mult}")

async def monitor_crash():
    """
    Monitor principal con intervalos HUMANOS (2-5s) para reducir detección
    """
    logger.info("[CRASH] 🚀 Iniciando monitor con ANTI-BLOQUEO (intervalo ~2-5s con jitter)")

    # Realizar warmup inicial si está habilitado
    if WARMUP_ENABLED:
        logger.info("[CRASH] 🔄 Realizando warmup inicial...")
        async with aiohttp.ClientSession() as warmup_session:
            await warmup_session(warmup_session)

    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_crash(session)
            if data:
                await procesar_crash(data)
                # INTERVALO HUMANO: 2-5 segundos con jitter adicional
                # Esto simula comportamiento humano y reduce detección
                sleep_time = random.uniform(2, 5) + random.random()
                logger.debug(f"[CRASH] 💤 Intervalo humano: {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)
            else:
                # Si falló, el backoff ya esperó, añadimos un poco más
                sleep_time = random.uniform(1, 3)
                await asyncio.sleep(sleep_time)

# ============================================
# ESTADÍSTICAS ANTI-BLOQUEO
# ============================================
async def stats_handler(request):
    """Endpoint para ver estadísticas de anti-bloqueo"""
    stats = {
        'crash_status': {
            'consecutive_errors': crash_status['consecutive_errors'],
            'next_allowed_time': crash_status['next_allowed_time'],
            'request_count': crash_status['request_count']
        },
        'proxy_stats': get_proxy_stats() if PROXIES else {},
        'proxies_configured': len(PROXIES) > 0,
        'warmup_enabled': WARMUP_ENABLED,
        'warmup_interval': WARMUP_INTERVAL
    }
    return web.json_response(stats)

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
        connected_clients.remove(ws)
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(text="Servidor Crash activo con ANTI-BLOQUEO. Use /ws para WebSocket, /stats para estadísticas o /health para health check.", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/stats', stats_handler)
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
    logger.info("🚀 Monitor Crash ANTI-BLOQUEO")
    logger.info("   - Proxies rotativos")
    logger.info("   - Headers realistas")
    logger.info("   - Warmup de cookies")
    logger.info("   - Backoff inteligente")
    logger.info("   - Intervalos humanos (2-5s)")
    if PROXIES:
        logger.info(f"   - {len(PROXIES)} proxies configurados")
    else:
        logger.info("   - ⚠️ Sin proxies (usando conexión directa)")
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
