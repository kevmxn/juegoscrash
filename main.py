#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor unificado CRASH + SPACEMAN con servidor HTTP para Render
- Crash: polling HTTP robusto (20 user‑agents, backoff, circuit breaker)
- Spaceman: WebSocket persistente con reconexión automática
- Servidor HTTP en el puerto de Render con endpoint /health
- Logs mejorados con timestamps y niveles
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import random
import os
import logging
from typing import Set, Dict, Any

# ============================================
# CONFIGURACIÓN DE LOGGING
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

    # Headers sin br (para evitar error de Brotli si no está instalado, pero lo instalaremos)
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',  # Eliminamos br para mayor compatibilidad
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
                logger.warning(f"[CRASH] ⚠️ Esperar {retry_after}s (Retry-After)")
                return None

            if resp.status == 200:
                crash_status['consecutive_errors'] = 0
                return await resp.json()

            if resp.status == 403:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                logger.warning(f"[CRASH] 🚫 403 Forbidden - backoff {backoff:.1f}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.error(f"[CRASH] 🔒 Bloqueado {BLOCK_TIME}s por exceso de errores")
                return None

            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + retry_after
                crash_status['consecutive_errors'] += 1
                logger.warning(f"[CRASH] ⚠️ Rate limit, esperar {retry_after}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.error(f"[CRASH] 🔒 Bloqueado {BLOCK_TIME}s por errores")
                return None

            if 500 <= resp.status < 600:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                logger.error(f"[CRASH] ❌ Error {resp.status}, backoff {backoff:.1f}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                return None

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
        logger.info(f"[CRASH] ✅ NUEVO: ID={event_id} | {max_mult}x | Duración={round_dur}s | Inicio={started_at}")
        return max_mult
    else:
        logger.warning(f"[CRASH] ⚠️ ID {event_id} mult inválido: {max_mult}")
        return None

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
                                                logger.info(f"[SPACEMAN] 🚀 NUEVO: GameID={game_id} | {multiplier:.2f}x")
                                            else:
                                                logger.info(f"[SPACEMAN] ⚠️ Duplicado: GameID={game_id} | {multiplier:.2f}x (ignorado)")
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError):
                                pass
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            logger.info("[SPACEMAN] 🔌 Conexión cerrada por el servidor")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error(f"[SPACEMAN] ❌ Error en WebSocket: {ws.exception()}")
                            break
        except asyncio.TimeoutError:
            logger.error(f"[SPACEMAN] ⏰ Timeout al conectar. Reintentando en {reconnect_delay:.1f}s")
        except aiohttp.ClientError as e:
            logger.error(f"[SPACEMAN] 🔴 Error de cliente: {e}. Reintentando en {reconnect_delay:.1f}s")
        except Exception as e:
            logger.error(f"[SPACEMAN] 💥 Error inesperado: {e}. Reintentando en {reconnect_delay:.1f}s")

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
    try:
        # Aquí podrías enviar historial, pero por ahora no tenemos BD integrada
        # Si quieres enviar historial, necesitarías tener una BD.
        # Por simplicidad, solo mantenemos la conexión abierta.
        logger.info("Cliente WebSocket conectado")
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.remove(ws)
        logger.info("Cliente WebSocket desconectado")
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(text="Servidor activo. Use /ws para WebSocket o /health para health check.", status=200)

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
    logger.info(f"✅ Servidor HTTP/WebSocket escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor unificado CRASH + SPACEMAN con servidor HTTP")
    logger.info("=" * 60)

    # Crear directorio para base de datos si no existe (opcional)
    # Si no usas BD, puedes omitir
    os.makedirs("data", exist_ok=True)
    logger.info("Base de datos inicializada en data/eventos.db")

    tasks = [
        asyncio.create_task(start_web_server(), name="HTTP"),
        asyncio.create_task(monitor_crash(), name="Crash"),
        asyncio.create_task(monitor_spaceman(), name="Spaceman"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("\n⏹ Deteniendo monitores...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("✅ Monitores detenidos.")

if __name__ == "__main__":
    asyncio.run(main())
