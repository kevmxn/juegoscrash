#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor unificado para CRASH (Stake) y SPACEMAN (Pragmatic Play)
- Crash: polling HTTP con 20 User-Agents, backoff exponencial, circuit breaker y jitter.
- Spaceman: conexión WebSocket persistente con reconexión automática.
"""

import asyncio
import aiohttp
import json
import time
import random
from typing import Set

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

    # Circuit breaker
    if now < crash_status['blocked_until']:
        wait = crash_status['blocked_until'] - now
        print(f"[CRASH] 🚫 Bloqueado por {wait:.1f}s (demasiados errores)")
        await asyncio.sleep(wait)
        return None

    # Backoff activo
    if now < crash_status['next_allowed_time']:
        wait = crash_status['next_allowed_time'] - now
        print(f"[CRASH] ⏳ En espera por {wait:.1f}s (backoff)")
        await asyncio.sleep(wait)
        return None

    headers = {'User-Agent': get_random_user_agent()}
    try:
        async with session.get(API_CRASH, headers=headers, timeout=5) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                crash_status['next_allowed_time'] = time.time() + retry_after
                crash_status['consecutive_errors'] += 1
                print(f"[CRASH] ⚠️ Servidor pide esperar {retry_after}s")
                return None

            if resp.status == 200:
                crash_status['consecutive_errors'] = 0
                return await resp.json()
            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + retry_after
                crash_status['consecutive_errors'] += 1
                print(f"[CRASH] ⚠️ Rate limit. Esperando {retry_after}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                    print(f"[CRASH] 🔒 Bloqueado por {BLOCK_TIME}s por exceso de errores")
                return None
            elif 500 <= resp.status < 600:
                crash_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
                crash_status['next_allowed_time'] = time.time() + backoff
                print(f"[CRASH] ❌ Error {resp.status}. Backoff {backoff:.1f}s")
                if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    crash_status['blocked_until'] = time.time() + BLOCK_TIME
                    print(f"[CRASH] 🔒 Bloqueado por {BLOCK_TIME}s por exceso de errores")
                return None
            else:
                print(f"[CRASH] ⚠️ Código no esperado: {resp.status}")
                return None
    except asyncio.TimeoutError:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff
        print(f"[CRASH] ⏰ Timeout. Backoff {backoff:.1f}s")
        if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            crash_status['blocked_until'] = time.time() + BLOCK_TIME
            print(f"[CRASH] 🔒 Bloqueado por {BLOCK_TIME}s por exceso de errores")
        return None
    except Exception as e:
        crash_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** crash_status['consecutive_errors']))
        crash_status['next_allowed_time'] = time.time() + backoff
        print(f"[CRASH] 💥 Error: {e}. Backoff {backoff:.1f}s")
        if crash_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            crash_status['blocked_until'] = time.time() + BLOCK_TIME
            print(f"[CRASH] 🔒 Bloqueado por {BLOCK_TIME}s por exceso de errores")
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
        print(f"[CRASH] ✅ NUEVO: ID={event_id} | {max_mult}x | Duración={round_dur}s | Inicio={started_at}")
    else:
        print(f"[CRASH] ⚠️ ID {event_id} con multiplicador inválido: {max_mult}")

async def monitor_crash():
    print("[CRASH] 🚀 Iniciando monitor de CRASH (Stake)")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_crash(session)
            if data:
                await procesar_crash(data)
            # Espera aleatoria entre 0.5 y 1.5 segundos para evitar patrones
            await asyncio.sleep(random.uniform(0.5, 1.5))

# ============================================
# FUNCIONES SPACEMAN
# ============================================
async def monitor_spaceman():
    global spaceman_last_multiplier
    reconnect_delay = BASE_RECONNECT_DELAY

    print("[SPACEMAN] 🚀 Iniciando monitor de SPACEMAN (Pragmatic Play)")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(SPACEMAN_WS) as ws:
                    print("[SPACEMAN] ✅ WebSocket conectado")

                    subscribe_msg = {
                        "type": "subscribe",
                        "casinoId": SPACEMAN_CASINO_ID,
                        "currency": SPACEMAN_CURRENCY,
                        "key": [SPACEMAN_GAME_ID]
                    }
                    await ws.send_json(subscribe_msg)
                    print("[SPACEMAN] 📡 Suscripción enviada")

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
                                                print(f"[SPACEMAN] 🚀 NUEVO: GameID={game_id} | {multiplier:.2f}x")
                                            else:
                                                print(f"[SPACEMAN] ⚠️ Duplicado: GameID={game_id} | {multiplier:.2f}x (ignorado)")
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError):
                                pass
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            print("[SPACEMAN] 🔌 Conexión cerrada por el servidor")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"[SPACEMAN] ❌ Error en WebSocket: {ws.exception()}")
                            break

        except asyncio.TimeoutError:
            print(f"[SPACEMAN] ⏰ Timeout al conectar. Reintentando en {reconnect_delay:.1f}s")
        except aiohttp.ClientError as e:
            print(f"[SPACEMAN] 🔴 Error de cliente: {e}. Reintentando en {reconnect_delay:.1f}s")
        except Exception as e:
            print(f"[SPACEMAN] 💥 Error inesperado: {e}. Reintentando en {reconnect_delay:.1f}s")

        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_RECONNECT_DELAY, reconnect_delay * 2)

# ============================================
# MAIN
# ============================================
async def main():
    print("=" * 60)
    print("🚀 Monitor unificado CRASH + SPACEMAN iniciado")
    print("=" * 60)

    tasks = [
        asyncio.create_task(monitor_crash(), name="Crash"),
        asyncio.create_task(monitor_spaceman(), name="Spaceman"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n⏹ Deteniendo monitores...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("✅ Monitores detenidos.")

if __name__ == "__main__":
    asyncio.run(main())
