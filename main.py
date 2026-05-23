#!/usr/bin/env python3
"""
Immersive Roulette Stats Server — Patrones de Color y Zona
===========================================================================
Recopila spins de Immersive Roulette vía API HTTP con polling robusto.
Calcula estadísticas de docenas, columnas, color y zona.
Detecta patrones predefinidos (N/R y B/A) y guarda aciertos/fallos.
Endpoints para que el bot consulte historial de patrones y secuencias.
"""

import asyncio
import json
import logging
import os
import random
import sqlite3
import time
from collections import defaultdict, deque
from typing import Dict, List, Optional

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [ImmersiveServer] %(levelname)s %(message)s'
)
logger = logging.getLogger("ImmersiveServer")
for _ln in ['aiohttp.access', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ========================= CONFIGURACIÓN =========================
ROULETTES = {
    "IMMERSIVE": {
        "name": "Immersive Roulette",
        "api_url": "https://api-cs.casino.org/svc-evolution-game-events/api/immersiveroulette/latest"
    }
}
STATS_DB = "roulette_stats.db"
MAX_STORED_SPINS = 200
POLL_INTERVAL_OK = 2.0          # segundos entre peticiones exitosas
POLL_MAX_SLEEP = 60.0
POLL_BACKOFF_BASE = 2.0

# Patrones de Color (N=Negro, R=Rojo)
PATTERNS_COLOR = [
    (['N','N','N','R','N'], "Negro"),
    (['R','R','R','N','R'], "Rojo"),
    (['N','N','N','R','R','N','N'], "Rojo"),
    (['R','R','R','N','N','R','R'], "Negro"),
]

# Patrones de Zona (B=Bajo 1-18, A=Alto 19-36)
PATTERNS_ZONE = [
    (['B','B','B','A','B'], "Bajo (1-18)"),
    (['A','A','A','B','A'], "Alto (19-36)"),
    (['B','B','B','A','A','B','B'], "Bajo (1-18)"),
    (['A','A','A','B','B','A','A'], "Alto (19-36)"),
]

# User-agents rotativos
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:124.0) Gecko/20100101 Firefox/124.0',
]

# ========================= BASE DE DATOS =========================
class DBPool:
    """Acceso thread-safe a SQLite para asyncio"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        # Tabla de spins
        conn.execute("""CREATE TABLE IF NOT EXISTS spins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            roulette TEXT NOT NULL,
            game_id TEXT NOT NULL UNIQUE,
            number INTEGER NOT NULL,
            ts INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_spins_roulette ON spins(roulette, id)")
        # Tabla de transiciones (docenas, columnas, color, zona)
        conn.execute("""CREATE TABLE IF NOT EXISTS transitions (
            roulette TEXT NOT NULL,
            from_number INTEGER NOT NULL,
            d0 INTEGER DEFAULT 0, d1 INTEGER DEFAULT 0, d2 INTEGER DEFAULT 0, d3 INTEGER DEFAULT 0,
            c0 INTEGER DEFAULT 0, c1 INTEGER DEFAULT 0, c2 INTEGER DEFAULT 0, c3 INTEGER DEFAULT 0,
            red INTEGER DEFAULT 0, black INTEGER DEFAULT 0,
            low INTEGER DEFAULT 0, high INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0,
            PRIMARY KEY(roulette, from_number)
        )""")
        # Tabla de señales de patrones (color y zona)
        conn.execute("""CREATE TABLE IF NOT EXISTS pattern_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            roulette TEXT NOT NULL,
            pattern_type TEXT NOT NULL,
            pattern_seq TEXT NOT NULL,
            bet TEXT NOT NULL,
            numbers_seq TEXT NOT NULL,
            result TEXT,
            resolved_ts REAL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_patterns_roulette ON pattern_signals(roulette, pattern_type)")
        # Tabla de estadísticas agregadas de patrones
        conn.execute("""CREATE TABLE IF NOT EXISTS pattern_stats (
            roulette TEXT NOT NULL,
            pattern_type TEXT NOT NULL,
            pattern_seq TEXT NOT NULL,
            bet TEXT NOT NULL,
            total INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            PRIMARY KEY(roulette, pattern_type, pattern_seq)
        )""")
        conn.commit()
        conn.close()
        logger.info(f"DB inicializada: {self.db_path}")

    async def execute(self, query: str, params: tuple = ()):
        async with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                return conn.execute(query, params).fetchall()
            finally:
                conn.close()

    async def execute_single(self, query: str, params: tuple = ()):
        async with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                return conn.execute(query, params).fetchone()
            finally:
                conn.close()

    async def commit(self, query: str, params: tuple = ()):
        async with self.lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(query, params)
                conn.commit()
            finally:
                conn.close()

db_pool = DBPool(STATS_DB)

# ========================= HELPERS =========================
def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

def get_color(n: int) -> str:
    if n == 0: return "VERDE"
    reds = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}
    return "Rojo" if n in reds else "Negro"

def get_zone(n: int) -> str:
    if n == 0: return "CERO"
    return "Bajo" if 1 <= n <= 18 else "Alto"

# ========================= ENGINE PRINCIPAL =========================
class StatsEngine:
    def __init__(self):
        self.last_numbers: Dict[str, Optional[int]] = {r: None for r in ROULETTES}
        self.last_game_ids: Dict[str, str] = {r: "" for r in ROULETTES}
        self.color_sequences: Dict[str, deque] = {r: deque(maxlen=50) for r in ROULETTES}
        self.zone_sequences: Dict[str, deque] = {r: deque(maxlen=50) for r in ROULETTES}
        self.pending_pattern: Dict[str, Optional[dict]] = {r: None for r in ROULETTES}
        self._load_last_states()

    def _load_last_states(self):
        conn = sqlite3.connect(db_pool.db_path)
        conn.row_factory = sqlite3.Row
        try:
            for roulette in ROULETTES:
                # Último spin
                row = conn.execute(
                    "SELECT number, game_id FROM spins WHERE roulette=? ORDER BY id DESC LIMIT 1",
                    (roulette,)
                ).fetchone()
                if row:
                    self.last_numbers[roulette] = row["number"]
                    self.last_game_ids[roulette] = row["game_id"]
                # Reconstruir secuencias de colores y zonas
                rows = conn.execute(
                    "SELECT number FROM spins WHERE roulette=? ORDER BY id ASC LIMIT 50",
                    (roulette,)
                ).fetchall()
                for r in rows:
                    n = r["number"]
                    if n != 0:
                        self.color_sequences[roulette].append(get_color(n))
                        self.zone_sequences[roulette].append(get_zone(n))
                logger.info(f"[{roulette}] Cargado. Último número: {self.last_numbers[roulette]}")
        finally:
            conn.close()

    async def process_spin(self, roulette: str, number: int, game_id: str) -> bool:
        """Procesa un nuevo spin: guarda, actualiza transiciones, patrones"""
        existing = await db_pool.execute_single(
            "SELECT 1 FROM spins WHERE game_id=?", (game_id,)
        )
        if existing:
            return False

        # Insertar nuevo spin
        await db_pool.commit(
            "INSERT INTO spins(roulette, game_id, number, ts) VALUES(?,?,?,?)",
            (roulette, game_id, number, int(time.time()))
        )

        prev_num = self.last_numbers.get(roulette)
        if prev_num is not None and prev_num != 0:
            d = get_dozen(number)
            c = get_column(number)
            col = get_color(number)
            zone = get_zone(number)
            await db_pool.commit(
                """UPDATE transitions SET
                   d%d = d%d + 1, c%d = c%d + 1,
                   red = red + %d, black = black + %d,
                   low = low + %d, high = high + %d,
                   total = total + 1
                   WHERE roulette=? AND from_number=?"""
                % (d, d, c, c,
                   1 if col=="Rojo" else 0, 1 if col=="Negro" else 0,
                   1 if zone=="Bajo" else 0, 1 if zone=="Alto" else 0),
                (roulette, prev_num)
            )
            await db_pool.commit(
                "INSERT OR IGNORE INTO transitions(roulette, from_number) VALUES(?,?)",
                (roulette, prev_num)
            )

        self.last_numbers[roulette] = number
        self.last_game_ids[roulette] = game_id

        # Actualizar secuencias (solo números distintos de cero)
        if number != 0:
            self.color_sequences[roulette].append(get_color(number))
            self.zone_sequences[roulette].append(get_zone(number))

        # Limpieza de spins antiguos
        await self._cleanup_old_spins(roulette)

        # --- Lógica de patrones ---
        # 1. Resolver patrón pendiente del spin anterior
        if self.pending_pattern[roulette] is not None:
            pend = self.pending_pattern[roulette]
            if pend["pattern_type"] == "color":
                real = get_color(number)
                result = "WIN" if real == pend["bet"] else "LOSS"
            else:
                real = get_zone(number)
                result = "WIN" if real == pend["bet"] else "LOSS"
            await db_pool.commit(
                "UPDATE pattern_signals SET result=?, resolved_ts=? WHERE id=?",
                (result, time.time(), pend["id"])
            )
            await self._update_pattern_stats(roulette, pend["pattern_type"],
                                             pend["pattern_seq"], pend["bet"], result)
            self.pending_pattern[roulette] = None
            logger.info(f"[{roulette}] Patrón {pend['pattern_seq']} resuelto -> {result}")

        # 2. Detectar nuevo patrón de color
        color_seq = list(self.color_sequences[roulette])
        for pattern, bet in PATTERNS_COLOR:
            if len(color_seq) >= len(pattern) and color_seq[-len(pattern):] == pattern:
                window_numbers = await self._get_last_n_numbers(roulette, len(pattern))
                if window_numbers:
                    pattern_seq_str = ",".join(pattern)
                    numbers_str = ",".join(str(n) for n in window_numbers)
                    cur = await db_pool.execute_single(
                        """INSERT INTO pattern_signals
                           (ts, roulette, pattern_type, pattern_seq, bet, numbers_seq)
                           VALUES (?,?,?,?,?,?) RETURNING id""",
                        (time.time(), roulette, "color", pattern_seq_str, bet, numbers_str)
                    )
                    if cur:
                        self.pending_pattern[roulette] = {
                            "id": cur["id"], "pattern_type": "color",
                            "pattern_seq": pattern_seq_str, "bet": bet
                        }
                        logger.info(f"[{roulette}] Nuevo patrón color: {pattern} -> {bet}")
                    break

        # 3. Detectar nuevo patrón de zona (solo si no hay pendiente de color)
        if self.pending_pattern[roulette] is None:
            zone_seq = list(self.zone_sequences[roulette])
            for pattern, bet in PATTERNS_ZONE:
                if len(zone_seq) >= len(pattern) and zone_seq[-len(pattern):] == pattern:
                    window_numbers = await self._get_last_n_numbers(roulette, len(pattern))
                    if window_numbers:
                        pattern_seq_str = ",".join(pattern)
                        numbers_str = ",".join(str(n) for n in window_numbers)
                        cur = await db_pool.execute_single(
                            """INSERT INTO pattern_signals
                               (ts, roulette, pattern_type, pattern_seq, bet, numbers_seq)
                               VALUES (?,?,?,?,?,?) RETURNING id""",
                            (time.time(), roulette, "zone", pattern_seq_str, bet, numbers_str)
                        )
                        if cur:
                            self.pending_pattern[roulette] = {
                                "id": cur["id"], "pattern_type": "zone",
                                "pattern_seq": pattern_seq_str, "bet": bet
                            }
                            logger.info(f"[{roulette}] Nuevo patrón zona: {pattern} -> {bet}")
                        break

        return True

    async def _get_last_n_numbers(self, roulette: str, n: int) -> List[int]:
        rows = await db_pool.execute(
            "SELECT number FROM spins WHERE roulette=? ORDER BY id DESC LIMIT ?",
            (roulette, n)
        )
        numbers = [r["number"] for r in rows]
        numbers.reverse()
        return numbers

    async def _update_pattern_stats(self, roulette: str, ptype: str, pseq: str, bet: str, result: str):
        await db_pool.commit(
            """INSERT INTO pattern_stats (roulette, pattern_type, pattern_seq, bet, total, wins, losses)
               VALUES (?,?,?,?,1,?,?) ON CONFLICT(roulette, pattern_type, pattern_seq) DO UPDATE SET
               total = total + 1,
               wins = wins + (?),
               losses = losses + (?)""",
            (roulette, ptype, pseq, bet,
             1 if result=="WIN" else 0, 1 if result=="LOSS" else 0,
             1 if result=="WIN" else 0, 1 if result=="LOSS" else 0)
        )

    async def _cleanup_old_spins(self, roulette: str):
        await db_pool.commit(
            """DELETE FROM spins WHERE roulette=? AND id NOT IN
               (SELECT id FROM spins WHERE roulette=? ORDER BY id DESC LIMIT ?)""",
            (roulette, roulette, MAX_STORED_SPINS)
        )

    # ------------------- Métodos de consulta para el bot -------------------
    async def get_last_n_spins(self, roulette: str, n: int = 20) -> List[dict]:
        rows = await db_pool.execute(
            "SELECT number, game_id FROM spins WHERE roulette=? ORDER BY id DESC LIMIT ?",
            (roulette, n)
        )
        return [{"number": r["number"], "game_id": r["game_id"]} for r in rows]

    async def get_total_spins(self, roulette: str) -> int:
        row = await db_pool.execute_single(
            "SELECT COUNT(*) as cnt FROM spins WHERE roulette=?", (roulette,)
        )
        return row["cnt"] if row else 0

    async def get_stats_table(self, roulette: str, cat_type: str) -> dict:
        rows = await db_pool.execute(
            "SELECT * FROM transitions WHERE roulette=?", (roulette,)
        )
        db_data = {row["from_number"]: dict(row) for row in rows}
        result = {}
        for num in range(0, 37):
            data = db_data.get(num, {})
            total = data.get("total", 0)
            if total == 0:
                if cat_type == 'dozen':
                    result[str(num)] = {"1":0.0,"2":0.0,"3":0.0,"zero":0.0,"total":0}
                elif cat_type == 'column':
                    result[str(num)] = {"1":0.0,"2":0.0,"3":0.0,"zero":0.0,"total":0}
                elif cat_type == 'color':
                    result[str(num)] = {"Rojo":0.0,"Negro":0.0,"total":0}
                else:  # zone
                    result[str(num)] = {"Bajo":0.0,"Alto":0.0,"total":0}
                continue

            if cat_type == 'dozen':
                result[str(num)] = {
                    "1": round(data.get("d1",0)/total*100, 1),
                    "2": round(data.get("d2",0)/total*100, 1),
                    "3": round(data.get("d3",0)/total*100, 1),
                    "zero": round(data.get("d0",0)/total*100, 1),
                    "total": total
                }
            elif cat_type == 'column':
                result[str(num)] = {
                    "1": round(data.get("c1",0)/total*100, 1),
                    "2": round(data.get("c2",0)/total*100, 1),
                    "3": round(data.get("c3",0)/total*100, 1),
                    "zero": round(data.get("c0",0)/total*100, 1),
                    "total": total
                }
            elif cat_type == 'color':
                result[str(num)] = {
                    "Rojo": round(data.get("red",0)/total*100, 1),
                    "Negro": round(data.get("black",0)/total*100, 1),
                    "total": total
                }
            else:  # zone
                result[str(num)] = {
                    "Bajo": round(data.get("low",0)/total*100, 1),
                    "Alto": round(data.get("high",0)/total*100, 1),
                    "total": total
                }
        return result

    async def get_latest_data(self, roulette: str) -> dict:
        return {
            "roulette": roulette,
            "roulette_name": ROULETTES[roulette]["name"],
            "total_spins": await self.get_total_spins(roulette),
            "last_20": await self.get_last_n_spins(roulette, 20),
            "stats_dozen": await self.get_stats_table(roulette, "dozen"),
            "stats_column": await self.get_stats_table(roulette, "column"),
            "stats_color": await self.get_stats_table(roulette, "color"),
            "stats_zone": await self.get_stats_table(roulette, "zone"),
            "last_colors": list(self.color_sequences[roulette]),
            "last_zones": list(self.zone_sequences[roulette]),
        }

    async def get_pattern_stats(self, roulette: str, pattern_type: str, pattern_seq: str) -> dict:
        row = await db_pool.execute_single(
            "SELECT total, wins, losses, bet FROM pattern_stats WHERE roulette=? AND pattern_type=? AND pattern_seq=?",
            (roulette, pattern_type, pattern_seq)
        )
        if not row:
            return {"total":0, "wins":0, "losses":0, "win_rate":0.0, "bet":""}
        total = row["total"]
        wins = row["wins"]
        loss = row["losses"]
        win_rate = wins/total if total>0 else 0.0
        return {"total":total, "wins":wins, "losses":loss, "win_rate":win_rate, "bet":row["bet"]}

    async def get_sequence_history(self, roulette: str, numbers: List[int], pattern_type: str) -> List[dict]:
        numbers_str = ",".join(str(n) for n in numbers)
        rows = await db_pool.execute(
            "SELECT result, ts FROM pattern_signals WHERE roulette=? AND pattern_type=? AND numbers_seq=? ORDER BY ts DESC",
            (roulette, pattern_type, numbers_str)
        )
        return [{"result": r["result"], "ts": r["ts"]} for r in rows]

# ========================= POLLER HTTP (ROBUSTO) =========================
async def poll_immersive(roulette_key: str, engine: StatsEngine):
    url = ROULETTES[roulette_key]["api_url"]
    consecutive_errors = 0
    sleep_next = POLL_INTERVAL_OK
    last_game_id = ""

    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(sleep_next)
            try:
                ua = random.choice(USER_AGENTS)
                headers = {
                    'User-Agent': ua,
                    'Accept': 'application/json',
                    'Origin': 'https://www.casino.org',
                    'Referer': 'https://www.casino.org/',
                    'Cache-Control': 'no-cache'
                }
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 30))
                        logger.warning(f"[{roulette_key}] Rate limit 429 → esperando {retry_after}s")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, retry_after + random.uniform(1, 5))
                        continue
                    if resp.status >= 500:
                        consecutive_errors += 1
                        backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        logger.error(f"[{roulette_key}] Error {resp.status} → backoff {backoff:.1f}s")
                        sleep_next = backoff
                        continue
                    if resp.status != 200:
                        logger.warning(f"[{roulette_key}] Código {resp.status}")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue

                    # Respuesta OK
                    try:
                        data = await resp.json(content_type=None)
                    except Exception as e:
                        logger.warning(f"[{roulette_key}] JSON inválido: {e}")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue

                    game_id = data.get("id")
                    result = data.get("result", {})
                    outcome = result.get("outcome", {})
                    number = outcome.get("number")
                    if game_id and number is not None and 0 <= number <= 36:
                        if game_id != last_game_id:
                            logger.info(f"[{roulette_key}] Nuevo spin: #{number} (id={game_id})")
                            await engine.process_spin(roulette_key, number, game_id)
                            last_game_id = game_id
                    else:
                        logger.debug(f"[{roulette_key}] Respuesta sin número válido")

                    consecutive_errors = 0
                    sleep_next = POLL_INTERVAL_OK + random.uniform(0.3, 1.0)

            except asyncio.TimeoutError:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                logger.error(f"[{roulette_key}] Timeout → backoff {backoff:.1f}s")
                sleep_next = backoff
            except aiohttp.ClientConnectorError as e:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                logger.error(f"[{roulette_key}] Conexión fallida: {e} → backoff {backoff:.1f}s")
                sleep_next = backoff
            except Exception as e:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                logger.exception(f"[{roulette_key}] Error inesperado: {e} → backoff {backoff:.1f}s")
                sleep_next = backoff

# ========================= FLASK APP =========================
flask_app = web.Application()
engine = None

async def handle_home(request):
    total_spins = await engine.get_total_spins("IMMERSIVE") if engine else 0
    return web.json_response({
        "status": "ok",
        "roulette": "Immersive Roulette",
        "total_spins": total_spins,
        "patterns": {
            "color": len(PATTERNS_COLOR),
            "zone": len(PATTERNS_ZONE)
        }
    })

async def handle_ping(request):
    return web.json_response({"status": "pong", "ts": time.time()})

async def handle_health(request):
    if not engine:
        return web.json_response({"status": "not_ready"}, status=503)
    total = await engine.get_total_spins("IMMERSIVE")
    return web.json_response({
        "status": "healthy",
        "total_spins": total,
        "pending_pattern": engine.pending_pattern.get("IMMERSIVE") is not None
    })

async def handle_latest(request):
    roulette = request.match_info.get("roulette", "").upper()
    if roulette != "IMMERSIVE":
        return web.json_response({"error": "Only IMMERSIVE available"}, status=404)
    data = await engine.get_latest_data(roulette)
    return web.json_response(data)

async def handle_pattern_stats(request):
    roulette = request.match_info.get("roulette", "").upper()
    if roulette != "IMMERSIVE":
        return web.json_response({"error": "Invalid roulette"}, status=404)
    ptype = request.query.get("type")
    pattern_seq = request.query.get("pattern")
    if not ptype or not pattern_seq:
        return web.json_response({"error": "Need type and pattern"}, status=400)
    stats = await engine.get_pattern_stats(roulette, ptype, pattern_seq)
    return web.json_response(stats)

async def handle_sequence_history(request):
    roulette = request.match_info.get("roulette", "").upper()
    if roulette != "IMMERSIVE":
        return web.json_response({"error": "Invalid roulette"}, status=404)
    numbers_str = request.query.get("numbers")
    ptype = request.query.get("type")
    if not numbers_str or not ptype:
        return web.json_response({"error": "Need numbers and type"}, status=400)
    try:
        numbers = [int(x) for x in numbers_str.split(",")]
    except:
        return web.json_response({"error": "Invalid numbers"}, status=400)
    history = await engine.get_sequence_history(roulette, numbers, ptype)
    return web.json_response(history)

async def handle_stats(request):
    if not engine:
        return web.json_response({"status": "not_ready"}, status=503)
    total = await engine.get_total_spins("IMMERSIVE")
    return web.json_response({
        "total_spins": total,
        "last_numbers": engine.last_numbers.get("IMMERSIVE"),
        "color_sequence_length": len(engine.color_sequences.get("IMMERSIVE", [])),
        "zone_sequence_length": len(engine.zone_sequences.get("IMMERSIVE", [])),
        "pending_pattern": engine.pending_pattern.get("IMMERSIVE") is not None
    })

def setup_routes():
    flask_app.router.add_get("/", handle_home)
    flask_app.router.add_get("/ping", handle_ping)
    flask_app.router.add_get("/health", handle_health)
    flask_app.router.add_get("/latest/{roulette}", handle_latest)
    flask_app.router.add_get("/patterns/{roulette}/stats", handle_pattern_stats)
    flask_app.router.add_get("/patterns/{roulette}/sequence", handle_sequence_history)
    flask_app.router.add_get("/stats", handle_stats)

async def start_background_tasks(app):
    global engine
    engine = StatsEngine()
    app["engine"] = engine
    app["poller_task"] = asyncio.create_task(poll_immersive("IMMERSIVE", engine))
    logger.info("Servidor iniciado. Polling activo para Immersive Roulette.")

async def cleanup_background_tasks(app):
    if "poller_task" in app:
        app["poller_task"].cancel()
        try:
            await app["poller_task"]
        except asyncio.CancelledError:
            pass

# ========================= SELF-PING (mantener Render despierto) =========================
async def self_ping_loop():
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not render_url:
        logger.info("RENDER_EXTERNAL_URL no configurada — self-ping desactivado")
        return
    url = f"{render_url.rstrip('/')}/ping"
    logger.info(f"Self-ping cada 14 min → {url}")
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(14 * 60)
            try:
                async with session.get(url, timeout=10) as resp:
                    logger.info(f"Self-ping OK: {resp.status}")
            except Exception as e:
                logger.warning(f"Self-ping falló: {e}")

# ========================= MAIN =========================
async def main_async():
    setup_routes()
    # Iniciar tasks
    asyncio.create_task(self_ping_loop())
    # Ejecutar web app
    port = int(os.environ.get("PORT", 10004))
    runner = web.AppRunner(flask_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"🌐 Servidor HTTP iniciado en puerto {port}")
    # Mantener el evento vivo
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main_async())
