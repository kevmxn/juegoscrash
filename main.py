#!/usr/bin/env python3
"""
Immersive Roulette Stats Server v3
===========================================================================
CAMBIOS vs v2:
  - Registro de aciertos/fallos de señales de DOCENAS y COLUMNAS
    (enviadas por el bot vía POST /signals/{roulette}/dozen|column)
  - Auto-resolución en cada spin (igual que color/zone patterns)
  - Stats por último número: efectividad histórica de señales según el
    número anterior al resultado
  - Expuesto en /latest/{roulette}: dozen_signals, column_signals
  - Endpoints: POST /signals/{roulette}/dozen
               POST /signals/{roulette}/column
               GET  /signals/{roulette}/dozen
               GET  /signals/{roulette}/column
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional, Dict, List

import aiohttp
from aiohttp import web, WSMsgType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ImmersiveServer] %(levelname)s %(message)s"
)
logger = logging.getLogger("ImmersiveServer")
for _ln in ["aiohttp.access", "aiohttp.server", "urllib3"]:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
EVOLUTION_URL   = "https://api-cs.casino.org/svc-evolution-game-events/api/immersiveroulette/latest"
POLL_SECS       = 1
RENDER_URL      = os.environ.get("RENDER_EXTERNAL_URL", "")
STATS_DB        = "immersive_stats.db"
MAX_STORED      = 500
ROULETTE        = "IMMERSIVE"
ROULETTE_NAME   = "Immersive Roulette"

EVOLUTION_HEADERS = {
    "origin":           "https://www.casino.org",
    "referer":          "https://www.casino.org/",
    "user-agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "accept":           "*/*",
    "accept-language":  "es,en;q=0.9",
    "sec-fetch-dest":   "empty",
    "sec-fetch-mode":   "cors",
    "sec-fetch-site":   "same-site",
}

# ─── HELPERS ──────────────────────────────────────────────────────────────────
COLOR_MAP: Dict[int, str] = {
    0: "V",
    1: "R",  2: "N",  3: "R",  4: "N",  5: "R",  6: "N",
    7: "R",  8: "N",  9: "R",  10: "N", 11: "N", 12: "R",
    13: "N", 14: "R", 15: "N", 16: "R", 17: "N", 18: "R",
    19: "R", 20: "N", 21: "R", 22: "N", 23: "R", 24: "N",
    25: "R", 26: "N", 27: "R", 28: "N", 29: "N", 30: "R",
    31: "N", 32: "R", 33: "N", 34: "R", 35: "N", 36: "R",
}

def get_color(n: int) -> str:
    return COLOR_MAP.get(n, "V")

def get_zone(n: int) -> str:
    if n == 0: return "Z"
    return "B" if n <= 18 else "A"

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

def parse_settled_at(s: str) -> float:
    """Convierte el campo settledAt (ISO 8601) de la API de Evolution a Unix timestamp.
    Devuelve 0.0 si el string está vacío o tiene formato inválido."""
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0

# ─── PATRONES ─────────────────────────────────────────────────────────────────
COLOR_PATTERNS = [
    {"id": "CP1", "p": ["N","N","N","R","N"],         "bet": "Negro"},
    {"id": "CP2", "p": ["R","R","R","N","R"],         "bet": "Rojo"},
    {"id": "CP3", "p": ["N","N","N","R","R","N","N"], "bet": "Rojo"},
    {"id": "CP4", "p": ["R","R","R","N","N","R","R"], "bet": "Negro"},
]

ZONE_PATTERNS = [
    {"id": "ZP1", "p": ["B","B","B","A","B"],         "bet": "Bajo"},
    {"id": "ZP2", "p": ["A","A","A","B","A"],         "bet": "Alto"},
    {"id": "ZP3", "p": ["B","B","B","A","A","B","B"], "bet": "Bajo"},
    {"id": "ZP4", "p": ["A","A","A","B","B","A","A"], "bet": "Alto"},
]

def check_patterns(seq: List[str], patterns: List[dict]) -> Optional[dict]:
    """Comprueba si el final de seq coincide con algún patrón. Devuelve el más largo."""
    matched = None
    for pat in patterns:
        plen = len(pat["p"])
        if len(seq) >= plen and seq[-plen:] == pat["p"]:
            if matched is None or plen > len(matched["p"]):
                matched = pat
    return matched

def color_matches_bet(color: str, bet: str) -> bool:
    return (bet == "Negro" and color == "N") or (bet == "Rojo" and color == "R")

def zone_matches_bet(zone: str, bet: str) -> bool:
    return (bet == "Bajo" and zone == "B") or (bet == "Alto" and zone == "A")

# ─── DB POOL ──────────────────────────────────────────────────────────────────
class DBPool:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock    = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # Spins extendido con color y zona
        conn.execute("""CREATE TABLE IF NOT EXISTS spins (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id  TEXT    NOT NULL UNIQUE,
            number   INTEGER NOT NULL,
            color    TEXT    NOT NULL,
            zone     TEXT    NOT NULL,
            ts       INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_spins_id ON spins(id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_spins_gid ON spins(game_id)")

        # Transiciones docenas/columnas (from_number → siguiente categoría)
        conn.execute("""CREATE TABLE IF NOT EXISTS transitions (
            from_number INTEGER PRIMARY KEY,
            d0 INTEGER DEFAULT 0, d1 INTEGER DEFAULT 0,
            d2 INTEGER DEFAULT 0, d3 INTEGER DEFAULT 0,
            c0 INTEGER DEFAULT 0, c1 INTEGER DEFAULT 0,
            c2 INTEGER DEFAULT 0, c3 INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0
        )""")

        # Transiciones COLOR por número
        conn.execute("""CREATE TABLE IF NOT EXISTS color_transitions (
            from_number INTEGER PRIMARY KEY,
            cnt_R INTEGER DEFAULT 0,
            cnt_N INTEGER DEFAULT 0,
            cnt_V INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0
        )""")

        # Transiciones ZONA por número
        conn.execute("""CREATE TABLE IF NOT EXISTS zone_transitions (
            from_number INTEGER PRIMARY KEY,
            cnt_B INTEGER DEFAULT 0,
            cnt_A INTEGER DEFAULT 0,
            cnt_Z INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0
        )""")

        # Historial de patrones de COLOR
        # sequence_json: números reales del patrón ej [10,22,24,3,26]
        # result_json: {"next_number": 30, "next_color": "R", "result": "Fallo"}
        conn.execute("""CREATE TABLE IF NOT EXISTS color_pattern_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_id    TEXT    NOT NULL,
            pattern_str   TEXT    NOT NULL,
            bet           TEXT    NOT NULL,
            sequence_json TEXT    NOT NULL,
            next_number   INTEGER,
            next_color    TEXT,
            result        TEXT,
            ts            INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cph_pid ON color_pattern_history(pattern_id)")

        # Historial de patrones de ZONA
        conn.execute("""CREATE TABLE IF NOT EXISTS zone_pattern_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_id    TEXT    NOT NULL,
            pattern_str   TEXT    NOT NULL,
            bet           TEXT    NOT NULL,
            sequence_json TEXT    NOT NULL,
            next_number   INTEGER,
            next_zone     TEXT,
            result        TEXT,
            ts            INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_zph_pid ON zone_pattern_history(pattern_id)")

        # ── Señales de DOCENAS (registradas por el bot) ───────────────────────
        # El bot envía POST /signals/{roulette}/dozen al activar una señal.
        # El servidor la resuelve automáticamente en el siguiente spin.
        conn.execute("""CREATE TABLE IF NOT EXISTS dozen_signal_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy    TEXT    NOT NULL,
            pair_json   TEXT    NOT NULL,
            missing     INTEGER NOT NULL,
            prob        REAL    NOT NULL,
            last_number INTEGER NOT NULL,
            next_number INTEGER,
            next_dozen  INTEGER,
            result      TEXT,
            ts          INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dsh_ln ON dozen_signal_history(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dsh_res ON dozen_signal_history(result)")

        # ── Señales de COLUMNAS (registradas por el bot) ──────────────────────
        conn.execute("""CREATE TABLE IF NOT EXISTS column_signal_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy     TEXT    NOT NULL,
            pair_json    TEXT    NOT NULL,
            missing      INTEGER NOT NULL,
            prob         REAL    NOT NULL,
            last_number  INTEGER NOT NULL,
            next_number  INTEGER,
            next_column  INTEGER,
            result       TEXT,
            ts           INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_csh_ln ON column_signal_history(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_csh_res ON column_signal_history(result)")

        # ── Patrones de secuencia DOCENAS (últimos 5 no-cero = 2 docenas) ─────
        # pattern_str: "D1,D1,D2,D1,D2"  numbers_json: [2,5,16,9,15]
        # last_number: último número del patrón (el que completó los 5)
        conn.execute("""CREATE TABLE IF NOT EXISTS dozen_seq_patterns (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_json    TEXT    NOT NULL,
            missing      INTEGER NOT NULL,
            pattern_str  TEXT    NOT NULL,
            numbers_json TEXT    NOT NULL,
            last_number  INTEGER NOT NULL,
            next_number  INTEGER,
            next_dozen   INTEGER,
            result       TEXT,
            ts           INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dsp_ln  ON dozen_seq_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dsp_res ON dozen_seq_patterns(result)")

        # ── Patrones de secuencia COLUMNAS (últimos 5 no-cero = 2 columnas) ───
        conn.execute("""CREATE TABLE IF NOT EXISTS column_seq_patterns (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_json    TEXT    NOT NULL,
            missing      INTEGER NOT NULL,
            pattern_str  TEXT    NOT NULL,
            numbers_json TEXT    NOT NULL,
            last_number  INTEGER NOT NULL,
            next_number  INTEGER,
            next_column  INTEGER,
            result       TEXT,
            ts           INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_csp_ln  ON column_seq_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_csp_res ON column_seq_patterns(result)")

        conn.commit()
        conn.close()
        logger.info(f"✅ DB inicializada: {self.db_path}")

    async def fetch(self, query: str, params: tuple = ()) -> list:
        async with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                return conn.execute(query, params).fetchall()
            finally:
                conn.close()

    async def fetchone(self, query: str, params: tuple = ()):
        async with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                return conn.execute(query, params).fetchone()
            finally:
                conn.close()

    async def write(self, query: str, params: tuple = (), get_lastrowid: bool = False):
        async with self.lock:
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.execute(query, params)
                conn.commit()
                return cur.lastrowid if get_lastrowid else None
            finally:
                conn.close()

    async def write_many(self, operations: List[tuple]):
        """Execute multiple writes in a single transaction."""
        async with self.lock:
            conn = sqlite3.connect(self.db_path)
            try:
                for query, params in operations:
                    conn.execute(query, params)
                conn.commit()
            finally:
                conn.close()

db = DBPool(STATS_DB)

# ─── STATS ENGINE ─────────────────────────────────────────────────────────────
class StatsEngine:
    def __init__(self):
        self.last_game_id: str          = ""
        self.last_number:  Optional[int] = None

        # Secuencias en memoria (se cargan desde DB al iniciar)
        self.number_seq: List[int] = []
        self.color_seq:  List[str] = []
        self.zone_seq:   List[str] = []

        # Señales pendientes de resolución (esperan el siguiente número)
        self.pending_color: Optional[dict] = None   # {pid, p_str, bet, sequence, row_id}
        self.pending_zone:  Optional[dict] = None
        self.pending_dozen_signal:  Optional[dict] = None  # {row_id, strategy, pair, missing, last_number}
        self.pending_column_signal: Optional[dict] = None

        # Patrones de secuencia (2 docenas / 2 columnas en últimos 5 no-cero)
        self.pending_dozen_seq:  Optional[dict] = None  # {row_id, pair, missing, pattern_str, numbers, last_number}
        self.pending_column_seq: Optional[dict] = None

        # Clientes WebSocket suscritos
        self.ws_clients: dict = {}

        self._load_state()

    def _load_state(self):
        """Carga estado desde DB (síncrono, al arrancar)."""
        conn = sqlite3.connect(db.db_path)
        conn.row_factory = sqlite3.Row
        try:
            # Último spin
            row = conn.execute(
                "SELECT game_id, number, color, zone FROM spins ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.last_game_id = row["game_id"]
                self.last_number  = row["number"]
                logger.info(f"[{ROULETTE}] Último spin: #{row['number']} ({row['color']}/{row['zone']})")

            # Últimos 100 para reconstruir secuencias
            rows = conn.execute(
                "SELECT number, color, zone FROM spins ORDER BY id DESC LIMIT 100"
            ).fetchall()
            rows = list(reversed(rows))
            self.number_seq = [r["number"] for r in rows]
            self.color_seq  = [r["color"]  for r in rows]
            self.zone_seq   = [r["zone"]   for r in rows]
            logger.info(f"[{ROULETTE}] Secuencia cargada: {len(rows)} spins")

            # Señal de color pendiente (sin resultado)
            row = conn.execute(
                "SELECT id, pattern_id, pattern_str, bet, sequence_json "
                "FROM color_pattern_history WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_color = {
                    "pid":      row["pattern_id"],
                    "p_str":    row["pattern_str"],
                    "bet":      row["bet"],
                    "sequence": json.loads(row["sequence_json"]),
                    "row_id":   row["id"],
                }
                logger.info(f"[COLOR] Señal pendiente cargada: {row['bet']} (patrón {row['pattern_id']})")

            # Señal de zona pendiente
            row = conn.execute(
                "SELECT id, pattern_id, pattern_str, bet, sequence_json "
                "FROM zone_pattern_history WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_zone = {
                    "pid":      row["pattern_id"],
                    "p_str":    row["pattern_str"],
                    "bet":      row["bet"],
                    "sequence": json.loads(row["sequence_json"]),
                    "row_id":   row["id"],
                }
                logger.info(f"[ZONA] Señal pendiente cargada: {row['bet']} (patrón {row['pattern_id']})")

            # Señal de DOCENA pendiente (enviada por el bot)
            row = conn.execute(
                "SELECT id, strategy, pair_json, missing, last_number "
                "FROM dozen_signal_history WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_dozen_signal = {
                    "row_id":      row["id"],
                    "strategy":    row["strategy"],
                    "pair":        json.loads(row["pair_json"]),
                    "missing":     row["missing"],
                    "last_number": row["last_number"],
                }
                logger.info(
                    f"[DOCENA] Señal pendiente cargada: par={row['pair_json']} "
                    f"last={row['last_number']}"
                )

            # Señal de COLUMNA pendiente (enviada por el bot)
            row = conn.execute(
                "SELECT id, strategy, pair_json, missing, last_number "
                "FROM column_signal_history WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_column_signal = {
                    "row_id":      row["id"],
                    "strategy":    row["strategy"],
                    "pair":        json.loads(row["pair_json"]),
                    "missing":     row["missing"],
                    "last_number": row["last_number"],
                }
                logger.info(
                    f"[COLUMNA] Señal pendiente cargada: par={row['pair_json']} "
                    f"last={row['last_number']}"
                )

            # Patrón de secuencia de DOCENAS pendiente
            row = conn.execute(
                "SELECT id, pair_json, missing, pattern_str, numbers_json, last_number "
                "FROM dozen_seq_patterns WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_dozen_seq = {
                    "row_id":      row["id"],
                    "pair":        json.loads(row["pair_json"]),
                    "missing":     row["missing"],
                    "pattern_str": row["pattern_str"],
                    "numbers":     json.loads(row["numbers_json"]),
                    "last_number": row["last_number"],
                }
                logger.info(
                    f"[D-SEQ] Patrón pendiente cargado: [{row['pattern_str']}] "
                    f"par={row['pair_json']} last={row['last_number']}"
                )

            # Patrón de secuencia de COLUMNAS pendiente
            row = conn.execute(
                "SELECT id, pair_json, missing, pattern_str, numbers_json, last_number "
                "FROM column_seq_patterns WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_column_seq = {
                    "row_id":      row["id"],
                    "pair":        json.loads(row["pair_json"]),
                    "missing":     row["missing"],
                    "pattern_str": row["pattern_str"],
                    "numbers":     json.loads(row["numbers_json"]),
                    "last_number": row["last_number"],
                }
                logger.info(
                    f"[C-SEQ] Patrón pendiente cargado: [{row['pattern_str']}] "
                    f"par={row['pair_json']} last={row['last_number']}"
                )
        finally:
            conn.close()

    # ── Procesamiento de spin ─────────────────────────────────────────────────
    async def process_spin(self, number: int, game_id: str) -> bool:
        # Verificar duplicado
        existing = await db.fetchone("SELECT 1 FROM spins WHERE game_id=?", (game_id,))
        if existing:
            return False

        color = get_color(number)
        zone  = get_zone(number)
        d     = get_dozen(number)
        c     = get_column(number)
        prev  = self.last_number

        ops = []
        # 1. Insertar spin
        ops.append((
            "INSERT INTO spins(game_id, number, color, zone, ts) VALUES(?,?,?,?,?)",
            (game_id, number, color, zone, int(time.time()))
        ))

        # 2. Transiciones docenas/columnas desde número anterior
        if prev is not None and prev != 0:
            ops.append((
                "INSERT OR IGNORE INTO transitions(from_number) VALUES(?)", (prev,)
            ))
            ops.append((
                f"UPDATE transitions SET d{d}=d{d}+1, c{c}=c{c}+1, total=total+1 WHERE from_number=?",
                (prev,)
            ))

        # 3. Transiciones COLOR desde número anterior
        if prev is not None and prev != 0:
            ops.append((
                "INSERT OR IGNORE INTO color_transitions(from_number) VALUES(?)", (prev,)
            ))
            ops.append((
                f"UPDATE color_transitions SET cnt_{color}=cnt_{color}+1, total=total+1 WHERE from_number=?",
                (prev,)
            ))

        # 4. Transiciones ZONA desde número anterior
        if prev is not None and prev != 0:
            ops.append((
                "INSERT OR IGNORE INTO zone_transitions(from_number) VALUES(?)", (prev,)
            ))
            ops.append((
                f"UPDATE zone_transitions SET cnt_{zone}=cnt_{zone}+1, total=total+1 WHERE from_number=?",
                (prev,)
            ))

        await db.write_many(ops)

        # 5. Resolver señales pendientes con el número actual
        await self._resolve_pending_color(number, color)
        await self._resolve_pending_zone(number, zone)
        await self._resolve_pending_dozen_signal(number, d, c)
        await self._resolve_pending_column_signal(number, d, c)
        # Resolver patrones de secuencia (antes de actualizar secuencias)
        await self._resolve_pending_dozen_seq(number, d)
        await self._resolve_pending_column_seq(number, c)

        # 6. Actualizar secuencias en memoria
        self.number_seq.append(number)
        self.color_seq.append(color)
        self.zone_seq.append(zone)
        if len(self.number_seq) > 200:
            self.number_seq.pop(0)
            self.color_seq.pop(0)
            self.zone_seq.pop(0)

        # 7. Detectar nuevos patrones
        await self._detect_color_pattern()
        await self._detect_zone_pattern()
        # Detectar patrones de secuencia (después de actualizar secuencias)
        await self._detect_dozen_seq_pattern(number)
        await self._detect_column_seq_pattern(number)

        # 8. Actualizar estado
        self.last_number  = number
        self.last_game_id = game_id

        # 9. Limpiar spins viejos
        await db.write(
            "DELETE FROM spins WHERE id NOT IN (SELECT id FROM spins ORDER BY id DESC LIMIT ?)",
            (MAX_STORED,)
        )

        logger.info(
            f"[{ROULETTE}] 🎰 #{number} {color}/{zone} | "
            f"D{d} C{c} | gid={game_id[:12]}..."
        )
        return True

    # ── Resolución de señales pendientes ─────────────────────────────────────
    async def _resolve_pending_color(self, number: int, color: str):
        if not self.pending_color:
            return
        bet    = self.pending_color["bet"]
        result = "Acierto" if color_matches_bet(color, bet) else "Fallo"
        await db.write(
            "UPDATE color_pattern_history SET next_number=?, next_color=?, result=? WHERE id=?",
            (number, color, result, self.pending_color["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(
            f"[COLOR] {icon} Patrón {self.pending_color['pid']} → "
            f"bet={bet} | cayó #{number}({color}) → {result}"
        )
        self.pending_color = None

    async def _resolve_pending_zone(self, number: int, zone: str):
        if not self.pending_zone:
            return
        bet    = self.pending_zone["bet"]
        result = "Acierto" if zone_matches_bet(zone, bet) else "Fallo"
        await db.write(
            "UPDATE zone_pattern_history SET next_number=?, next_zone=?, result=? WHERE id=?",
            (number, zone, result, self.pending_zone["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(
            f"[ZONA] {icon} Patrón {self.pending_zone['pid']} → "
            f"bet={bet} | cayó #{number}({zone}) → {result}"
        )
        self.pending_zone = None

    # ── Resolución señales DOCENAS/COLUMNAS ───────────────────────────────────
    async def _resolve_pending_dozen_signal(self, number: int, dozen: int, column: int):
        """Resuelve la señal de docena pendiente con el número recién caído."""
        if not self.pending_dozen_signal:
            return
        pair   = self.pending_dozen_signal["pair"]
        result = "Acierto" if (dozen != 0 and dozen in pair) else "Fallo"
        await db.write(
            "UPDATE dozen_signal_history SET next_number=?, next_dozen=?, result=? WHERE id=?",
            (number, dozen, result, self.pending_dozen_signal["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(
            f"[DOCENA] {icon} par={pair} → cayó #{number}(D{dozen}) → {result} "
            f"[last={self.pending_dozen_signal['last_number']}]"
        )
        self.pending_dozen_signal = None

    async def _resolve_pending_column_signal(self, number: int, dozen: int, column: int):
        """Resuelve la señal de columna pendiente con el número recién caído."""
        if not self.pending_column_signal:
            return
        pair   = self.pending_column_signal["pair"]
        result = "Acierto" if (column != 0 and column in pair) else "Fallo"
        await db.write(
            "UPDATE column_signal_history SET next_number=?, next_column=?, result=? WHERE id=?",
            (number, column, result, self.pending_column_signal["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(
            f"[COLUMNA] {icon} par={pair} → cayó #{number}(C{column}) → {result} "
            f"[last={self.pending_column_signal['last_number']}]"
        )
        self.pending_column_signal = None

    # ── Detección y resolución de patrones de SECUENCIA (2D / 2C en últimos 5) ─

    async def _detect_dozen_seq_pattern(self, current_number: int):
        """Detecta si los últimos 5 números no-cero forman un patrón de 2 docenas.

        Ejemplo: secuencia [2,5,16,9,15] → docenas [D1,D1,D2,D1,D2] → par (1,2)
        """
        if self.pending_dozen_seq:
            return  # Ya hay un patrón pendiente de resolución

        # Pares (número, docena) de los no-cero en memoria
        nz = [
            (self.number_seq[i], (self.number_seq[i] - 1) // 12 + 1)
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 5:
            return

        last5_nums, last5_dozens = zip(*nz[-5:])
        unique_dozens = set(last5_dozens)

        if len(unique_dozens) != 2:
            return  # Necesitamos exactamente 2 docenas distintas

        pair    = sorted(unique_dozens)
        missing = list({1, 2, 3} - set(pair))[0]
        pattern_str  = ",".join(f"D{d}" for d in last5_dozens)
        numbers_json = list(last5_nums)
        last_number  = current_number  # número que completó el patrón

        row_id = await db.write(
            "INSERT INTO dozen_seq_patterns"
            "(pair_json, missing, pattern_str, numbers_json, last_number, ts) "
            "VALUES(?,?,?,?,?,?)",
            (json.dumps(pair), missing, pattern_str,
             json.dumps(numbers_json), last_number, int(time.time())),
            get_lastrowid=True
        )
        self.pending_dozen_seq = {
            "row_id":      row_id,
            "pair":        pair,
            "missing":     missing,
            "pattern_str": pattern_str,
            "numbers":     numbers_json,
            "last_number": last_number,
        }
        logger.info(
            f"[D-SEQ] 🔔 Patrón: [{pattern_str}] → D{pair} | falta D{missing} "
            f"| nums={numbers_json} | last={last_number}"
        )

    async def _detect_column_seq_pattern(self, current_number: int):
        """Detecta si los últimos 5 números no-cero forman un patrón de 2 columnas.

        Ejemplo: secuencia [1,4,7,10,13] → columnas [C1,C1,C1,C1,C1] → no aplica (3 necesarias)
        Ejemplo: [1,2,4,5,7] → columnas [C1,C2,C1,C2,C1] → par (1,2)
        """
        if self.pending_column_seq:
            return

        nz = [
            (self.number_seq[i], ((self.number_seq[i] - 1) % 3) + 1)
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 5:
            return

        last5_nums, last5_cols = zip(*nz[-5:])
        unique_cols = set(last5_cols)

        if len(unique_cols) != 2:
            return

        pair    = sorted(unique_cols)
        missing = list({1, 2, 3} - set(pair))[0]
        pattern_str  = ",".join(f"C{c}" for c in last5_cols)
        numbers_json = list(last5_nums)
        last_number  = current_number

        row_id = await db.write(
            "INSERT INTO column_seq_patterns"
            "(pair_json, missing, pattern_str, numbers_json, last_number, ts) "
            "VALUES(?,?,?,?,?,?)",
            (json.dumps(pair), missing, pattern_str,
             json.dumps(numbers_json), last_number, int(time.time())),
            get_lastrowid=True
        )
        self.pending_column_seq = {
            "row_id":      row_id,
            "pair":        pair,
            "missing":     missing,
            "pattern_str": pattern_str,
            "numbers":     numbers_json,
            "last_number": last_number,
        }
        logger.info(
            f"[C-SEQ] 🔔 Patrón: [{pattern_str}] → C{pair} | falta C{missing} "
            f"| nums={numbers_json} | last={last_number}"
        )

    async def _resolve_pending_dozen_seq(self, number: int, dozen: int):
        """Resuelve el patrón de secuencia de docenas con el número recién caído."""
        if not self.pending_dozen_seq:
            return
        pair   = self.pending_dozen_seq["pair"]
        result = "Acierto" if (dozen != 0 and dozen in pair) else "Fallo"
        await db.write(
            "UPDATE dozen_seq_patterns SET next_number=?, next_dozen=?, result=? WHERE id=?",
            (number, dozen, result, self.pending_dozen_seq["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(
            f"[D-SEQ] {icon} par=D{pair} → cayó #{number}(D{dozen}) → {result} "
            f"[seq={self.pending_dozen_seq['numbers']} last={self.pending_dozen_seq['last_number']}]"
        )
        self.pending_dozen_seq = None

    async def _resolve_pending_column_seq(self, number: int, column: int):
        """Resuelve el patrón de secuencia de columnas con el número recién caído."""
        if not self.pending_column_seq:
            return
        pair   = self.pending_column_seq["pair"]
        result = "Acierto" if (column != 0 and column in pair) else "Fallo"
        await db.write(
            "UPDATE column_seq_patterns SET next_number=?, next_column=?, result=? WHERE id=?",
            (number, column, result, self.pending_column_seq["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(
            f"[C-SEQ] {icon} par=C{pair} → cayó #{number}(C{column}) → {result} "
            f"[seq={self.pending_column_seq['numbers']} last={self.pending_column_seq['last_number']}]"
        )
        self.pending_column_seq = None

    # ── Registro de señales desde el bot ─────────────────────────────────────
    async def register_dozen_signal(
        self, strategy: str, pair: list, missing: int, prob: float, last_number: int
    ) -> int:
        """El bot llama a POST /signals/IMMERSIVE/dozen al activar una señal."""
        if self.pending_dozen_signal:
            logger.warning(
                f"[DOCENA] ⚠️ Señal anterior sin resolver (id={self.pending_dozen_signal['row_id']}) "
                f"→ marcada como Cancelada"
            )
            await db.write(
                "UPDATE dozen_signal_history SET result='Cancelada' WHERE id=?",
                (self.pending_dozen_signal["row_id"],)
            )
        row_id = await db.write(
            "INSERT INTO dozen_signal_history"
            "(strategy, pair_json, missing, prob, last_number, ts) VALUES(?,?,?,?,?,?)",
            (strategy, json.dumps(pair), missing, round(prob, 6), last_number, int(time.time())),
            get_lastrowid=True
        )
        self.pending_dozen_signal = {
            "row_id": row_id, "strategy": strategy,
            "pair": pair, "missing": missing, "last_number": last_number,
        }
        logger.info(
            f"[DOCENA] 🎯 Señal registrada: strat={strategy} par={pair} "
            f"missing={missing} prob={prob:.0%} last={last_number}"
        )
        return row_id

    async def register_column_signal(
        self, strategy: str, pair: list, missing: int, prob: float, last_number: int
    ) -> int:
        """El bot llama a POST /signals/IMMERSIVE/column al activar una señal."""
        if self.pending_column_signal:
            logger.warning(
                f"[COLUMNA] ⚠️ Señal anterior sin resolver → marcada como Cancelada"
            )
            await db.write(
                "UPDATE column_signal_history SET result='Cancelada' WHERE id=?",
                (self.pending_column_signal["row_id"],)
            )
        row_id = await db.write(
            "INSERT INTO column_signal_history"
            "(strategy, pair_json, missing, prob, last_number, ts) VALUES(?,?,?,?,?,?)",
            (strategy, json.dumps(pair), missing, round(prob, 6), last_number, int(time.time())),
            get_lastrowid=True
        )
        self.pending_column_signal = {
            "row_id": row_id, "strategy": strategy,
            "pair": pair, "missing": missing, "last_number": last_number,
        }
        logger.info(
            f"[COLUMNA] 🎯 Señal registrada: strat={strategy} par={pair} "
            f"missing={missing} prob={prob:.0%} last={last_number}"
        )
        return row_id

    # ── Detección de patrones ─────────────────────────────────────────────────
    async def _detect_color_pattern(self):
        """Detecta si los últimos N colores coinciden con algún patrón."""
        if self.pending_color:
            return  # Ya hay una señal activa
        # Filtrar ceros para detección de color
        non_zero_colors = [
            self.color_seq[i]
            for i in range(len(self.color_seq))
            if self.color_seq[i] != "V"
        ]
        matched = check_patterns(non_zero_colors, COLOR_PATTERNS)
        if not matched:
            return

        # Obtener secuencia real de números (últimos len(p) no-cero)
        plen     = len(matched["p"])
        nz_nums  = [
            self.number_seq[i]
            for i in range(len(self.number_seq))
            if self.color_seq[i] != "V"
        ]
        seq_nums = nz_nums[-plen:] if len(nz_nums) >= plen else nz_nums
        p_str    = ",".join(matched["p"])

        row_id = await db.write(
            "INSERT INTO color_pattern_history(pattern_id, pattern_str, bet, sequence_json, ts) "
            "VALUES(?,?,?,?,?)",
            (matched["id"], p_str, matched["bet"], json.dumps(seq_nums), int(time.time())),
            get_lastrowid=True
        )
        self.pending_color = {
            "pid":      matched["id"],
            "p_str":    p_str,
            "bet":      matched["bet"],
            "sequence": seq_nums,
            "row_id":   row_id,
        }
        logger.info(
            f"[COLOR] 🔔 Patrón detectado: {matched['id']} [{p_str}] "
            f"→ Apuesta: {matched['bet']} | Seq: {seq_nums}"
        )

    async def _detect_zone_pattern(self):
        """Detecta si los últimos N zonas coinciden con algún patrón."""
        if self.pending_zone:
            return
        non_zero_zones = [
            self.zone_seq[i]
            for i in range(len(self.zone_seq))
            if self.zone_seq[i] != "Z"
        ]
        matched = check_patterns(non_zero_zones, ZONE_PATTERNS)
        if not matched:
            return

        plen     = len(matched["p"])
        nz_nums  = [
            self.number_seq[i]
            for i in range(len(self.number_seq))
            if self.zone_seq[i] != "Z"
        ]
        seq_nums = nz_nums[-plen:] if len(nz_nums) >= plen else nz_nums
        p_str    = ",".join(matched["p"])

        row_id = await db.write(
            "INSERT INTO zone_pattern_history(pattern_id, pattern_str, bet, sequence_json, ts) "
            "VALUES(?,?,?,?,?)",
            (matched["id"], p_str, matched["bet"], json.dumps(seq_nums), int(time.time())),
            get_lastrowid=True
        )
        self.pending_zone = {
            "pid":      matched["id"],
            "p_str":    p_str,
            "bet":      matched["bet"],
            "sequence": seq_nums,
            "row_id":   row_id,
        }
        logger.info(
            f"[ZONA] 🔔 Patrón detectado: {matched['id']} [{p_str}] "
            f"→ Apuesta: {matched['bet']} | Seq: {seq_nums}"
        )

    # ── Consultas de stats ────────────────────────────────────────────────────
    async def get_total_spins(self) -> int:
        row = await db.fetchone("SELECT COUNT(*) as cnt FROM spins")
        return row["cnt"] if row else 0

    async def get_last_n(self, n: int = 20) -> List[dict]:
        rows = await db.fetch(
            "SELECT number, color, zone, game_id FROM spins ORDER BY id DESC LIMIT ?", (n,)
        )
        return [
            {"number": r["number"], "color": r["color"],
             "zone": r["zone"], "game_id": r["game_id"]}
            for r in rows
        ]

    async def get_stats_dozen(self) -> dict:
        rows = await db.fetch("SELECT * FROM transitions")
        db_data = {row["from_number"]: dict(row) for row in rows}
        result = {}
        for num in range(37):
            data = db_data.get(num)
            if not data or data["total"] == 0:
                result[str(num)] = {"1":0.0,"2":0.0,"3":0.0,"zero":0.0,"total":0}
                continue
            t = data["total"]
            result[str(num)] = {
                "1":   round(data["d1"]/t*100, 1),
                "2":   round(data["d2"]/t*100, 1),
                "3":   round(data["d3"]/t*100, 1),
                "zero":round(data["d0"]/t*100, 1),
                "total": t
            }
        return result

    async def get_stats_column(self) -> dict:
        rows = await db.fetch("SELECT * FROM transitions")
        db_data = {row["from_number"]: dict(row) for row in rows}
        result = {}
        for num in range(37):
            data = db_data.get(num)
            if not data or data["total"] == 0:
                result[str(num)] = {"1":0.0,"2":0.0,"3":0.0,"zero":0.0,"total":0}
                continue
            t = data["total"]
            result[str(num)] = {
                "1":   round(data["c1"]/t*100, 1),
                "2":   round(data["c2"]/t*100, 1),
                "3":   round(data["c3"]/t*100, 1),
                "zero":round(data["c0"]/t*100, 1),
                "total": t
            }
        return result

    async def get_stats_color(self) -> dict:
        rows = await db.fetch("SELECT * FROM color_transitions")
        db_data = {row["from_number"]: dict(row) for row in rows}
        result = {}
        for num in range(37):
            data = db_data.get(num)
            if not data or data["total"] == 0:
                result[str(num)] = {"R":0.0,"N":0.0,"V":0.0,"total":0}
                continue
            t = data["total"]
            result[str(num)] = {
                "R":   round(data["cnt_R"]/t*100, 1),
                "N":   round(data["cnt_N"]/t*100, 1),
                "V":   round(data["cnt_V"]/t*100, 1),
                "total": t
            }
        return result

    async def get_stats_zone(self) -> dict:
        rows = await db.fetch("SELECT * FROM zone_transitions")
        db_data = {row["from_number"]: dict(row) for row in rows}
        result = {}
        for num in range(37):
            data = db_data.get(num)
            if not data or data["total"] == 0:
                result[str(num)] = {"B":0.0,"A":0.0,"Z":0.0,"total":0}
                continue
            t = data["total"]
            result[str(num)] = {
                "B":   round(data["cnt_B"]/t*100, 1),
                "A":   round(data["cnt_A"]/t*100, 1),
                "Z":   round(data["cnt_Z"]/t*100, 1),
                "total": t
            }
        return result

    async def get_pattern_history(self, kind: str, limit: int = 100) -> List[dict]:
        """kind = 'color' | 'zone'"""
        table = "color_pattern_history" if kind == "color" else "zone_pattern_history"
        next_col = "next_color" if kind == "color" else "next_zone"
        rows = await db.fetch(
            f"SELECT pattern_id, pattern_str, bet, sequence_json, "
            f"next_number, {next_col} as next_cat, result, ts "
            f"FROM {table} WHERE result IS NOT NULL "
            f"ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        result = []
        for r in rows:
            try:
                seq = json.loads(r["sequence_json"])
            except Exception:
                seq = []
            result.append({
                "pattern_id":   r["pattern_id"],
                "pattern":      r["pattern_str"],
                "bet":          r["bet"],
                "sequence":     seq,
                "next_number":  r["next_number"],
                "next_cat":     r["next_cat"],
                "result":       r["result"],
                "ts":           r["ts"],
            })
        return result

    async def get_pattern_summary(self, kind: str) -> dict:
        """Resumen de efectividad por patrón."""
        table = "color_pattern_history" if kind == "color" else "zone_pattern_history"
        rows = await db.fetch(
            f"SELECT pattern_id, bet, result FROM {table} WHERE result IS NOT NULL"
        )
        summary: Dict[str, dict] = {}
        for r in rows:
            pid = r["pattern_id"]
            if pid not in summary:
                summary[pid] = {"bet": r["bet"], "aciertos": 0, "fallos": 0, "total": 0}
            summary[pid]["total"] += 1
            if r["result"] == "Acierto":
                summary[pid]["aciertos"] += 1
            else:
                summary[pid]["fallos"] += 1
        for pid in summary:
            t = summary[pid]["total"]
            summary[pid]["efectividad"] = round(summary[pid]["aciertos"]/t*100, 1) if t else 0
        return summary

    # ── Stats de señales de DOCENAS/COLUMNAS ─────────────────────────────────
    async def get_signal_summary(self, kind: str) -> dict:
        """Resumen global de aciertos/fallos de señales (dozen | column)."""
        table = "dozen_signal_history" if kind == "dozen" else "column_signal_history"
        rows  = await db.fetch(
            f"SELECT strategy, result FROM {table} WHERE result IN ('Acierto','Fallo')"
        )
        aciertos = sum(1 for r in rows if r["result"] == "Acierto")
        fallos   = sum(1 for r in rows if r["result"] == "Fallo")
        total    = aciertos + fallos
        # Por estrategia
        by_strat: Dict[str, dict] = {}
        for r in rows:
            s = r["strategy"]
            if s not in by_strat:
                by_strat[s] = {"aciertos": 0, "fallos": 0, "total": 0}
            by_strat[s]["total"] += 1
            if r["result"] == "Acierto":
                by_strat[s]["aciertos"] += 1
            else:
                by_strat[s]["fallos"] += 1
        for s in by_strat:
            t = by_strat[s]["total"]
            by_strat[s]["efectividad"] = round(by_strat[s]["aciertos"]/t*100, 1) if t else 0.0
        return {
            "aciertos":   aciertos,
            "fallos":     fallos,
            "total":      total,
            "efectividad": round(aciertos/total*100, 1) if total > 0 else 0.0,
            "por_estrategia": by_strat,
        }

    async def get_signal_stats_by_number(self, kind: str) -> dict:
        """Aciertos/fallos indexados por last_number (el número anterior a la señal)."""
        table = "dozen_signal_history" if kind == "dozen" else "column_signal_history"
        rows  = await db.fetch(
            f"SELECT last_number, result FROM {table} WHERE result IN ('Acierto','Fallo')"
        )
        stats: Dict[str, dict] = {}
        for r in rows:
            key = str(r["last_number"])
            if key not in stats:
                stats[key] = {"aciertos": 0, "fallos": 0, "total": 0, "efectividad": 0.0}
            stats[key]["total"] += 1
            if r["result"] == "Acierto":
                stats[key]["aciertos"] += 1
            else:
                stats[key]["fallos"] += 1
        for key in stats:
            t = stats[key]["total"]
            stats[key]["efectividad"] = round(stats[key]["aciertos"]/t*100, 1) if t else 0.0
        return stats

    # ── Stats de patrones de SECUENCIA (2D / 2C) ─────────────────────────────

    async def get_seq_pattern_history(self, kind: str, limit: int = 100) -> List[dict]:
        """Historial de patrones de secuencia resueltos.
        kind = 'dozen' | 'column'
        Devuelve registros con: pair, missing, pattern, numbers, last_number,
                                next_number, next_cat, result, ts
        """
        table   = "dozen_seq_patterns"   if kind == "dozen" else "column_seq_patterns"
        nxt_col = "next_dozen"           if kind == "dozen" else "next_column"
        rows = await db.fetch(
            f"SELECT pair_json, missing, pattern_str, numbers_json, last_number, "
            f"next_number, {nxt_col} AS next_cat, result, ts "
            f"FROM {table} WHERE result IS NOT NULL "
            f"ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        result = []
        for r in rows:
            result.append({
                "pair":        json.loads(r["pair_json"]),
                "missing":     r["missing"],
                "pattern":     r["pattern_str"],
                "numbers":     json.loads(r["numbers_json"]),
                "last_number": r["last_number"],
                "next_number": r["next_number"],
                "next_cat":    r["next_cat"],
                "result":      r["result"],
                "ts":          r["ts"],
            })
        return result

    async def get_seq_pattern_stats_by_number(self, kind: str) -> dict:
        """Estadísticas de patrones de secuencia indexadas por last_number.

        Para cada last_number devuelve:
          - aciertos/fallos/total/efectividad global
          - pairs: cada par con sus conteos y efectividad
          - top_pair: el par más frecuente (con su efectividad propia)

        Permite al bot saber: "cuando el último número fue 17,
        qué par de docenas/columnas salió más y con qué win rate".
        """
        table = "dozen_seq_patterns" if kind == "dozen" else "column_seq_patterns"
        rows  = await db.fetch(
            f"SELECT last_number, pair_json, result FROM {table} "
            f"WHERE result IN ('Acierto','Fallo')"
        )

        by_num: Dict[str, dict] = {}
        for r in rows:
            key  = str(r["last_number"])
            pair = tuple(sorted(json.loads(r["pair_json"])))
            pk   = f"{pair[0]},{pair[1]}"

            if key not in by_num:
                by_num[key] = {"aciertos": 0, "fallos": 0, "total": 0,
                               "efectividad": 0.0, "_pairs": {}}

            by_num[key]["total"] += 1
            if pk not in by_num[key]["_pairs"]:
                by_num[key]["_pairs"][pk] = {
                    "pair": list(pair), "count": 0,
                    "aciertos": 0, "fallos": 0, "efectividad": 0.0
                }
            by_num[key]["_pairs"][pk]["count"] += 1

            if r["result"] == "Acierto":
                by_num[key]["aciertos"] += 1
                by_num[key]["_pairs"][pk]["aciertos"] += 1
            else:
                by_num[key]["fallos"] += 1
                by_num[key]["_pairs"][pk]["fallos"] += 1

        # Post-process: efectividades y top_pair
        for key, data in by_num.items():
            t = data["total"]
            data["efectividad"] = round(data["aciertos"] / t * 100, 1) if t else 0.0

            pairs_dict = data["_pairs"]
            for pk, pdata in pairs_dict.items():
                pt = pdata["count"]
                pdata["efectividad"] = round(pdata["aciertos"] / pt * 100, 1) if pt else 0.0

            if pairs_dict:
                top_pk = max(pairs_dict, key=lambda x: pairs_dict[x]["count"])
                data["top_pair"] = pairs_dict[top_pk]
                data["pairs"]    = pairs_dict
            del data["_pairs"]

        return by_num

    async def get_latest_data(self) -> dict:
        """Todo en una consulta para el polling del bot."""
        total        = await self.get_total_spins()
        last_20      = await self.get_last_n(20)
        stats_dozen  = await self.get_stats_dozen()
        stats_column = await self.get_stats_column()
        stats_color  = await self.get_stats_color()
        stats_zone   = await self.get_stats_zone()
        color_hist   = await self.get_pattern_history("color", 50)
        zone_hist    = await self.get_pattern_history("zone", 50)
        color_sum    = await self.get_pattern_summary("color")
        zone_sum     = await self.get_pattern_summary("zone")

        # Señales de docenas y columnas (aciertos/fallos)
        dozen_sum     = await self.get_signal_summary("dozen")
        dozen_by_num  = await self.get_signal_stats_by_number("dozen")
        column_sum    = await self.get_signal_summary("column")
        column_by_num = await self.get_signal_stats_by_number("column")

        # ── Patrones de secuencia 2D / 2C ─────────────────────────────────────
        dozen_seq_hist   = await self.get_seq_pattern_history("dozen", 60)
        dozen_seq_by_num = await self.get_seq_pattern_stats_by_number("dozen")
        col_seq_hist     = await self.get_seq_pattern_history("column", 60)
        col_seq_by_num   = await self.get_seq_pattern_stats_by_number("column")

        return {
            "roulette":       ROULETTE,
            "roulette_name":  ROULETTE_NAME,
            "total_spins":    total,
            "last_20":        last_20,
            "stats_dozen":    stats_dozen,
            "stats_column":   stats_column,
            "stats_color":    stats_color,
            "stats_zone":     stats_zone,
            "color_patterns": {
                "pending":   self.pending_color,
                "history":   color_hist,
                "summary":   color_sum,
            },
            "zone_patterns": {
                "pending":   self.pending_zone,
                "history":   zone_hist,
                "summary":   zone_sum,
            },
            # ── Señales bot de docenas y columnas ────────────────────────────
            "dozen_signals": {
                "pending":   self.pending_dozen_signal,
                "summary":   dozen_sum,
                "by_number": dozen_by_num,
            },
            "column_signals": {
                "pending":   self.pending_column_signal,
                "summary":   column_sum,
                "by_number": column_by_num,
            },
            # ── Patrones de secuencia (2 docenas / 2 columnas en últimos 5) ──
            # Estructura:
            #   pending: patrón activo sin resolver
            #   history: últimos 60 resueltos con números reales y resultado
            #   by_number: por cada last_number → top_pair, efectividad, pairs
            "dozen_seq_patterns": {
                "pending":   self.pending_dozen_seq,
                "history":   dozen_seq_hist,
                "by_number": dozen_seq_by_num,
            },
            "column_seq_patterns": {
                "pending":   self.pending_column_seq,
                "history":   col_seq_hist,
                "by_number": col_seq_by_num,
            },
        }

    async def broadcast_update(self, number: int, game_id: str):
        if not self.ws_clients:
            return
        data    = await self.get_latest_data()
        message = json.dumps({"type": "new_spin", "data": data})
        disconnected = []
        for ws in list(self.ws_clients):
            try:
                await ws.send_str(message)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.ws_clients.pop(ws, None)


engine = StatsEngine()

# ─── HTTP HANDLERS ────────────────────────────────────────────────────────────
async def handle_home(request):
    total = await engine.get_total_spins()
    return web.json_response({
        "status":    "ok",
        "server":    "Immersive Roulette Stats Server v2",
        "roulette":  ROULETTE_NAME,
        "total_spins": total,
        "last_number": engine.last_number,
        "pending_color_signal": engine.pending_color,
        "pending_zone_signal":  engine.pending_zone,
        "ws_clients": len(engine.ws_clients),
    })

async def handle_ping(request):
    return web.json_response({"status": "pong", "ts": time.time()})

async def handle_health(request):
    return web.json_response({
        "status":      "ok",
        "total_spins": await engine.get_total_spins(),
        "last_number": engine.last_number,
        "last_game_id": engine.last_game_id,
    })

async def handle_latest(request):
    """GET /latest/IMMERSIVE — Todo en 1 petición para polling del bot."""
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    return web.json_response(await engine.get_latest_data())

async def handle_stats_dozen(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    return web.json_response(await engine.get_stats_dozen())

async def handle_stats_column(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    return web.json_response(await engine.get_stats_column())

async def handle_stats_color(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    return web.json_response(await engine.get_stats_color())

async def handle_stats_zone(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    return web.json_response(await engine.get_stats_zone())

async def handle_patterns_color(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    history = await engine.get_pattern_history("color", 100)
    summary = await engine.get_pattern_summary("color")
    return web.json_response({
        "pending": engine.pending_color,
        "summary": summary,
        "history": history,
    })

async def handle_patterns_zone(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    history = await engine.get_pattern_history("zone", 100)
    summary = await engine.get_pattern_summary("zone")
    return web.json_response({
        "pending": engine.pending_zone,
        "summary": summary,
        "history": history,
    })

# ─── HANDLERS SEÑALES DOCENAS / COLUMNAS ──────────────────────────────────────

async def handle_signal_dozen_post(request):
    """POST /signals/{roulette}/dozen — Bot registra señal de docena activa."""
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    try:
        data       = await request.json()
        strategy   = str(data.get("strategy", ""))
        pair       = list(data.get("pair", []))
        missing    = int(data.get("missing", 0))
        prob       = float(data.get("prob", 0.0))
        last_number = int(data.get("last_number", 0))
        if not pair:
            return web.json_response({"error": "pair requerido"}, status=400)
        row_id = await engine.register_dozen_signal(strategy, pair, missing, prob, last_number)
        return web.json_response({"ok": True, "row_id": row_id})
    except Exception as e:
        logger.error(f"❌ Error registrando señal docena: {e}")
        return web.json_response({"error": str(e)}, status=400)

async def handle_signal_column_post(request):
    """POST /signals/{roulette}/column — Bot registra señal de columna activa."""
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    try:
        data       = await request.json()
        strategy   = str(data.get("strategy", ""))
        pair       = list(data.get("pair", []))
        missing    = int(data.get("missing", 0))
        prob       = float(data.get("prob", 0.0))
        last_number = int(data.get("last_number", 0))
        if not pair:
            return web.json_response({"error": "pair requerido"}, status=400)
        row_id = await engine.register_column_signal(strategy, pair, missing, prob, last_number)
        return web.json_response({"ok": True, "row_id": row_id})
    except Exception as e:
        logger.error(f"❌ Error registrando señal columna: {e}")
        return web.json_response({"error": str(e)}, status=400)

async def handle_signal_dozen_get(request):
    """GET /signals/{roulette}/dozen — Estadísticas de señales de docenas."""
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    summary  = await engine.get_signal_summary("dozen")
    by_number = await engine.get_signal_stats_by_number("dozen")
    return web.json_response({
        "pending":   engine.pending_dozen_signal,
        "summary":   summary,
        "by_number": by_number,
    })

async def handle_signal_column_get(request):
    """GET /signals/{roulette}/column — Estadísticas de señales de columnas."""
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    summary   = await engine.get_signal_summary("column")
    by_number = await engine.get_signal_stats_by_number("column")
    return web.json_response({
        "pending":   engine.pending_column_signal,
        "summary":   summary,
        "by_number": by_number,
    })

async def handle_spins(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    try:
        n = min(int(request.match_info.get("n", "20")), 200)
    except Exception:
        n = 20
    return web.json_response(await engine.get_last_n(n))

async def handle_websocket(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await ws.send_str(json.dumps({
        "type": "welcome",
        "roulette": ROULETTE,
        "roulette_name": ROULETTE_NAME,
    }))
    engine.ws_clients[ws] = True
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    if data.get("type") == "subscribe":
                        full = await engine.get_latest_data()
                        await ws.send_str(json.dumps({"type": "full_state", "data": full}))
                except Exception:
                    pass
            elif msg.type == WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        engine.ws_clients.pop(ws, None)
    return ws

# ─── POLLER EVOLUTION API ─────────────────────────────────────────────────────
async def poll_evolution():
    """
    Poller original con timing adaptativo.

    Lógica añadida (sin cambiar el flujo base):
      - Parsea settledAt de cada payload para calcular el intervalo real entre giros.
      - Tras registrar un giro, duerme el 80 % del intervalo calculado menos el
        tiempo ya transcurrido desde que Evolution publicó el resultado.
        Esto evita bombardear la API durante la ventana en que no puede haber
        un giro nuevo, eliminando la exposición al error 500 recurrente.
      - Al despertar (o sin intervalo conocido todavía) consulta cada POLL_SECS = 1 s
        hasta detectar el siguiente giro.
      - Fix: el backoff exponencial ahora aplica también a respuestas HTTP no-200
        (antes solo se aplicaba a errores de red).
    """
    recon           = 5
    last_id         = engine.last_game_id
    last_settled_ts = 0.0   # settledAt del último giro registrado (Unix ts)
    spin_interval   = 0.0   # intervalo calculado entre los dos últimos giros

    logger.info(f"🎰 Iniciando poller Evolution: {EVOLUTION_URL}")

    async with aiohttp.ClientSession(headers=EVOLUTION_HEADERS) as session:
        while True:
            try:
                async with session.get(
                    EVOLUTION_URL,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"⚠️ API status: {resp.status}")
                        await asyncio.sleep(recon)          # backoff en HTTP errors
                        recon = min(recon * 2, 60)
                        continue

                    payload = await resp.json(content_type=None)
                    recon   = 5

                    game_id = str(payload.get("id", ""))
                    if not game_id or game_id == last_id:
                        await asyncio.sleep(POLL_SECS)
                        continue

                    # Verificar que está resuelto
                    data   = payload.get("data", {})
                    status = data.get("status", "")
                    if status != "Resolved":
                        await asyncio.sleep(POLL_SECS)
                        continue

                    # Extraer número
                    outcome = data.get("result", {}).get("outcome", {})
                    number  = outcome.get("number")
                    if number is None:
                        await asyncio.sleep(POLL_SECS)
                        continue

                    number = int(number)
                    if not (0 <= number <= 36):
                        await asyncio.sleep(POLL_SECS)
                        continue

                    # ── Calcular intervalo desde settledAt ────────────────────
                    current_settled_ts = parse_settled_at(data.get("settledAt", ""))
                    if current_settled_ts == 0.0:
                        current_settled_ts = time.time()   # fallback si no hay campo

                    if last_settled_ts > 0 and current_settled_ts > last_settled_ts:
                        spin_interval = current_settled_ts - last_settled_ts
                        logger.info(
                            f"[Poller] ⏱️ Intervalo entre giros: {spin_interval:.1f}s"
                        )

                    last_settled_ts = current_settled_ts

                    # ── Registrar giro ────────────────────────────────────────
                    last_id = game_id
                    if await engine.process_spin(number, game_id):
                        await engine.broadcast_update(number, game_id)

                    # ── Sleep adaptativo ──────────────────────────────────────
                    # Dormimos hasta el 80 % del intervalo típico, descontando
                    # el tiempo que ya pasó desde que Evolution publicó el giro.
                    # Con POLL_SECS = 1 s el siguiente ciclo arranca en modo
                    # vigilancia rápida automáticamente.
                    if spin_interval > 5:
                        elapsed = time.time() - current_settled_ts
                        safe_sleep = max(spin_interval * 0.80 - elapsed, 0.0)
                        if safe_sleep > 1:
                            logger.debug(
                                f"[Poller] 😴 Sleep adaptativo {safe_sleep:.1f}s "
                                f"(intervalo={spin_interval:.1f}s, elapsed={elapsed:.1f}s)"
                            )
                            await asyncio.sleep(safe_sleep)
                    continue   # siguiente ciclo sin sleep extra → poll cada 1 s

            except aiohttp.ClientError as e:
                logger.warning(f"⚠️ Error de red: {e}. Reconectando en {recon}s")
                await asyncio.sleep(recon)
                recon = min(recon * 2, 60)
                continue
            except Exception as e:
                logger.error(f"❌ Error inesperado en poller: {e}")
                await asyncio.sleep(recon)

            await asyncio.sleep(POLL_SECS)

# ─── SELF-PING ────────────────────────────────────────────────────────────────
async def self_ping_loop():
    url = RENDER_URL.rstrip("/")
    if not url or "localhost" in url:
        return
    await asyncio.sleep(30)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f"{url}/ping", timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status == 200:
                        logger.debug("✅ Self-ping OK")
            except Exception as e:
                logger.debug(f"Ping error: {e}")
            await asyncio.sleep(240)

# ─── APP ──────────────────────────────────────────────────────────────────────
async def start_tasks(app):
    app["task_poller"] = asyncio.create_task(poll_evolution())
    app["task_ping"]   = asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas de fondo iniciadas")

async def stop_tasks(app):
    for k in ["task_poller", "task_ping"]:
        t = app.get(k)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/",                           handle_home)
    app.router.add_get("/ping",                       handle_ping)
    app.router.add_get("/health",                     handle_health)
    app.router.add_get("/latest/{roulette}",          handle_latest)
    app.router.add_get("/stats/{roulette}/dozen",     handle_stats_dozen)
    app.router.add_get("/stats/{roulette}/column",    handle_stats_column)
    app.router.add_get("/stats/{roulette}/color",     handle_stats_color)
    app.router.add_get("/stats/{roulette}/zone",      handle_stats_zone)
    app.router.add_get("/patterns/{roulette}/color",  handle_patterns_color)
    app.router.add_get("/patterns/{roulette}/zone",   handle_patterns_zone)
    app.router.add_post("/signals/{roulette}/dozen",  handle_signal_dozen_post)
    app.router.add_post("/signals/{roulette}/column", handle_signal_column_post)
    app.router.add_get("/signals/{roulette}/dozen",   handle_signal_dozen_get)
    app.router.add_get("/signals/{roulette}/column",  handle_signal_column_get)
    app.router.add_get("/spins/{roulette}/{n}",       handle_spins)
    app.router.add_get("/ws",                         handle_websocket)
    app.on_startup.append(start_tasks)
    app.on_cleanup.append(stop_tasks)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10004))
    web.run_app(create_app(), host="0.0.0.0", port=port, access_log=None)
