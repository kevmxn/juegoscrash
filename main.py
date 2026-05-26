#!/usr/bin/env python3
"""
Immersive Roulette Stats Server v4
===========================================================================
CAMBIOS vs v3:
  - Nueva lógica de guardado de patrones con multi-intento (hasta 3):
      * E1 Docenas/Columnas: últimos 5 no-cero con 2 docenas/columnas
      * E2 Docenas/Columnas: triple repetición de la misma docena/columna
      * E3 Docenas/Columnas: E1 + racha de la docena/columna faltante
      * Color/Zona patrones 1-4: secuencia + números reales + multi-intento
  - attempt_log JSON en cada patrón: [{attempt, number, result}, ...]
  - final_result TEXT: "WIN" | "LOSS" (resuelto)
  - Detección E2/E3 automática en el servidor
  - Tablas renombradas: dozen_seq_patterns → e1_dozen_patterns (con migración)
  - El servidor expone todos los patrones en /latest para el bot
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
POLL_SECS       = 2
DEFAULT_WAIT    = 20
DEFAULT_POLL    = 2
RENDER_URL      = os.environ.get("RENDER_EXTERNAL_URL", "")
STATS_DB        = "immersive_stats.db"
MAX_STORED      = 500
ROULETTE        = "IMMERSIVE"
ROULETTE_NAME   = "Immersive Roulette"
MAX_ATTEMPTS    = 3   # intentos máximos por señal/patrón

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
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0

# ─── PATRONES COLOR/ZONA ──────────────────────────────────────────────────────
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

        # Spins
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

        # Transiciones docenas/columnas
        conn.execute("""CREATE TABLE IF NOT EXISTS transitions (
            from_number INTEGER PRIMARY KEY,
            d0 INTEGER DEFAULT 0, d1 INTEGER DEFAULT 0,
            d2 INTEGER DEFAULT 0, d3 INTEGER DEFAULT 0,
            c0 INTEGER DEFAULT 0, c1 INTEGER DEFAULT 0,
            c2 INTEGER DEFAULT 0, c3 INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0
        )""")

        # Transiciones COLOR
        conn.execute("""CREATE TABLE IF NOT EXISTS color_transitions (
            from_number INTEGER PRIMARY KEY,
            cnt_R INTEGER DEFAULT 0,
            cnt_N INTEGER DEFAULT 0,
            cnt_V INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0
        )""")

        # Transiciones ZONA
        conn.execute("""CREATE TABLE IF NOT EXISTS zone_transitions (
            from_number INTEGER PRIMARY KEY,
            cnt_B INTEGER DEFAULT 0,
            cnt_A INTEGER DEFAULT 0,
            cnt_Z INTEGER DEFAULT 0,
            total INTEGER DEFAULT 0
        )""")

        # ── Patrones COLOR (multi-intento) ────────────────────────────────────
        # attempt_log JSON: [{"attempt":1,"number":30,"result":"LOSS"},{"attempt":2,"number":22,"result":"WIN"}]
        # sequence_json: números reales del patrón ej [2, 4, 15, 30, 22]
        conn.execute("""CREATE TABLE IF NOT EXISTS color_pattern_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_id    TEXT    NOT NULL,
            pattern_str   TEXT    NOT NULL,
            bet           TEXT    NOT NULL,
            sequence_json TEXT    NOT NULL,
            attempt_log   TEXT    NOT NULL DEFAULT '[]',
            final_result  TEXT,
            ts            INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cph_pid ON color_pattern_history(pattern_id)")

        # ── Patrones ZONA (multi-intento) ─────────────────────────────────────
        conn.execute("""CREATE TABLE IF NOT EXISTS zone_pattern_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_id    TEXT    NOT NULL,
            pattern_str   TEXT    NOT NULL,
            bet           TEXT    NOT NULL,
            sequence_json TEXT    NOT NULL,
            attempt_log   TEXT    NOT NULL DEFAULT '[]',
            final_result  TEXT,
            ts            INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_zph_pid ON zone_pattern_history(pattern_id)")

        # ── Señales DOCENAS (bot → servidor) ──────────────────────────────────
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

        # ── Señales COLUMNAS (bot → servidor) ─────────────────────────────────
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

        # ── E1 Docenas: últimos 5 no-cero con exactamente 2 docenas ──────────
        # pattern_str: "D1,D2,D2,D1,D2"  numbers_json: [6,13,15,8,23]
        # attempt_log: [{attempt,number,result}, ...]   final_result: WIN|LOSS
        conn.execute("""CREATE TABLE IF NOT EXISTS e1_dozen_patterns (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_json    TEXT    NOT NULL,
            missing      INTEGER NOT NULL,
            pattern_str  TEXT    NOT NULL,
            numbers_json TEXT    NOT NULL,
            last_number  INTEGER NOT NULL,
            attempt_log  TEXT    NOT NULL DEFAULT '[]',
            final_result TEXT,
            ts           INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e1dp_ln  ON e1_dozen_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e1dp_res ON e1_dozen_patterns(final_result)")

        # ── E2 Docenas: triple repetición de la misma docena ─────────────────
        # pattern_str: "D3,D3,D3"  numbers_json: [32,34,25]
        # pair = las otras 2 docenas (apuesta)  triple_dozen = la que se repitió
        conn.execute("""CREATE TABLE IF NOT EXISTS e2_dozen_patterns (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            triple_dozen  INTEGER NOT NULL,
            pair_json     TEXT    NOT NULL,
            pattern_str   TEXT    NOT NULL,
            numbers_json  TEXT    NOT NULL,
            last_number   INTEGER NOT NULL,
            attempt_log   TEXT    NOT NULL DEFAULT '[]',
            final_result  TEXT,
            ts            INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e2dp_ln  ON e2_dozen_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e2dp_res ON e2_dozen_patterns(final_result)")

        # ── E3 Docenas: E1 (5 últimos en 2 docenas) + racha de docena faltante
        # e1_pattern_str: "D1,D2,D2,D1,D2"  e1_numbers_json: [6,13,15,8,23]
        # e2_pattern_str: "D3,D3"            e2_numbers_json: [32,34]
        conn.execute("""CREATE TABLE IF NOT EXISTS e3_dozen_patterns (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_json        TEXT    NOT NULL,
            missing          INTEGER NOT NULL,
            e1_pattern_str   TEXT    NOT NULL,
            e1_numbers_json  TEXT    NOT NULL,
            e2_pattern_str   TEXT    NOT NULL,
            e2_numbers_json  TEXT    NOT NULL,
            last_number      INTEGER NOT NULL,
            attempt_log      TEXT    NOT NULL DEFAULT '[]',
            final_result     TEXT,
            ts               INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e3dp_ln  ON e3_dozen_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e3dp_res ON e3_dozen_patterns(final_result)")

        # ── E1 Columnas ───────────────────────────────────────────────────────
        conn.execute("""CREATE TABLE IF NOT EXISTS e1_column_patterns (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_json    TEXT    NOT NULL,
            missing      INTEGER NOT NULL,
            pattern_str  TEXT    NOT NULL,
            numbers_json TEXT    NOT NULL,
            last_number  INTEGER NOT NULL,
            attempt_log  TEXT    NOT NULL DEFAULT '[]',
            final_result TEXT,
            ts           INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e1cp_ln  ON e1_column_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e1cp_res ON e1_column_patterns(final_result)")

        # ── E2 Columnas ───────────────────────────────────────────────────────
        conn.execute("""CREATE TABLE IF NOT EXISTS e2_column_patterns (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            triple_column  INTEGER NOT NULL,
            pair_json      TEXT    NOT NULL,
            pattern_str    TEXT    NOT NULL,
            numbers_json   TEXT    NOT NULL,
            last_number    INTEGER NOT NULL,
            attempt_log    TEXT    NOT NULL DEFAULT '[]',
            final_result   TEXT,
            ts             INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e2cp_ln  ON e2_column_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e2cp_res ON e2_column_patterns(final_result)")

        # ── E3 Columnas ───────────────────────────────────────────────────────
        conn.execute("""CREATE TABLE IF NOT EXISTS e3_column_patterns (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            pair_json        TEXT    NOT NULL,
            missing          INTEGER NOT NULL,
            e1_pattern_str   TEXT    NOT NULL,
            e1_numbers_json  TEXT    NOT NULL,
            e2_pattern_str   TEXT    NOT NULL,
            e2_numbers_json  TEXT    NOT NULL,
            last_number      INTEGER NOT NULL,
            attempt_log      TEXT    NOT NULL DEFAULT '[]',
            final_result     TEXT,
            ts               INTEGER NOT NULL
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e3cp_ln  ON e3_column_patterns(last_number)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_e3cp_res ON e3_column_patterns(final_result)")

        conn.commit()

        # ── Migración desde tablas antiguas ───────────────────────────────────
        self._migrate(conn)

        conn.close()
        logger.info(f"✅ DB inicializada: {self.db_path}")

    def _migrate(self, conn):
        """Migra de schema antiguo (v2/v3) al nuevo (v4)."""
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}

        # Renombrar dozen_seq_patterns → e1_dozen_patterns
        if "dozen_seq_patterns" in tables and "e1_dozen_patterns" not in tables:
            logger.info("[MIGRATE] Renombrando dozen_seq_patterns → e1_dozen_patterns")
            conn.execute("ALTER TABLE dozen_seq_patterns RENAME TO e1_dozen_patterns")
            for col in ["attempt_log TEXT NOT NULL DEFAULT '[]'", "final_result TEXT"]:
                try: conn.execute(f"ALTER TABLE e1_dozen_patterns ADD COLUMN {col}")
                except Exception: pass

        # Renombrar column_seq_patterns → e1_column_patterns
        if "column_seq_patterns" in tables and "e1_column_patterns" not in tables:
            logger.info("[MIGRATE] Renombrando column_seq_patterns → e1_column_patterns")
            conn.execute("ALTER TABLE column_seq_patterns RENAME TO e1_column_patterns")
            for col in ["attempt_log TEXT NOT NULL DEFAULT '[]'", "final_result TEXT"]:
                try: conn.execute(f"ALTER TABLE e1_column_patterns ADD COLUMN {col}")
                except Exception: pass

        # Añadir columnas faltantes a color/zone
        for table in ["color_pattern_history", "zone_pattern_history"]:
            for col in ["attempt_log TEXT NOT NULL DEFAULT '[]'", "final_result TEXT"]:
                try: conn.execute(f"ALTER TABLE {table} ADD COLUMN {col}")
                except Exception: pass

        # Añadir attempt_log/final_result a e1 tables si ya existen sin esas cols
        for table in ["e1_dozen_patterns", "e1_column_patterns"]:
            for col in ["attempt_log TEXT NOT NULL DEFAULT '[]'", "final_result TEXT"]:
                try: conn.execute(f"ALTER TABLE {table} ADD COLUMN {col}")
                except Exception: pass

        conn.commit()

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
        self.last_game_id: str           = ""
        self.last_number:  Optional[int] = None

        # Secuencias en memoria
        self.number_seq: List[int] = []
        self.color_seq:  List[str] = []
        self.zone_seq:   List[str] = []

        # ── Señales bot pendientes ────────────────────────────────────────────
        self.pending_dozen_signal:  Optional[dict] = None
        self.pending_column_signal: Optional[dict] = None

        # ── Patrones COLOR/ZONA pendientes (multi-intento) ───────────────────
        # {pid, p_str, bet, sequence, row_id, attempt, attempt_log}
        self.pending_color: Optional[dict] = None
        self.pending_zone:  Optional[dict] = None

        # ── Patrones E1 pendientes (multi-intento) ────────────────────────────
        # {row_id, pair, missing, pattern_str, numbers, last_number, attempt, attempt_log}
        self.pending_e1_dozen:  Optional[dict] = None
        self.pending_e1_column: Optional[dict] = None

        # ── Patrones E2 pendientes (multi-intento) ────────────────────────────
        # {row_id, triple_cat, pair, pattern_str, numbers, last_number, attempt, attempt_log}
        self.pending_e2_dozen:  Optional[dict] = None
        self.pending_e2_column: Optional[dict] = None

        # ── Patrones E3 pendientes (multi-intento) ────────────────────────────
        # {row_id, pair, missing, e1_pattern, e1_numbers, e2_pattern, e2_numbers,
        #  last_number, attempt, attempt_log}
        self.pending_e3_dozen:  Optional[dict] = None
        self.pending_e3_column: Optional[dict] = None

        # WebSocket clientes
        self.ws_clients: dict = {}

        self._load_state()

    def _load_state(self):
        conn = sqlite3.connect(db.db_path)
        conn.row_factory = sqlite3.Row
        try:
            # Último spin
            row = conn.execute(
                "SELECT game_id, number FROM spins ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.last_game_id = row["game_id"]
                self.last_number  = row["number"]

            # Reconstruir secuencias desde los últimos 100 spins
            rows = conn.execute(
                "SELECT number, color, zone FROM spins ORDER BY id DESC LIMIT 100"
            ).fetchall()
            rows = list(reversed(rows))
            self.number_seq = [r["number"] for r in rows]
            self.color_seq  = [r["color"]  for r in rows]
            self.zone_seq   = [r["zone"]   for r in rows]
            logger.info(f"[{ROULETTE}] Secuencia cargada: {len(rows)} spins")

            # Señal color pendiente
            row = conn.execute(
                "SELECT id, pattern_id, pattern_str, bet, sequence_json "
                "FROM color_pattern_history WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_color = {
                    "pid": row["pattern_id"], "p_str": row["pattern_str"],
                    "bet": row["bet"], "sequence": json.loads(row["sequence_json"]),
                    "row_id": row["id"], "attempt": 1, "attempt_log": [],
                }

            # Señal zona pendiente
            row = conn.execute(
                "SELECT id, pattern_id, pattern_str, bet, sequence_json "
                "FROM zone_pattern_history WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_zone = {
                    "pid": row["pattern_id"], "p_str": row["pattern_str"],
                    "bet": row["bet"], "sequence": json.loads(row["sequence_json"]),
                    "row_id": row["id"], "attempt": 1, "attempt_log": [],
                }

            # Señal docena bot pendiente
            row = conn.execute(
                "SELECT id, strategy, pair_json, missing, last_number "
                "FROM dozen_signal_history WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_dozen_signal = {
                    "row_id": row["id"], "strategy": row["strategy"],
                    "pair": json.loads(row["pair_json"]),
                    "missing": row["missing"], "last_number": row["last_number"],
                }

            # Señal columna bot pendiente
            row = conn.execute(
                "SELECT id, strategy, pair_json, missing, last_number "
                "FROM column_signal_history WHERE result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.pending_column_signal = {
                    "row_id": row["id"], "strategy": row["strategy"],
                    "pair": json.loads(row["pair_json"]),
                    "missing": row["missing"], "last_number": row["last_number"],
                }

            # E1 Docena pendiente
            row = conn.execute(
                "SELECT id, pair_json, missing, pattern_str, numbers_json, last_number, attempt_log "
                "FROM e1_dozen_patterns WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                alog = json.loads(row["attempt_log"] or "[]")
                self.pending_e1_dozen = {
                    "row_id": row["id"], "pair": json.loads(row["pair_json"]),
                    "missing": row["missing"], "pattern_str": row["pattern_str"],
                    "numbers": json.loads(row["numbers_json"]),
                    "last_number": row["last_number"],
                    "attempt": len(alog) + 1, "attempt_log": alog,
                }

            # E1 Columna pendiente
            row = conn.execute(
                "SELECT id, pair_json, missing, pattern_str, numbers_json, last_number, attempt_log "
                "FROM e1_column_patterns WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                alog = json.loads(row["attempt_log"] or "[]")
                self.pending_e1_column = {
                    "row_id": row["id"], "pair": json.loads(row["pair_json"]),
                    "missing": row["missing"], "pattern_str": row["pattern_str"],
                    "numbers": json.loads(row["numbers_json"]),
                    "last_number": row["last_number"],
                    "attempt": len(alog) + 1, "attempt_log": alog,
                }

            # E2 Docena pendiente
            row = conn.execute(
                "SELECT id, triple_dozen, pair_json, pattern_str, numbers_json, last_number, attempt_log "
                "FROM e2_dozen_patterns WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                alog = json.loads(row["attempt_log"] or "[]")
                self.pending_e2_dozen = {
                    "row_id": row["id"], "triple_dozen": row["triple_dozen"],
                    "pair": json.loads(row["pair_json"]),
                    "pattern_str": row["pattern_str"],
                    "numbers": json.loads(row["numbers_json"]),
                    "last_number": row["last_number"],
                    "attempt": len(alog) + 1, "attempt_log": alog,
                }

            # E2 Columna pendiente
            row = conn.execute(
                "SELECT id, triple_column, pair_json, pattern_str, numbers_json, last_number, attempt_log "
                "FROM e2_column_patterns WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                alog = json.loads(row["attempt_log"] or "[]")
                self.pending_e2_column = {
                    "row_id": row["id"], "triple_column": row["triple_column"],
                    "pair": json.loads(row["pair_json"]),
                    "pattern_str": row["pattern_str"],
                    "numbers": json.loads(row["numbers_json"]),
                    "last_number": row["last_number"],
                    "attempt": len(alog) + 1, "attempt_log": alog,
                }

            # E3 Docena pendiente
            row = conn.execute(
                "SELECT id, pair_json, missing, e1_pattern_str, e1_numbers_json, "
                "e2_pattern_str, e2_numbers_json, last_number, attempt_log "
                "FROM e3_dozen_patterns WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                alog = json.loads(row["attempt_log"] or "[]")
                self.pending_e3_dozen = {
                    "row_id": row["id"], "pair": json.loads(row["pair_json"]),
                    "missing": row["missing"],
                    "e1_pattern": row["e1_pattern_str"],
                    "e1_numbers": json.loads(row["e1_numbers_json"]),
                    "e2_pattern": row["e2_pattern_str"],
                    "e2_numbers": json.loads(row["e2_numbers_json"]),
                    "last_number": row["last_number"],
                    "attempt": len(alog) + 1, "attempt_log": alog,
                }

            # E3 Columna pendiente
            row = conn.execute(
                "SELECT id, pair_json, missing, e1_pattern_str, e1_numbers_json, "
                "e2_pattern_str, e2_numbers_json, last_number, attempt_log "
                "FROM e3_column_patterns WHERE final_result IS NULL ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                alog = json.loads(row["attempt_log"] or "[]")
                self.pending_e3_column = {
                    "row_id": row["id"], "pair": json.loads(row["pair_json"]),
                    "missing": row["missing"],
                    "e1_pattern": row["e1_pattern_str"],
                    "e1_numbers": json.loads(row["e1_numbers_json"]),
                    "e2_pattern": row["e2_pattern_str"],
                    "e2_numbers": json.loads(row["e2_numbers_json"]),
                    "last_number": row["last_number"],
                    "attempt": len(alog) + 1, "attempt_log": alog,
                }

            logger.info(f"[{ROULETTE}] Estado cargado desde DB")
        finally:
            conn.close()

    # ── Resolución multi-intento genérica ─────────────────────────────────────
    async def _resolve_attempt(
        self, pending: dict, table: str, matched: bool, number: int,
        extra_fields: dict = None
    ) -> Optional[dict]:
        """
        Actualiza attempt_log. Si WIN o intento >= MAX_ATTEMPTS → final_result.
        Retorna el pending actualizado (None si terminado).
        """
        result_str = "WIN" if matched else "LOSS"
        attempt    = pending.get("attempt", 1)
        alog       = list(pending.get("attempt_log", []))
        alog.append({"attempt": attempt, "number": number, "result": result_str})

        if matched or attempt >= MAX_ATTEMPTS:
            final = "WIN" if matched else "LOSS"
            # Construir SET extras (e.g. next_dozen=?)
            extra_set = ""
            extra_vals = []
            if extra_fields:
                for k, v in extra_fields.items():
                    extra_set += f", {k}=?"
                    extra_vals.append(v)
            await db.write(
                f"UPDATE {table} SET attempt_log=?, final_result=?{extra_set} WHERE id=?",
                (json.dumps(alog), final, *extra_vals, pending["row_id"])
            )
            icon = "✅" if matched else "❌"
            logger.info(
                f"[{table}] {icon} #{number} → {final} (intento {attempt}/{MAX_ATTEMPTS})"
            )
            return None  # señal terminada
        else:
            # Seguir esperando siguiente intento
            pending["attempt"]     = attempt + 1
            pending["attempt_log"] = alog
            await db.write(
                f"UPDATE {table} SET attempt_log=? WHERE id=?",
                (json.dumps(alog), pending["row_id"])
            )
            logger.info(
                f"[{table}] 🔁 Intento {attempt} LOSS #{number} → esperando intento {attempt+1}"
            )
            return pending  # señal sigue activa

    # ── Procesamiento de spin ─────────────────────────────────────────────────
    async def process_spin(self, number: int, game_id: str) -> bool:
        existing = await db.fetchone("SELECT 1 FROM spins WHERE game_id=?", (game_id,))
        if existing:
            return False

        color = get_color(number)
        zone  = get_zone(number)
        d     = get_dozen(number)
        c     = get_column(number)
        prev  = self.last_number

        ops = []
        ops.append((
            "INSERT INTO spins(game_id, number, color, zone, ts) VALUES(?,?,?,?,?)",
            (game_id, number, color, zone, int(time.time()))
        ))

        if prev is not None and prev != 0:
            ops.append(("INSERT OR IGNORE INTO transitions(from_number) VALUES(?)", (prev,)))
            ops.append((
                f"UPDATE transitions SET d{d}=d{d}+1, c{c}=c{c}+1, total=total+1 WHERE from_number=?",
                (prev,)
            ))
            ops.append(("INSERT OR IGNORE INTO color_transitions(from_number) VALUES(?)", (prev,)))
            ops.append((
                f"UPDATE color_transitions SET cnt_{color}=cnt_{color}+1, total=total+1 WHERE from_number=?",
                (prev,)
            ))
            ops.append(("INSERT OR IGNORE INTO zone_transitions(from_number) VALUES(?)", (prev,)))
            ops.append((
                f"UPDATE zone_transitions SET cnt_{zone}=cnt_{zone}+1, total=total+1 WHERE from_number=?",
                (prev,)
            ))

        await db.write_many(ops)

        # 1. Resolver señales pendientes ANTES de detectar nuevas
        await self._resolve_pending_color(number, color)
        await self._resolve_pending_zone(number, zone)
        await self._resolve_pending_dozen_signal(number, d)
        await self._resolve_pending_column_signal(number, c)
        await self._resolve_pending_e1_dozen(number, d)
        await self._resolve_pending_e2_dozen(number, d)
        await self._resolve_pending_e3_dozen(number, d)
        await self._resolve_pending_e1_column(number, c)
        await self._resolve_pending_e2_column(number, c)
        await self._resolve_pending_e3_column(number, c)

        # 2. Actualizar secuencias en memoria
        self.number_seq.append(number)
        self.color_seq.append(color)
        self.zone_seq.append(zone)
        if len(self.number_seq) > 200:
            self.number_seq.pop(0)
            self.color_seq.pop(0)
            self.zone_seq.pop(0)

        # 3. Detectar nuevos patrones (después de actualizar secuencias)
        await self._detect_color_pattern()
        await self._detect_zone_pattern()
        await self._detect_e1_dozen_pattern(number)
        await self._detect_e2_dozen_pattern(number)
        await self._detect_e3_dozen_pattern(number)
        await self._detect_e1_column_pattern(number)
        await self._detect_e2_column_pattern(number)
        await self._detect_e3_column_pattern(number)

        # 4. Actualizar estado
        self.last_number  = number
        self.last_game_id = game_id

        # 5. Limpiar spins viejos
        await db.write(
            "DELETE FROM spins WHERE id NOT IN (SELECT id FROM spins ORDER BY id DESC LIMIT ?)",
            (MAX_STORED,)
        )

        logger.info(
            f"[{ROULETTE}] 🎰 #{number} {color}/{zone} | D{d} C{c} | gid={game_id[:12]}..."
        )
        return True

    # ── Resolución señales pendientes ─────────────────────────────────────────
    async def _resolve_pending_color(self, number: int, color: str):
        if not self.pending_color: return
        matched = color_matches_bet(color, self.pending_color["bet"])
        self.pending_color = await self._resolve_attempt(
            self.pending_color, "color_pattern_history", matched, number
        )

    async def _resolve_pending_zone(self, number: int, zone: str):
        if not self.pending_zone: return
        matched = zone_matches_bet(zone, self.pending_zone["bet"])
        self.pending_zone = await self._resolve_attempt(
            self.pending_zone, "zone_pattern_history", matched, number
        )

    async def _resolve_pending_dozen_signal(self, number: int, dozen: int):
        if not self.pending_dozen_signal: return
        pair   = self.pending_dozen_signal["pair"]
        result = "Acierto" if (dozen != 0 and dozen in pair) else "Fallo"
        await db.write(
            "UPDATE dozen_signal_history SET next_number=?, next_dozen=?, result=? WHERE id=?",
            (number, dozen, result, self.pending_dozen_signal["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(f"[DOCENA-SIG] {icon} par={pair} → #{number}(D{dozen}) → {result}")
        self.pending_dozen_signal = None

    async def _resolve_pending_column_signal(self, number: int, column: int):
        if not self.pending_column_signal: return
        pair   = self.pending_column_signal["pair"]
        result = "Acierto" if (column != 0 and column in pair) else "Fallo"
        await db.write(
            "UPDATE column_signal_history SET next_number=?, next_column=?, result=? WHERE id=?",
            (number, column, result, self.pending_column_signal["row_id"])
        )
        icon = "✅" if result == "Acierto" else "❌"
        logger.info(f"[COLUMNA-SIG] {icon} par={pair} → #{number}(C{column}) → {result}")
        self.pending_column_signal = None

    # ── Resolución patrones E1/E2/E3 ─────────────────────────────────────────
    async def _resolve_pending_e1_dozen(self, number: int, dozen: int):
        if not self.pending_e1_dozen: return
        pair    = self.pending_e1_dozen["pair"]
        matched = dozen != 0 and dozen in pair
        self.pending_e1_dozen = await self._resolve_attempt(
            self.pending_e1_dozen, "e1_dozen_patterns", matched, number
        )

    async def _resolve_pending_e2_dozen(self, number: int, dozen: int):
        if not self.pending_e2_dozen: return
        pair    = self.pending_e2_dozen["pair"]
        # E2: gana si cae una docena del par (las contrarias a la repetida)
        matched = dozen != 0 and dozen in pair
        self.pending_e2_dozen = await self._resolve_attempt(
            self.pending_e2_dozen, "e2_dozen_patterns", matched, number
        )

    async def _resolve_pending_e3_dozen(self, number: int, dozen: int):
        if not self.pending_e3_dozen: return
        pair    = self.pending_e3_dozen["pair"]
        matched = dozen != 0 and dozen in pair
        self.pending_e3_dozen = await self._resolve_attempt(
            self.pending_e3_dozen, "e3_dozen_patterns", matched, number
        )

    async def _resolve_pending_e1_column(self, number: int, column: int):
        if not self.pending_e1_column: return
        pair    = self.pending_e1_column["pair"]
        matched = column != 0 and column in pair
        self.pending_e1_column = await self._resolve_attempt(
            self.pending_e1_column, "e1_column_patterns", matched, number
        )

    async def _resolve_pending_e2_column(self, number: int, column: int):
        if not self.pending_e2_column: return
        pair    = self.pending_e2_column["pair"]
        matched = column != 0 and column in pair
        self.pending_e2_column = await self._resolve_attempt(
            self.pending_e2_column, "e2_column_patterns", matched, number
        )

    async def _resolve_pending_e3_column(self, number: int, column: int):
        if not self.pending_e3_column: return
        pair    = self.pending_e3_column["pair"]
        matched = column != 0 and column in pair
        self.pending_e3_column = await self._resolve_attempt(
            self.pending_e3_column, "e3_column_patterns", matched, number
        )

    # ── Detección patrones COLOR/ZONA ─────────────────────────────────────────
    async def _detect_color_pattern(self):
        if self.pending_color: return
        non_zero_colors = [
            self.color_seq[i]
            for i in range(len(self.color_seq))
            if self.color_seq[i] != "V"
        ]
        matched = check_patterns(non_zero_colors, COLOR_PATTERNS)
        if not matched: return

        plen    = len(matched["p"])
        nz_nums = [
            self.number_seq[i]
            for i in range(len(self.number_seq))
            if self.color_seq[i] != "V"
        ]
        seq_nums = nz_nums[-plen:] if len(nz_nums) >= plen else nz_nums
        p_str    = ",".join(matched["p"])

        row_id = await db.write(
            "INSERT INTO color_pattern_history"
            "(pattern_id, pattern_str, bet, sequence_json, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?)",
            (matched["id"], p_str, matched["bet"],
             json.dumps(seq_nums), "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_color = {
            "pid": matched["id"], "p_str": p_str,
            "bet": matched["bet"], "sequence": seq_nums,
            "row_id": row_id, "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[COLOR] 🔔 Patrón {matched['id']} [{p_str}] → {matched['bet']} | nums={seq_nums}"
        )

    async def _detect_zone_pattern(self):
        if self.pending_zone: return
        non_zero_zones = [
            self.zone_seq[i]
            for i in range(len(self.zone_seq))
            if self.zone_seq[i] != "Z"
        ]
        matched = check_patterns(non_zero_zones, ZONE_PATTERNS)
        if not matched: return

        plen    = len(matched["p"])
        nz_nums = [
            self.number_seq[i]
            for i in range(len(self.number_seq))
            if self.zone_seq[i] != "Z"
        ]
        seq_nums = nz_nums[-plen:] if len(nz_nums) >= plen else nz_nums
        p_str    = ",".join(matched["p"])

        row_id = await db.write(
            "INSERT INTO zone_pattern_history"
            "(pattern_id, pattern_str, bet, sequence_json, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?)",
            (matched["id"], p_str, matched["bet"],
             json.dumps(seq_nums), "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_zone = {
            "pid": matched["id"], "p_str": p_str,
            "bet": matched["bet"], "sequence": seq_nums,
            "row_id": row_id, "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[ZONA] 🔔 Patrón {matched['id']} [{p_str}] → {matched['bet']} | nums={seq_nums}"
        )

    # ── Detección E1: últimos 5 no-cero con exactamente 2 docenas ────────────
    async def _detect_e1_dozen_pattern(self, current_number: int):
        if self.pending_e1_dozen: return
        nz = [
            (self.number_seq[i], (self.number_seq[i] - 1) // 12 + 1)
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 5: return

        last5_nums, last5_dozens = zip(*nz[-5:])
        unique = set(last5_dozens)
        if len(unique) != 2: return

        pair        = sorted(unique)
        missing     = list({1, 2, 3} - set(pair))[0]
        pattern_str = ",".join(f"D{d}" for d in last5_dozens)
        numbers_json = list(last5_nums)

        row_id = await db.write(
            "INSERT INTO e1_dozen_patterns"
            "(pair_json, missing, pattern_str, numbers_json, last_number, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?,?)",
            (json.dumps(pair), missing, pattern_str,
             json.dumps(numbers_json), current_number, "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_e1_dozen = {
            "row_id": row_id, "pair": pair, "missing": missing,
            "pattern_str": pattern_str, "numbers": numbers_json,
            "last_number": current_number, "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[E1-DOC] 🔔 [{pattern_str}] → D{pair} falta D{missing} | {numbers_json}"
        )

    # ── Detección E2: triple repetición de la misma docena ───────────────────
    async def _detect_e2_dozen_pattern(self, current_number: int):
        if self.pending_e2_dozen: return
        nz = [
            (self.number_seq[i], (self.number_seq[i] - 1) // 12 + 1)
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 3: return

        last3_nums, last3_dozens = zip(*nz[-3:])
        if len(set(last3_dozens)) != 1: return  # necesitamos triple

        triple_dozen = last3_dozens[0]
        pair         = sorted({1, 2, 3} - {triple_dozen})
        pattern_str  = ",".join(f"D{triple_dozen}" for _ in last3_dozens)
        numbers_json = list(last3_nums)

        row_id = await db.write(
            "INSERT INTO e2_dozen_patterns"
            "(triple_dozen, pair_json, pattern_str, numbers_json, last_number, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?,?)",
            (triple_dozen, json.dumps(pair), pattern_str,
             json.dumps(numbers_json), current_number, "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_e2_dozen = {
            "row_id": row_id, "triple_dozen": triple_dozen,
            "pair": pair, "pattern_str": pattern_str,
            "numbers": numbers_json, "last_number": current_number,
            "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[E2-DOC] 🔔 Triple D{triple_dozen}: [{pattern_str}] → apostar D{pair} | {numbers_json}"
        )

    # ── Detección E3: E1 (5 últimos en 2 docenas) + racha de la faltante ─────
    async def _detect_e3_dozen_pattern(self, current_number: int):
        if self.pending_e3_dozen: return
        nz = [
            self.number_seq[i]
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 6: return

        current = nz[-1]
        current_dozen = (current - 1) // 12 + 1

        # Los 5 anteriores al actual deben formar exactamente 2 docenas
        prev5 = nz[-6:-1]
        prev5_dozens = [(n - 1) // 12 + 1 for n in prev5]
        unique = set(prev5_dozens)
        if len(unique) != 2: return
        if current_dozen in unique: return  # el actual debe ser la faltante

        pair    = sorted(unique)
        missing = current_dozen

        # Racha de la docena faltante al final (incluye actual y anteriores consecutivos)
        e2_nums = []
        for n in reversed(nz):
            if (n - 1) // 12 + 1 == missing:
                e2_nums.insert(0, n)
            else:
                break

        e1_pattern_str = ",".join(f"D{d}" for d in prev5_dozens)
        e2_pattern_str = ",".join(f"D{missing}" for _ in e2_nums)

        row_id = await db.write(
            "INSERT INTO e3_dozen_patterns"
            "(pair_json, missing, e1_pattern_str, e1_numbers_json, "
            "e2_pattern_str, e2_numbers_json, last_number, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (json.dumps(pair), missing,
             e1_pattern_str, json.dumps(list(prev5)),
             e2_pattern_str, json.dumps(e2_nums),
             current_number, "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_e3_dozen = {
            "row_id": row_id, "pair": pair, "missing": missing,
            "e1_pattern": e1_pattern_str, "e1_numbers": list(prev5),
            "e2_pattern": e2_pattern_str, "e2_numbers": e2_nums,
            "last_number": current_number, "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[E3-DOC] 🔔 E1=[{e1_pattern_str}] E2=[{e2_pattern_str}] "
            f"→ D{pair} | last={current_number}"
        )

    # ── Detección E1 Columnas ─────────────────────────────────────────────────
    async def _detect_e1_column_pattern(self, current_number: int):
        if self.pending_e1_column: return
        nz = [
            (self.number_seq[i], ((self.number_seq[i] - 1) % 3) + 1)
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 5: return

        last5_nums, last5_cols = zip(*nz[-5:])
        unique = set(last5_cols)
        if len(unique) != 2: return

        pair        = sorted(unique)
        missing     = list({1, 2, 3} - set(pair))[0]
        pattern_str = ",".join(f"C{c}" for c in last5_cols)
        numbers_json = list(last5_nums)

        row_id = await db.write(
            "INSERT INTO e1_column_patterns"
            "(pair_json, missing, pattern_str, numbers_json, last_number, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?,?)",
            (json.dumps(pair), missing, pattern_str,
             json.dumps(numbers_json), current_number, "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_e1_column = {
            "row_id": row_id, "pair": pair, "missing": missing,
            "pattern_str": pattern_str, "numbers": numbers_json,
            "last_number": current_number, "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[E1-COL] 🔔 [{pattern_str}] → C{pair} falta C{missing} | {numbers_json}"
        )

    # ── Detección E2 Columnas ─────────────────────────────────────────────────
    async def _detect_e2_column_pattern(self, current_number: int):
        if self.pending_e2_column: return
        nz = [
            (self.number_seq[i], ((self.number_seq[i] - 1) % 3) + 1)
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 3: return

        last3_nums, last3_cols = zip(*nz[-3:])
        if len(set(last3_cols)) != 1: return

        triple_col  = last3_cols[0]
        pair        = sorted({1, 2, 3} - {triple_col})
        pattern_str = ",".join(f"C{triple_col}" for _ in last3_cols)
        numbers_json = list(last3_nums)

        row_id = await db.write(
            "INSERT INTO e2_column_patterns"
            "(triple_column, pair_json, pattern_str, numbers_json, last_number, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?,?)",
            (triple_col, json.dumps(pair), pattern_str,
             json.dumps(numbers_json), current_number, "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_e2_column = {
            "row_id": row_id, "triple_column": triple_col,
            "pair": pair, "pattern_str": pattern_str,
            "numbers": numbers_json, "last_number": current_number,
            "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[E2-COL] 🔔 Triple C{triple_col}: [{pattern_str}] → apostar C{pair} | {numbers_json}"
        )

    # ── Detección E3 Columnas ─────────────────────────────────────────────────
    async def _detect_e3_column_pattern(self, current_number: int):
        if self.pending_e3_column: return
        nz = [
            self.number_seq[i]
            for i in range(len(self.number_seq))
            if self.number_seq[i] != 0
        ]
        if len(nz) < 6: return

        current    = nz[-1]
        current_col = ((current - 1) % 3) + 1

        prev5     = nz[-6:-1]
        prev5_cols = [((n - 1) % 3) + 1 for n in prev5]
        unique    = set(prev5_cols)
        if len(unique) != 2: return
        if current_col in unique: return

        pair    = sorted(unique)
        missing = current_col

        e2_nums = []
        for n in reversed(nz):
            if ((n - 1) % 3) + 1 == missing:
                e2_nums.insert(0, n)
            else:
                break

        e1_pattern_str = ",".join(f"C{c}" for c in prev5_cols)
        e2_pattern_str = ",".join(f"C{missing}" for _ in e2_nums)

        row_id = await db.write(
            "INSERT INTO e3_column_patterns"
            "(pair_json, missing, e1_pattern_str, e1_numbers_json, "
            "e2_pattern_str, e2_numbers_json, last_number, attempt_log, ts) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (json.dumps(pair), missing,
             e1_pattern_str, json.dumps(list(prev5)),
             e2_pattern_str, json.dumps(e2_nums),
             current_number, "[]", int(time.time())),
            get_lastrowid=True
        )
        self.pending_e3_column = {
            "row_id": row_id, "pair": pair, "missing": missing,
            "e1_pattern": e1_pattern_str, "e1_numbers": list(prev5),
            "e2_pattern": e2_pattern_str, "e2_numbers": e2_nums,
            "last_number": current_number, "attempt": 1, "attempt_log": [],
        }
        logger.info(
            f"[E3-COL] 🔔 E1=[{e1_pattern_str}] E2=[{e2_pattern_str}] "
            f"→ C{pair} | last={current_number}"
        )

    # ── Registro señales bot ──────────────────────────────────────────────────
    async def register_dozen_signal(
        self, strategy: str, pair: list, missing: int, prob: float, last_number: int
    ) -> int:
        if self.pending_dozen_signal:
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
            f"[DOCENA-SIG] 🎯 strat={strategy} par={pair} missing={missing} last={last_number}"
        )
        return row_id

    async def register_column_signal(
        self, strategy: str, pair: list, missing: int, prob: float, last_number: int
    ) -> int:
        if self.pending_column_signal:
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
            f"[COLUMNA-SIG] 🎯 strat={strategy} par={pair} missing={missing} last={last_number}"
        )
        return row_id

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

    # ── Historial patrones multi-intento ─────────────────────────────────────
    async def _get_multiintent_history(self, table: str, extra_cols: str = "", limit: int = 60) -> List[dict]:
        """Lee historial de cualquier tabla con attempt_log y final_result."""
        rows = await db.fetch(
            f"SELECT *{', ' + extra_cols if extra_cols else ''} "
            f"FROM {table} WHERE final_result IS NOT NULL "
            f"ORDER BY id DESC LIMIT ?",
            (limit,)
        )
        result = []
        for r in rows:
            d = dict(r)
            # Parsear campos JSON
            for field in ["pair_json", "numbers_json", "e1_numbers_json", "e2_numbers_json", "sequence_json"]:
                if field in d and d[field]:
                    try: d[field] = json.loads(d[field])
                    except: pass
            if "attempt_log" in d:
                try: d["attempt_log"] = json.loads(d["attempt_log"] or "[]")
                except: d["attempt_log"] = []
            result.append(d)
        return result

    async def _get_pattern_stats_by_number(self, table: str, last_num_col: str = "last_number") -> dict:
        """Estadísticas por último número para cualquier tabla de patrones."""
        rows = await db.fetch(
            f"SELECT {last_num_col}, final_result FROM {table} "
            f"WHERE final_result IN ('WIN','LOSS')"
        )
        stats: Dict[str, dict] = {}
        for r in rows:
            key = str(r[last_num_col])
            if key not in stats:
                stats[key] = {"wins": 0, "losses": 0, "total": 0, "winrate": 0.0}
            stats[key]["total"] += 1
            if r["final_result"] == "WIN":
                stats[key]["wins"] += 1
            else:
                stats[key]["losses"] += 1
        for key in stats:
            t = stats[key]["total"]
            stats[key]["winrate"] = round(stats[key]["wins"] / t * 100, 1) if t else 0.0
        return stats

    async def get_pattern_history_color(self, limit: int = 60) -> List[dict]:
        return await self._get_multiintent_history("color_pattern_history", limit=limit)

    async def get_pattern_history_zone(self, limit: int = 60) -> List[dict]:
        return await self._get_multiintent_history("zone_pattern_history", limit=limit)

    async def get_pattern_summary(self, kind: str) -> dict:
        table = "color_pattern_history" if kind == "color" else "zone_pattern_history"
        rows = await db.fetch(
            f"SELECT pattern_id, bet, final_result FROM {table} WHERE final_result IS NOT NULL"
        )
        summary: Dict[str, dict] = {}
        for r in rows:
            pid = r["pattern_id"]
            if pid not in summary:
                summary[pid] = {"bet": r["bet"], "wins": 0, "losses": 0, "total": 0}
            summary[pid]["total"] += 1
            if r["final_result"] == "WIN":
                summary[pid]["wins"] += 1
            else:
                summary[pid]["losses"] += 1
        for pid in summary:
            t = summary[pid]["total"]
            summary[pid]["winrate"] = round(summary[pid]["wins"]/t*100, 1) if t else 0
        return summary

    async def get_signal_summary(self, kind: str) -> dict:
        table = "dozen_signal_history" if kind == "dozen" else "column_signal_history"
        rows  = await db.fetch(
            f"SELECT strategy, result FROM {table} WHERE result IN ('Acierto','Fallo')"
        )
        aciertos = sum(1 for r in rows if r["result"] == "Acierto")
        fallos   = sum(1 for r in rows if r["result"] == "Fallo")
        total    = aciertos + fallos
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
            "aciertos": aciertos, "fallos": fallos, "total": total,
            "efectividad": round(aciertos/total*100, 1) if total > 0 else 0.0,
            "por_estrategia": by_strat,
        }

    async def get_signal_stats_by_number(self, kind: str) -> dict:
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

    # ── Estadísticas E1/E2/E3 ─────────────────────────────────────────────────
    async def _get_ex_summary(self, table: str) -> dict:
        rows = await db.fetch(
            f"SELECT final_result FROM {table} WHERE final_result IS NOT NULL"
        )
        wins   = sum(1 for r in rows if r["final_result"] == "WIN")
        losses = sum(1 for r in rows if r["final_result"] == "LOSS")
        total  = wins + losses
        return {
            "wins": wins, "losses": losses, "total": total,
            "winrate": round(wins/total*100, 1) if total else 0.0
        }

    async def _get_ex_by_last_number(self, table: str) -> dict:
        return await self._get_pattern_stats_by_number(table)

    async def get_latest_data(self) -> dict:
        total         = await self.get_total_spins()
        last_20       = await self.get_last_n(20)
        stats_dozen   = await self.get_stats_dozen()
        stats_column  = await self.get_stats_column()
        stats_color   = await self.get_stats_color()
        stats_zone    = await self.get_stats_zone()

        # Color / Zona patrones
        color_hist = await self.get_pattern_history_color(60)
        zone_hist  = await self.get_pattern_history_zone(60)
        color_sum  = await self.get_pattern_summary("color")
        zone_sum   = await self.get_pattern_summary("zone")

        # Señales bot
        dozen_sum     = await self.get_signal_summary("dozen")
        dozen_by_num  = await self.get_signal_stats_by_number("dozen")
        column_sum    = await self.get_signal_summary("column")
        column_by_num = await self.get_signal_stats_by_number("column")

        # E1 Docenas
        e1d_hist   = await self._get_multiintent_history("e1_dozen_patterns", limit=60)
        e1d_sum    = await self._get_ex_summary("e1_dozen_patterns")
        e1d_by_num = await self._get_ex_by_last_number("e1_dozen_patterns")

        # E2 Docenas
        e2d_hist   = await self._get_multiintent_history("e2_dozen_patterns", limit=60)
        e2d_sum    = await self._get_ex_summary("e2_dozen_patterns")
        e2d_by_num = await self._get_ex_by_last_number("e2_dozen_patterns")

        # E3 Docenas
        e3d_hist   = await self._get_multiintent_history("e3_dozen_patterns", limit=60)
        e3d_sum    = await self._get_ex_summary("e3_dozen_patterns")
        e3d_by_num = await self._get_ex_by_last_number("e3_dozen_patterns")

        # E1 Columnas
        e1c_hist   = await self._get_multiintent_history("e1_column_patterns", limit=60)
        e1c_sum    = await self._get_ex_summary("e1_column_patterns")
        e1c_by_num = await self._get_ex_by_last_number("e1_column_patterns")

        # E2 Columnas
        e2c_hist   = await self._get_multiintent_history("e2_column_patterns", limit=60)
        e2c_sum    = await self._get_ex_summary("e2_column_patterns")
        e2c_by_num = await self._get_ex_by_last_number("e2_column_patterns")

        # E3 Columnas
        e3c_hist   = await self._get_multiintent_history("e3_column_patterns", limit=60)
        e3c_sum    = await self._get_ex_summary("e3_column_patterns")
        e3c_by_num = await self._get_ex_by_last_number("e3_column_patterns")

        return {
            "roulette":      ROULETTE,
            "roulette_name": ROULETTE_NAME,
            "total_spins":   total,
            "last_20":       last_20,
            "stats_dozen":   stats_dozen,
            "stats_column":  stats_column,
            "stats_color":   stats_color,
            "stats_zone":    stats_zone,

            # ── COLOR / ZONA ─────────────────────────────────────────────────
            "color_patterns": {
                "pending": self.pending_color,
                "history": color_hist,
                "summary": color_sum,
            },
            "zone_patterns": {
                "pending": self.pending_zone,
                "history": zone_hist,
                "summary": zone_sum,
            },

            # ── SEÑALES BOT ───────────────────────────────────────────────────
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

            # ── E1 DOCENAS ────────────────────────────────────────────────────
            # Formato attempt_log: [{attempt,number,result}, ...]
            # final_result: WIN | LOSS
            "e1_dozen_patterns": {
                "pending":   self.pending_e1_dozen,
                "history":   e1d_hist,
                "summary":   e1d_sum,
                "by_number": e1d_by_num,
            },

            # ── E2 DOCENAS ────────────────────────────────────────────────────
            "e2_dozen_patterns": {
                "pending":   self.pending_e2_dozen,
                "history":   e2d_hist,
                "summary":   e2d_sum,
                "by_number": e2d_by_num,
            },

            # ── E3 DOCENAS ────────────────────────────────────────────────────
            "e3_dozen_patterns": {
                "pending":   self.pending_e3_dozen,
                "history":   e3d_hist,
                "summary":   e3d_sum,
                "by_number": e3d_by_num,
            },

            # ── E1 COLUMNAS ───────────────────────────────────────────────────
            "e1_column_patterns": {
                "pending":   self.pending_e1_column,
                "history":   e1c_hist,
                "summary":   e1c_sum,
                "by_number": e1c_by_num,
            },

            # ── E2 COLUMNAS ───────────────────────────────────────────────────
            "e2_column_patterns": {
                "pending":   self.pending_e2_column,
                "history":   e2c_hist,
                "summary":   e2c_sum,
                "by_number": e2c_by_num,
            },

            # ── E3 COLUMNAS ───────────────────────────────────────────────────
            "e3_column_patterns": {
                "pending":   self.pending_e3_column,
                "history":   e3c_hist,
                "summary":   e3c_sum,
                "by_number": e3c_by_num,
            },
        }

    async def broadcast_update(self, number: int, game_id: str):
        if not self.ws_clients: return
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
        "server":    "Immersive Roulette Stats Server v4",
        "roulette":  ROULETTE_NAME,
        "total_spins": total,
        "last_number": engine.last_number,
        "pending_color":     engine.pending_color,
        "pending_zone":      engine.pending_zone,
        "pending_e1_dozen":  engine.pending_e1_dozen,
        "pending_e2_dozen":  engine.pending_e2_dozen,
        "pending_e3_dozen":  engine.pending_e3_dozen,
        "pending_e1_column": engine.pending_e1_column,
        "pending_e2_column": engine.pending_e2_column,
        "pending_e3_column": engine.pending_e3_column,
        "ws_clients": len(engine.ws_clients),
    })

async def handle_ping(request):
    return web.json_response({"status": "pong", "ts": time.time()})

async def handle_health(request):
    return web.json_response({
        "status":       "ok",
        "total_spins":  await engine.get_total_spins(),
        "last_number":  engine.last_number,
        "last_game_id": engine.last_game_id,
    })

async def handle_latest(request):
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
    history = await engine.get_pattern_history_color(100)
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
    history = await engine.get_pattern_history_zone(100)
    summary = await engine.get_pattern_summary("zone")
    return web.json_response({
        "pending": engine.pending_zone,
        "summary": summary,
        "history": history,
    })

# ─── HANDLERS SEÑALES DOCENAS / COLUMNAS ──────────────────────────────────────
async def handle_signal_dozen_post(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    try:
        data        = await request.json()
        strategy    = str(data.get("strategy", ""))
        pair        = list(data.get("pair", []))
        missing     = int(data.get("missing", 0))
        prob        = float(data.get("prob", 0.0))
        last_number = int(data.get("last_number", 0))
        if not pair:
            return web.json_response({"error": "pair requerido"}, status=400)
        row_id = await engine.register_dozen_signal(strategy, pair, missing, prob, last_number)
        return web.json_response({"ok": True, "row_id": row_id})
    except Exception as e:
        logger.error(f"❌ Error registrando señal docena: {e}")
        return web.json_response({"error": str(e)}, status=400)

async def handle_signal_column_post(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    try:
        data        = await request.json()
        strategy    = str(data.get("strategy", ""))
        pair        = list(data.get("pair", []))
        missing     = int(data.get("missing", 0))
        prob        = float(data.get("prob", 0.0))
        last_number = int(data.get("last_number", 0))
        if not pair:
            return web.json_response({"error": "pair requerido"}, status=400)
        row_id = await engine.register_column_signal(strategy, pair, missing, prob, last_number)
        return web.json_response({"ok": True, "row_id": row_id})
    except Exception as e:
        logger.error(f"❌ Error registrando señal columna: {e}")
        return web.json_response({"error": str(e)}, status=400)

async def handle_signal_dozen_get(request):
    key = request.match_info.get("roulette", "").upper()
    if key != ROULETTE:
        return web.json_response({"error": f"Solo disponible: {ROULETTE}"}, status=404)
    summary   = await engine.get_signal_summary("dozen")
    by_number = await engine.get_signal_stats_by_number("dozen")
    return web.json_response({
        "pending":   engine.pending_dozen_signal,
        "summary":   summary,
        "by_number": by_number,
    })

async def handle_signal_column_get(request):
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
    recon           = 5
    last_id         = engine.last_game_id
    last_settled_ts = 0.0
    spin_interval   = 0.0
    poll_secs       = DEFAULT_POLL

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
                        await asyncio.sleep(recon)
                        recon = min(recon * 2, 60)
                        continue

                    payload = await resp.json(content_type=None)
                    recon   = 5

                    game_id = str(payload.get("id", ""))
                    if not game_id or game_id == last_id:
                        await asyncio.sleep(poll_secs)
                        continue

                    data   = payload.get("data", {})
                    status = data.get("status", "")
                    if status != "Resolved":
                        await asyncio.sleep(poll_secs)
                        continue

                    outcome = data.get("result", {}).get("outcome", {})
                    number  = outcome.get("number")
                    if number is None:
                        await asyncio.sleep(poll_secs)
                        continue

                    number = int(number)
                    if not (0 <= number <= 36):
                        await asyncio.sleep(poll_secs)
                        continue

                    current_settled_ts = parse_settled_at(data.get("settledAt", ""))
                    if current_settled_ts == 0.0:
                        current_settled_ts = time.time()

                    if last_settled_ts > 0 and current_settled_ts > last_settled_ts:
                        spin_interval = current_settled_ts - last_settled_ts
                        logger.info(f"[Poller] ⏱️ Intervalo: {spin_interval:.1f}s")

                    last_settled_ts = current_settled_ts
                    last_id         = game_id

                    if await engine.process_spin(number, game_id):
                        await engine.broadcast_update(number, game_id)

                    if spin_interval > 5:
                        poll_secs = POLL_SECS
                        elapsed   = time.time() - current_settled_ts
                        safe_sleep = max(spin_interval * 0.80 - elapsed, 0.0)
                        if safe_sleep > 1:
                            await asyncio.sleep(safe_sleep)
                    else:
                        await asyncio.sleep(DEFAULT_WAIT)
                    continue

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
