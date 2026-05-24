#!/usr/bin/env python3
"""
Immersive Roulette Stats Server v2
===========================================================================
CAMBIOS vs anterior:
  - Única ruleta: IMMERSIVE ROULETTE (Evolution, HTTP polling)
  - Fuente de datos: https://api-cs.casino.org/svc-evolution-game-events/api/immersiveroulette/latest
  - Nuevas tablas: color_transitions, zone_transitions
  - Detección de patrones de color (N/R) y zona (B/A) con secuencias reales
  - Registro de aciertos/fallos por patrón para ML
  - Endpoints extendidos: /stats/color, /stats/zone, /patterns/color, /patterns/zone
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
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
POLL_SECS       = 3
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
    if n == 0: return "0"
    return "B" if n <= 18 else "A"

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

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
            if self.zone_seq[i] != "0"
        ]
        matched = check_patterns(non_zero_zones, ZONE_PATTERNS)
        if not matched:
            return

        plen     = len(matched["p"])
        nz_nums  = [
            self.number_seq[i]
            for i in range(len(self.number_seq))
            if self.zone_seq[i] != "0"
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
    """Hace polling HTTP a la API de Evolution para obtener el último resultado."""
    recon  = 5
    last_id = engine.last_game_id
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
                        await asyncio.sleep(POLL_SECS)
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

                    last_id = game_id
                    if await engine.process_spin(number, game_id):
                        await engine.broadcast_update(number, game_id)

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
    app.router.add_get("/spins/{roulette}/{n}",       handle_spins)
    app.router.add_get("/ws",                         handle_websocket)
    app.on_startup.append(start_tasks)
    app.on_cleanup.append(stop_tasks)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10004))
    web.run_app(create_app(), host="0.0.0.0", port=port, access_log=None)
