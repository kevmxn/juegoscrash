#!/usr/bin/env python3
"""
Immersive Roulette - Spin Logger (versión simplificada)
===========================================================================
Esta versión SOLO registra los giros (spins) en tiempo real y los expone
tal cual. Se eliminó toda la lógica de detección de patrones, señales
E1/E2/E3, docenas/columnas y estadísticas de transición.
  - Poll continuo a la API de Evolution para detectar giros nuevos
  - Guarda cada giro (number, color, zone, game_id, ts) en SQLite
  - Difunde cada giro nuevo por WebSocket (/ws) apenas ocurre
  - Expone los últimos giros vía HTTP (/latest/{roulette}, /spins/{roulette}/{n})
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

def parse_settled_at(s: str) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0

# ─── DB POOL ──────────────────────────────────────────────────────────────────
class DBPool:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock    = asyncio.Lock()
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

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


db = DBPool(STATS_DB)

# ─── SPIN ENGINE ──────────────────────────────────────────────────────────────
class SpinEngine:
    def __init__(self):
        self.last_game_id: str           = ""
        self.last_number:  Optional[int] = None

        # Secuencias en memoria (solo para tener el estado reciente a mano)
        self.number_seq: List[int] = []
        self.color_seq:  List[str] = []
        self.zone_seq:   List[str] = []

        # WebSocket clientes
        self.ws_clients: dict = {}

        self._load_state()

    def _load_state(self):
        conn = sqlite3.connect(db.db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT game_id, number FROM spins ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row:
                self.last_game_id = row["game_id"]
                self.last_number  = row["number"]

            rows = conn.execute(
                "SELECT number, color, zone FROM spins ORDER BY id DESC LIMIT 100"
            ).fetchall()
            rows = list(reversed(rows))
            self.number_seq = [r["number"] for r in rows]
            self.color_seq  = [r["color"]  for r in rows]
            self.zone_seq   = [r["zone"]   for r in rows]
            logger.info(f"[{ROULETTE}] Secuencia cargada: {len(rows)} spins")
        finally:
            conn.close()

    # ── Procesamiento de spin ─────────────────────────────────────────────────
    async def process_spin(self, number: int, game_id: str) -> Optional[dict]:
        """Registra un giro nuevo. Retorna el dict del giro (o None si es duplicado)."""
        existing = await db.fetchone("SELECT 1 FROM spins WHERE game_id=?", (game_id,))
        if existing:
            return None

        color = get_color(number)
        zone  = get_zone(number)
        ts    = int(time.time())

        await db.write(
            "INSERT INTO spins(game_id, number, color, zone, ts) VALUES(?,?,?,?,?)",
            (game_id, number, color, zone, ts)
        )

        self.number_seq.append(number)
        self.color_seq.append(color)
        self.zone_seq.append(zone)
        if len(self.number_seq) > 200:
            self.number_seq.pop(0)
            self.color_seq.pop(0)
            self.zone_seq.pop(0)

        self.last_number  = number
        self.last_game_id = game_id

        # Limpiar spins viejos
        await db.write(
            "DELETE FROM spins WHERE id NOT IN (SELECT id FROM spins ORDER BY id DESC LIMIT ?)",
            (MAX_STORED,)
        )

        logger.info(f"[{ROULETTE}] 🎰 #{number} {color}/{zone} | gid={game_id[:12]}...")
        return {"number": number, "color": color, "zone": zone, "game_id": game_id, "ts": ts}

    async def get_total_spins(self) -> int:
        row = await db.fetchone("SELECT COUNT(*) as cnt FROM spins")
        return row["cnt"] if row else 0

    async def get_last_n(self, n: int = 20) -> List[dict]:
        rows = await db.fetch(
            "SELECT number, color, zone, game_id, ts FROM spins ORDER BY id DESC LIMIT ?", (n,)
        )
        return [
            {"number": r["number"], "color": r["color"], "zone": r["zone"],
             "game_id": r["game_id"], "ts": r["ts"]}
            for r in rows
        ]

    async def get_latest_data(self) -> dict:
        return {
            "roulette":      ROULETTE,
            "roulette_name": ROULETTE_NAME,
            "total_spins":   await self.get_total_spins(),
            "last_number":   self.last_number,
            "last_game_id":  self.last_game_id,
            "last_20":       await self.get_last_n(20),
        }

    async def broadcast_update(self, spin: dict):
        if not self.ws_clients: return
        message = json.dumps({"type": "new_spin", "spin": spin})
        disconnected = []
        for ws in list(self.ws_clients):
            try:
                await ws.send_str(message)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.ws_clients.pop(ws, None)


engine = SpinEngine()

# ─── HTTP HANDLERS ────────────────────────────────────────────────────────────
async def handle_home(request):
    return web.json_response({
        "status":      "ok",
        "server":      "Immersive Roulette Spin Logger",
        "roulette":    ROULETTE_NAME,
        "total_spins": await engine.get_total_spins(),
        "last_number": engine.last_number,
        "ws_clients":  len(engine.ws_clients),
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

                    spin = await engine.process_spin(number, game_id)
                    if spin:
                        await engine.broadcast_update(spin)

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
    app.router.add_get("/",                     handle_home)
    app.router.add_get("/ping",                 handle_ping)
    app.router.add_get("/health",               handle_health)
    app.router.add_get("/latest/{roulette}",    handle_latest)
    app.router.add_get("/spins/{roulette}/{n}", handle_spins)
    app.router.add_get("/ws",                   handle_websocket)
    app.on_startup.append(start_tasks)
    app.on_cleanup.append(stop_tasks)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10004))
    web.run_app(create_app(), host="0.0.0.0", port=port, access_log=None)
