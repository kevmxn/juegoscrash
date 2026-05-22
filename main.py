#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   CRASH BOT — Sistema Unificado 2.00x + Estrategias VB4     ║
║   API HTTPS Polling en tiempo real                           ║
║   Stake Crash | Sesión Global | Render-Ready                 ║
╠══════════════════════════════════════════════════════════════╣
║  Señal VB4 — 5 condiciones (mínimo 3/5):                    ║
║   C1. EMA4 > EMA12 Y EMA4 subiendo                          ║
║   C2. ≥5 de las últimas 8 velas alcistas                    ║
║   C3. Fuerza media últimas 4 > media previas 4              ║
║   C4. Mini-chart acumulado en pendiente positiva            ║
║   C5. Última vela alcista y fuerza > 0                      ║
║  Escala 20 niveles: classify() de -10 a +10                 ║
║  Velas precio porcentual | Ventana 2 rondas | Cool-down 3   ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import threading
import json
import logging
import os
import sys
import time
import random
from datetime import datetime, timedelta
from typing import Optional
from flask import Flask
import aiohttp
from telebot.async_telebot import AsyncTeleBot
from telebot import types

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
API_CRASH = "https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest"

WIN_TARGET  = 2.00
MAX_MULTS   = 400
TRIM_MULTS  = 200
MAX_COLS    = 3
MAX_ATTS    = 2
CYCLE_SIZE  = 10
BASE_BET    = 0.10

# VB4 ─ parámetros del predictor
VB4_CHECKS_NEEDED   = 3     # mínimo de checks para señal (de 5)
VB4_MAX_LOOK        = 2     # ventana de evaluación (rondas)
VB4_COOLDOWN_BARS   = 3     # rondas de cooldown tras resolución
VB4_EMA_FAST        = 4     # EMA rápida (display + check C1)
VB4_EMA_SLOW_PRED   = 12    # EMA lenta (check C1)
VB4_EMA_SLOW_DISP   = 7     # EMA lenta para display
VB4_CANDLE_INIT     = 100.0 # precio base inicial

POLL_INTERVAL_OK  = 3.0
POLL_MAX_SLEEP    = 60.0
POLL_BACKOFF_BASE = 2.0

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36',
]

# ─── ESTADO GLOBAL BASE ────────────────────────────────────────────────────────
g_mults:    list = []
g_seen_ids: set  = set()

g_signal_state               = 'idle'   # 'idle' | 'evaluating' | 'so'
g_signal_type: Optional[str] = None
g_signal_trigger_mult: float = 0.0

g_all_chats: set                  = set()
g_trend_favorable: Optional[bool] = None

g_poller_status = {
    'total_requests':     0,
    'total_new_rounds':   0,
    'consecutive_errors': 0,
    'last_poll_ts':       0.0,
    'last_round_ts':      0.0,
}

# ─── ESTADO VB4 ────────────────────────────────────────────────────────────────
# Velas de precio porcentual
g_vb4_bars:       list  = []      # [{'open': float, 'close': float}]
g_vb4_forces:     list  = []      # [int]   fuerza clasificada por ronda
g_vb4_bar_values: list  = []      # [float] multiplicador raw por ronda

# Mini-chart acumulado ±1 (close >= 2.00 → +1, else → -1)
g_vb4_mini_cum:   float = 0.0
g_vb4_mini_data:  list  = []      # [float] valores acumulados

# Control de señal y cooldown
g_vb4_last_resolved: int   = -9999   # índice de bars cuando se resolvió última señal
g_vb4_pending: Optional[dict] = None # {'created_idx': int, 'max_look': int}
g_vb4_hits:    int = 0
g_vb4_misses:  int = 0

bot = AsyncTeleBot(BOT_TOKEN)


# ─── HORA ARGENTINA ───────────────────────────────────────────────────────────
def argentina_time() -> str:
    now_arg = datetime.utcnow() - timedelta(hours=3)
    return now_arg.strftime("%H:%M")


# ─── BROADCAST ────────────────────────────────────────────────────────────────
async def broadcast(msg: str, parse_mode: str = None):
    dead = set()
    for chat_id in list(g_all_chats):
        try:
            await bot.send_message(chat_id, msg, parse_mode=parse_mode)
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ('blocked', 'not found', 'deactivated', 'kicked')):
                dead.add(chat_id)
                logger.warning(f"Chat {chat_id} inactivo → removido")
            else:
                logger.warning(f"Broadcast error → {chat_id}: {e}")
    g_all_chats.difference_update(dead)


async def broadcast_trend_change(favorable: bool):
    hora = argentina_time()
    msg = (
        f"🟢 TENDENCIA FAVORABLE  {hora}"
        if favorable else
        f"🔴 TENDENCIA DESFAVORABLE  {hora}"
    )
    await broadcast(msg)


# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema_on_closes(bars: list, period: int) -> list:
    """EMA calculada sobre los valores 'close' de las velas VB4."""
    if len(bars) < 1:
        return []
    k = 2 / (period + 1)
    closes = [b['close'] for b in bars]
    ema = [closes[0]]
    for i in range(1, len(closes)):
        ema.append(closes[i] * k + ema[-1] * (1 - k))
    return ema


# ─── ESTADÍSTICAS DE CUOTAS ───────────────────────────────────────────────────
def get_quota_stats(n: int = 200) -> dict:
    data  = g_mults[-n:] if len(g_mults) >= n else g_mults[:]
    total = len(data)
    if total == 0:
        return {
            'total': 0, 'has_enough': False, 'favorable': None,
            'count_100_199': 0, 'count_200_499': 0,
            'count_500_999': 0, 'count_1000_plus': 0,
            'pct_100_199': 0.0, 'pct_200_499': 0.0,
            'pct_500_999': 0.0, 'pct_1000_plus': 0.0,
        }
    r1 = sum(1 for m in data if 1.00 <= m['value'] <  2.00)
    r2 = sum(1 for m in data if 2.00 <= m['value'] <  5.00)
    r3 = sum(1 for m in data if 5.00 <= m['value'] < 10.00)
    r4 = sum(1 for m in data if m['value'] >= 10.00)
    pct1 = r1 / total * 100
    pct2 = r2 / total * 100
    pct3 = r3 / total * 100
    pct4 = r4 / total * 100
    return {
        'total':           total,
        'has_enough':      total >= 200,
        'favorable':       not (pct1 > 52.0 or pct2 < 29.0),
        'count_100_199':   r1,   'count_200_499':  r2,
        'count_500_999':   r3,   'count_1000_plus': r4,
        'pct_100_199':     pct1, 'pct_200_499':    pct2,
        'pct_500_999':     pct3, 'pct_1000_plus':  pct4,
    }


def quota_stats_text(stats: dict) -> str:
    if stats['total'] == 0:
        return "📡 _Sin datos suficientes para analizar cuotas._\n"
    n_label = "200" if stats['has_enough'] else str(stats['total']) + " (acumulando...)"
    r1_flag = " ✅" if stats['pct_100_199'] <= 52.0 else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= 29.0 else " ❌"
    fav_line = (
        "✅ *¡TENDENCIA FAVORABLE!*\n      _Se recomienda operar_"
        if stats['favorable'] else
        "⚠️ *TENDENCIA DESFAVORABLE*\n      _Se recomienda esperar_"
    )
    return (
        f"📈 *Análisis de la Tendencia últimos*\n"
        f"      *{n_label} multiplicadores*\n"
        f"🔵 Cuotas (1.00-1.99x): `{stats['count_100_199']}` — {stats['pct_100_199']:.2f}%{r1_flag}\n"
        f"🟣 Cuotas (2.00-4.99x): `{stats['count_200_499']}` — {stats['pct_200_499']:.2f}%{r2_flag}\n"
        f"🟡 Cuotas (5.00-9.99x): `{stats['count_500_999']}` — {stats['pct_500_999']:.2f}%\n"
        f"🔴 Cuotas (+10.00x):    `{stats['count_1000_plus']}` — {stats['pct_1000_plus']:.2f}%\n"
        " \n"
        f"{fav_line}\n"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  VB4  —  Motor de señales
# ══════════════════════════════════════════════════════════════════════════════

def vb4_classify(v: float) -> int:
    """
    Traduce un multiplicador a la escala de fuerza VB4 (-10 a +10).
    Negativo = bajo 2.00x  |  Positivo = sobre 2.00x  |  0 = fuera de rango.
    """
    if 1.00 <= v <= 1.09: return -10
    if 1.10 <= v <= 1.19: return  -9
    if 1.20 <= v <= 1.29: return  -8
    if 1.30 <= v <= 1.39: return  -7
    if 1.40 <= v <= 1.49: return  -6
    if 1.50 <= v <= 1.59: return  -5
    if 1.60 <= v <= 1.69: return  -4
    if 1.70 <= v <= 1.79: return  -3
    if 1.80 <= v <= 1.89: return  -2
    if 1.90 <= v <= 1.99: return  -1
    if 2.00 <= v <= 2.99: return   1
    if 3.00 <= v <= 3.99: return   2
    if 4.00 <= v <= 4.99: return   3
    if 5.00 <= v <= 5.99: return   4
    if 6.00 <= v <= 6.99: return   5
    if 7.00 <= v <= 7.99: return   6
    if 8.00 <= v <= 8.99: return   7
    if 9.00 <= v <= 9.99: return   8
    if 10.00 <= v <= 14.99: return 9
    if 15.00 <= v <= 19.99: return 10
    return 0


def vb4_create_candle(force: int, prev_close: float) -> dict:
    """
    Crea una vela de precio porcentual a partir de la fuerza.
    El precio cambia un 0.1% * |force| respecto al cierre anterior.
    Alcista si force > 0, bajista si force < 0.
    """
    magnitude = abs(force)
    change    = prev_close * (0.001 * magnitude)   # 0.1% por nivel
    open_     = prev_close
    close     = prev_close + (change if force > 0 else -change)
    return {'open': open_, 'close': close}


def vb4_ingest(value: float):
    """
    Procesa un nuevo multiplicador en el motor VB4:
    crea vela, actualiza forces, mini-chart y recorta si necesario.
    """
    global g_vb4_mini_cum

    force    = vb4_classify(value)
    prev_cls = g_vb4_bars[-1]['close'] if g_vb4_bars else VB4_CANDLE_INIT
    candle   = vb4_create_candle(force, prev_cls)

    g_vb4_bars.append(candle)
    g_vb4_forces.append(force)
    g_vb4_bar_values.append(value)

    # mini-chart acumulado ±1
    g_vb4_mini_cum += 1 if value >= WIN_TARGET else -1
    g_vb4_mini_data.append(g_vb4_mini_cum)

    # recorte de memoria
    if len(g_vb4_bars) > MAX_MULTS:
        g_vb4_bars[:]       = g_vb4_bars[-TRIM_MULTS:]
        g_vb4_forces[:]     = g_vb4_forces[-TRIM_MULTS:]
        g_vb4_bar_values[:] = g_vb4_bar_values[-TRIM_MULTS:]
        g_vb4_mini_data[:]  = g_vb4_mini_data[-TRIM_MULTS:]


def vb4_run_predictor() -> bool:
    """
    Evalúa las 5 condiciones VB4 y devuelve True si ≥ VB4_CHECKS_NEEDED
    son verdaderas Y hay cooldown suficiente Y no hay señal pendiente.

    Condiciones:
      C1  EMA4(bars) > EMA12(bars)  Y  EMA4 subiendo en el último tick
      C2  ≥5 de las últimas 8 velas son alcistas (close > open)
      C3  Media de fuerza últimas 4 > media anteriores 4 − 0.1
      C4  Mini-chart acumulado: últimos 5 puntos tienen pendiente > 0
      C5  Última vela alcista (close > open) Y fuerza > 0
    """
    n = len(g_vb4_bars)
    if n < 10:
        return False
    if g_vb4_pending is not None:
        return False
    if n - g_vb4_last_resolved < VB4_COOLDOWN_BARS:
        return False

    checks = []

    # C1 — EMA4 > EMA12 y EMA4 creciendo
    ema4  = calc_ema_on_closes(g_vb4_bars, VB4_EMA_FAST)
    ema12 = calc_ema_on_closes(g_vb4_bars, VB4_EMA_SLOW_PRED)
    if len(ema4) >= 2 and len(ema12) >= 1:
        checks.append(ema4[-1] > ema12[-1] and (ema4[-1] - ema4[-2]) > 0)
    else:
        checks.append(False)

    # C2 — mayoría alcista en las últimas 8 velas
    last8         = g_vb4_bars[-8:]
    bullish_count = sum(1 for b in last8 if b['close'] > b['open'])
    checks.append(bullish_count >= 5)

    # C3 — fuerza mejorando
    if len(g_vb4_forces) >= 8:
        recent_avg = sum(g_vb4_forces[-4:]) / 4
        prev_avg   = sum(g_vb4_forces[-8:-4]) / 4
        checks.append(recent_avg > prev_avg - 0.1)
    else:
        checks.append(False)

    # C4 — pendiente positiva en el mini-chart (últimos 5 puntos)
    if len(g_vb4_mini_data) >= 5:
        checks.append(g_vb4_mini_data[-1] - g_vb4_mini_data[-5] > 0)
    else:
        checks.append(False)

    # C5 — última vela alcista y fuerza positiva
    last_bar   = g_vb4_bars[-1]
    last_force = g_vb4_forces[-1] if g_vb4_forces else 0
    checks.append(last_bar['close'] > last_bar['open'] and last_force > 0)

    true_count = sum(checks)
    logger.debug(f"VB4 checks: {checks} → {true_count}/5")
    return true_count >= VB4_CHECKS_NEEDED


def vb4_checks_detail() -> str:
    """Texto con el detalle de las 5 condiciones (para logs/mensajes)."""
    n = len(g_vb4_bars)
    if n < 10:
        return "Sin datos suficientes"

    ema4  = calc_ema_on_closes(g_vb4_bars, VB4_EMA_FAST)
    ema12 = calc_ema_on_closes(g_vb4_bars, VB4_EMA_SLOW_PRED)
    c1 = (len(ema4) >= 2 and len(ema12) >= 1
          and ema4[-1] > ema12[-1]
          and (ema4[-1] - ema4[-2]) > 0)

    last8 = g_vb4_bars[-8:]
    c2 = sum(1 for b in last8 if b['close'] > b['open']) >= 5

    if len(g_vb4_forces) >= 8:
        c3 = sum(g_vb4_forces[-4:]) / 4 > sum(g_vb4_forces[-8:-4]) / 4 - 0.1
    else:
        c3 = False

    c4 = (len(g_vb4_mini_data) >= 5
          and g_vb4_mini_data[-1] - g_vb4_mini_data[-5] > 0)

    last_force = g_vb4_forces[-1] if g_vb4_forces else 0
    last_bar   = g_vb4_bars[-1]
    c5 = last_bar['close'] > last_bar['open'] and last_force > 0

    checks  = [c1, c2, c3, c4, c5]
    labels  = ["C1 EMA↑", "C2 Alcistas", "C3 Fuerza", "C4 Mini↑", "C5 Vela+"]
    true_n  = sum(checks)
    parts   = [f"{'✅' if c else '❌'} {l}" for c, l in zip(checks, labels)]
    return f"{true_n}/5 → " + " | ".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
#  SESIÓN GLOBAL
# ══════════════════════════════════════════════════════════════════════════════

class GlobalSession:
    IDLE       = 'idle'
    EVALUATING = 'evaluating'
    WAITING_SO = 'waiting_so'
    DONE       = 'done'

    def __init__(self, carry_fichas: list = None):
        self.base_bet = BASE_BET
        self.state    = self.IDLE

        self.scale   = 1
        self.col     = 1
        self.attempt = 1
        self.lost    = 0.0
        self.cur_bet = BASE_BET

        self.entries = 0
        self.wins    = 0
        self.losses  = 0
        self.created = datetime.now()

        self.signal_trigger_mult:   float = 0.0
        self.attempt1_result_value: float = 0.0

        self.fichas: list     = carry_fichas if carry_fichas is not None else []
        self._cur_ficha: dict = None

    def start_ficha(self):
        self._cur_ficha = {
            'n':      len(self.fichas) + 1,
            'c1':     0.0,
            'c2':     0.0,
            'c3':     0.0,
            'result': None,
            'ts':     argentina_time(),
        }

    def on_result(self, win: bool) -> tuple:
        """
        Retorna (tipo, apuesta_usada).
        Tipos: 'win' | 'cycle_win' | 'so' | 'new_col' | 'cycle_loss'
        """
        self.entries += 1
        prev_bet = self.cur_bet
        prev_col = self.col

        if self._cur_ficha is not None:
            col_key = f'c{prev_col}'
            self._cur_ficha[col_key] = self._cur_ficha.get(col_key, 0.0) + prev_bet

        if win:
            self.wins   += 1
            self.lost    = 0.0
            self.cur_bet = self.base_bet
            self.col     = 1
            self.attempt = 1
            self.scale  += 1

            if self._cur_ficha is not None:
                self._cur_ficha['result'] = 'win'
                self.fichas.append(self._cur_ficha)
                self._cur_ficha = None
            if len(self.fichas) > 100:
                self.fichas = self.fichas[-100:]

            if self.scale > CYCLE_SIZE:
                self.state = self.DONE
                return ('cycle_win', prev_bet)
            self.state = self.IDLE
            return ('win', prev_bet)

        else:
            self.losses  += 1
            self.lost    += prev_bet
            self.cur_bet  = self.lost + self.base_bet
            self.attempt += 1

            if self.attempt > MAX_ATTS:
                self.attempt = 1
                self.col    += 1
                if self.col > MAX_COLS:
                    if self._cur_ficha is not None:
                        self._cur_ficha['result'] = 'loss'
                        self.fichas.append(self._cur_ficha)
                        self._cur_ficha = None
                    if len(self.fichas) > 100:
                        self.fichas = self.fichas[-100:]
                    self.state = self.DONE
                    return ('cycle_loss', prev_bet)
                self.state = self.IDLE
                return ('new_col', prev_bet)
            else:
                self.state = self.WAITING_SO
                return ('so', prev_bet)

    def status_short(self) -> str:
        estado_txt = {
            self.IDLE:       "⏳ Esperando señal",
            self.EVALUATING: "⚡ Evaluando resultado",
            self.WAITING_SO: "🔄 Esperando 2ª Oportunidad",
            self.DONE:       "✅ Ciclo finalizado",
        }.get(self.state, "—")
        return (
            f"📡 Estado: {estado_txt}\n"
            f"🎯 Señal: `{min(self.scale, CYCLE_SIZE)}/{CYCLE_SIZE}`\n"
            f"📍 Col: `{self.col}/{MAX_COLS}` | Intento: `{self.attempt}/{MAX_ATTS}`\n"
            f"💵 Próxima apuesta: `${self.cur_bet:.2f}`\n"
            f"📈 G/P: `{self.wins}/{self.losses}`"
        )


g_session: GlobalSession = GlobalSession()


def reset_global_session():
    global g_session, g_vb4_last_resolved, g_vb4_pending
    old_fichas        = list(g_session.fichas)
    g_session         = GlobalSession(carry_fichas=old_fichas)
    g_vb4_last_resolved = len(g_vb4_bars)   # cooldown desde ahora
    g_vb4_pending     = None
    logger.info("🔄 Sesión global reiniciada — fichas preservadas")


# ══════════════════════════════════════════════════════════════════════════════
#  PROCESADOR DE MULTIPLICADORES
# ══════════════════════════════════════════════════════════════════════════════

async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_type, g_signal_trigger_mult
    global g_mults, g_seen_ids
    global g_trend_favorable, g_session
    global g_vb4_pending, g_vb4_last_resolved, g_vb4_hits, g_vb4_misses

    logger.info(
        f"🎲 {value:.2f}x | ID: {round_id} | "
        f"Señal: {g_signal_state}/{g_signal_type}"
    )

    # ── FASE 1: Evaluar resultado de la señal activa ─────────────────────────
    if g_signal_state == 'evaluating':
        win = value >= WIN_TARGET
        if g_session.state == GlobalSession.EVALUATING:
            tipo, bet = g_session.on_result(win)
            await _dispatch_result(value, tipo, bet, is_so=False)
            # Marcar resolución VB4
            if tipo in ('win', 'cycle_win', 'new_col', 'cycle_loss'):
                _vb4_resolve(success=win)
            g_signal_state = 'so' if g_session.state == GlobalSession.WAITING_SO else 'idle'
            if g_signal_state != 'so':
                g_signal_type = None
        else:
            g_signal_state = 'idle'
            g_signal_type  = None

    # ── FASE 2: Evaluar 2ª oportunidad (SO) ──────────────────────────────────
    elif g_signal_state == 'so':
        win = value >= WIN_TARGET
        g_signal_state = 'idle'
        g_signal_type  = None
        if g_session.state == GlobalSession.WAITING_SO:
            tipo, bet = g_session.on_result(win)
            _vb4_resolve(success=win)
            await _dispatch_result(value, tipo, bet, is_so=True)

    # ── FASE 3: Actualizar historial de cuotas ───────────────────────────────
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})
    if len(g_mults) >= MAX_MULTS:
        g_mults[:] = g_mults[-TRIM_MULTS:]
    if len(g_seen_ids) > 2000:
        g_seen_ids.clear()

    # ── FASE 4: Actualizar motor VB4 ─────────────────────────────────────────
    vb4_ingest(value)

    # ── FASE 5: Detectar cambio de tendencia de cuotas ───────────────────────
    if g_all_chats:
        stats_trend = get_quota_stats(200)
        if stats_trend['total'] >= 10:
            new_fav = stats_trend['favorable']
            if new_fav != g_trend_favorable:
                g_trend_favorable = new_fav
                asyncio.create_task(broadcast_trend_change(new_fav))

    # ── FASE 6: Buscar nueva señal VB4 ───────────────────────────────────────
    if g_signal_state == 'idle' and g_session.state == GlobalSession.IDLE:
        stats_now = get_quota_stats(200)
        trend_ok  = (stats_now['total'] == 0) or (stats_now['favorable'] is not False)

        if g_session.col > 1 or trend_ok:
            if vb4_run_predictor():
                g_vb4_pending      = {
                    'created_idx': len(g_vb4_bars),
                    'max_look':    VB4_MAX_LOOK,
                }
                g_signal_state        = 'evaluating'
                g_signal_type         = 'vb4'
                g_signal_trigger_mult = value
                g_session.signal_trigger_mult = value
                g_session.state       = GlobalSession.EVALUATING

                if g_session.col == 1:
                    g_session.start_ficha()

                detail = vb4_checks_detail()
                logger.info(
                    f"🚀 SEÑAL VB4 Col{g_session.col} | "
                    f"Trigger: {value:.2f}x | {detail}"
                )
                await _send_signal_vb4(value, detail)


def _vb4_resolve(success: bool):
    """Marca la resolución de la señal VB4 y actualiza contadores/cooldown."""
    global g_vb4_last_resolved, g_vb4_pending, g_vb4_hits, g_vb4_misses
    g_vb4_last_resolved = len(g_vb4_bars)
    g_vb4_pending       = None
    if success:
        g_vb4_hits   += 1
    else:
        g_vb4_misses += 1


# ══════════════════════════════════════════════════════════════════════════════
#  MENSAJERÍA
# ══════════════════════════════════════════════════════════════════════════════

async def _send_signal_vb4(trigger: float, detail: str):
    txt = (
        "🚨 *¡SEÑAL VB4 DETECTADA! 💎 2.00x*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Último Multiplicador — `{trigger:.2f}x`\n"
        f"💵 *Apostar Ahora: `${g_session.cur_bet:.2f}`*\n"
        f"🕹️ Señal `{g_session.scale}/{CYCLE_SIZE}` | "
        f"Col `{g_session.col}/{MAX_COLS}` | "
        f"Intento `{g_session.attempt}/{MAX_ATTS}`\n"
        f"🧬 _{detail}_\n"
        f"⏳ _Ventana: próximas {VB4_MAX_LOOK} rondas_"
    )
    await broadcast(txt, parse_mode='Markdown')


async def _check_trend_after_cycle():
    stats = get_quota_stats(200)
    if stats['total'] > 0 and not stats['favorable']:
        hora    = argentina_time()
        r1_flag = "✅" if stats['pct_100_199'] <= 52.0 else "❌"
        r2_flag = "✅" if stats['pct_200_499'] >= 29.0 else "❌"
        await broadcast(
            f"🔴 *TENDENCIA DESFAVORABLE — {hora}*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔵 1.00-1.99x: `{stats['pct_100_199']:.1f}%` (límite ≤52%) {r1_flag}\n"
            f"🟣 2.00-4.99x: `{stats['pct_200_499']:.1f}%` (mínimo ≥29%) {r2_flag}\n"
            f"📊 Basado en los últimos `{stats['total']}` multiplicadores\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⏳ _El bot esperará hasta que la tendencia mejore._",
            parse_mode='Markdown'
        )


async def _dispatch_result(value: float, tipo: str, bet: float, is_so: bool):
    global g_session
    emoji_val = "🟢" if value >= WIN_TARGET else "🔴"
    so_prefix = "🔄 2ª Oportunidad — " if is_so else ""

    if tipo in ('win', 'cycle_win'):
        if tipo == 'cycle_win':
            txt = (
                f"✅ *GANADA* — {emoji_val} Resultado: `{value:.2f}x`\n"
                "━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"{so_prefix}💵 Próxima Apuesta: `${BASE_BET:.2f}`\n\n"
                "🏆 *¡CICLO COMPLETO — 10 señales exitosas!*\n"
                f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n"
                "🔄 _Sesión reiniciada automáticamente_"
            )
            await broadcast(txt, parse_mode='Markdown')
            reset_global_session()
            await _check_trend_after_cycle()
            return

        txt = (
            f"✅ *GANADA* — {emoji_val} Resultado: `{value:.2f}x`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{so_prefix}💵 Próxima Apuesta: `${BASE_BET:.2f}`\n"
            f"⏳ _Esperando próxima señal... ({g_session.scale}/{CYCLE_SIZE})_"
        )

    elif tipo == 'so':
        g_session.attempt1_result_value = value
        txt = (
            f"❌ *Perdida* — {emoji_val} Resultado: `{value:.2f}x`\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🔄 *¡SEGUNDA OPORTUNIDAD!*\n"
            f"💵 Apuesta: `${g_session.cur_bet:.2f}`\n"
            f"🕹️ Col `{g_session.col}/{MAX_COLS}` | Intento `2/{MAX_ATTS}`"
        )
        await broadcast(txt, parse_mode='Markdown')
        return

    elif tipo == 'new_col':
        r1  = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        txt = (
            f"🔴 *Resultados: `{r1}` — `{value:.2f}x`*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 Avanzando a Columna `{g_session.col}/{MAX_COLS}`\n"
            f"💵 Nueva apuesta: `${g_session.cur_bet:.2f}`\n"
            "⏳ _Esperando próxima señal..._"
        )

    elif tipo == 'cycle_loss':
        r1  = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        txt = (
            f"🔴 *Resultados: `{r1}` — `{value:.2f}x`*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️ *CICLO TERMINADO — 3 Columnas Fallidas*\n"
            f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n"
            "🔄 _Sesión reiniciada automáticamente_"
        )
        await broadcast(txt, parse_mode='Markdown')
        reset_global_session()
        await _check_trend_after_cycle()
        return

    else:
        txt = f"Resultado inesperado: {tipo}"

    await broadcast(txt, parse_mode='Markdown')


# ══════════════════════════════════════════════════════════════════════════════
#  POLLER HTTPS
# ══════════════════════════════════════════════════════════════════════════════

async def http_poller():
    consecutive_errors = 0
    sleep_next = POLL_INTERVAL_OK
    logger.info(f"📡 Iniciando poller HTTP → {API_CRASH}")

    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(sleep_next)
            try:
                ua = random.choice(USER_AGENTS)
                headers = {
                    'User-Agent': ua,
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'no-cache',
                }
                g_poller_status['total_requests'] += 1
                g_poller_status['last_poll_ts']    = time.time()

                async with session.get(
                    API_CRASH, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10), ssl=True,
                ) as resp:

                    if resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 30))
                        logger.warning(f"⚠️ Rate limited (429) → esperando {retry_after}s")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, retry_after + random.uniform(1, 5))
                        continue

                    if resp.status >= 500:
                        consecutive_errors += 1
                        backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        logger.error(f"❌ Error servidor {resp.status} → backoff {backoff:.1f}s")
                        sleep_next = backoff
                        continue

                    if resp.status != 200:
                        logger.warning(f"⚠️ Código inesperado: {resp.status}")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue

                    try:
                        data = await resp.json(content_type=None)
                    except (json.JSONDecodeError, aiohttp.ContentTypeError) as e:
                        logger.warning(f"⚠️ JSON inválido: {e}")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue

                    api_id     = data.get('id')
                    data_inner = data.get('data', {})
                    result_d   = data_inner.get('result', {})
                    max_mult   = result_d.get('maxMultiplier')

                    if not api_id or max_mult is None or max_mult <= 0:
                        logger.debug("⏳ Giro en curso o sin resultado")
                        consecutive_errors = 0
                        sleep_next = POLL_INTERVAL_OK + random.uniform(0.5, 1.5)
                        continue

                    round_id = str(api_id)
                    if round_id in g_seen_ids:
                        consecutive_errors = 0
                        sleep_next = POLL_INTERVAL_OK + random.uniform(0.5, 1.5)
                        continue

                    g_seen_ids.add(round_id)
                    g_poller_status['total_new_rounds'] += 1
                    g_poller_status['last_round_ts']     = time.time()
                    consecutive_errors = 0
                    sleep_next = POLL_INTERVAL_OK + random.uniform(0.3, 1.0)

                    logger.info(
                        f"🎰 GIRO #{g_poller_status['total_new_rounds']} | "
                        f"ID: {round_id} | {max_mult:.2f}x | "
                        f"VB4 bars: {len(g_vb4_bars)} | "
                        f"Force: {vb4_classify(float(max_mult))}"
                    )
                    await process_multiplier(float(max_mult), round_id)

            except aiohttp.ClientConnectorError as e:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                logger.error(f"🔌 Sin conexión: {e} → backoff {backoff:.1f}s")
                sleep_next = backoff

            except asyncio.TimeoutError:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                logger.error(f"⏰ Timeout → backoff {backoff:.1f}s")
                sleep_next = backoff

            except Exception as e:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                logger.exception(f"💥 Error inesperado: {e} → backoff {backoff:.1f}s")
                sleep_next = backoff

            finally:
                g_poller_status['consecutive_errors'] = consecutive_errors


# ══════════════════════════════════════════════════════════════════════════════
#  FLASK — KEEP-ALIVE
# ══════════════════════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    last_round_ago = (
        f"{int(time.time() - g_poller_status['last_round_ts'])}s atrás"
        if g_poller_status['last_round_ts'] else "sin datos"
    )
    ema4  = calc_ema_on_closes(g_vb4_bars, VB4_EMA_FAST)
    ema12 = calc_ema_on_closes(g_vb4_bars, VB4_EMA_SLOW_PRED)
    trend = "—"
    if ema4 and ema12:
        trend = "alcista 📈" if ema4[-1] > ema12[-1] else "bajista 📉"

    return (
        f"🤖 CrashBot VB4 ACTIVO | "
        f"Datos: {len(g_mults)}/400 | "
        f"VB4 velas: {len(g_vb4_bars)} | "
        f"Señal: {g_signal_state} | "
        f"VB4 trend: {trend} | "
        f"Hits/Miss: {g_vb4_hits}/{g_vb4_misses} | "
        f"Último: {last_round_ago} | "
        f"Chats: {len(g_all_chats)}"
    ), 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/stats')
def stats_route():
    last5    = [f"{m['value']:.2f}x" for m in g_mults[-5:]] if g_mults else []
    ema4     = calc_ema_on_closes(g_vb4_bars, VB4_EMA_FAST)
    ema12    = calc_ema_on_closes(g_vb4_bars, VB4_EMA_SLOW_PRED)
    last_force = g_vb4_forces[-1] if g_vb4_forces else None
    return {
        "status":              "ok",
        "mults_collected":     len(g_mults),
        "signal_state":        g_signal_state,
        "signal_type":         g_signal_type,
        "trigger_mult":        g_signal_trigger_mult,
        "session_state":       g_session.state,
        "session_col":         g_session.col,
        "wins":                g_session.wins,
        "losses":              g_session.losses,
        "registered_chats":    len(g_all_chats),
        "trend_favorable":     g_trend_favorable,
        "last_5":              last5,
        "poller_requests":     g_poller_status['total_requests'],
        "poller_new_rounds":   g_poller_status['total_new_rounds'],
        "poller_errors":       g_poller_status['consecutive_errors'],
        # VB4
        "vb4_bars":            len(g_vb4_bars),
        "vb4_ema4":            round(ema4[-1], 4)  if ema4  else None,
        "vb4_ema12":           round(ema12[-1], 4) if ema12 else None,
        "vb4_last_force":      last_force,
        "vb4_mini_cum":        g_vb4_mini_cum,
        "vb4_hits":            g_vb4_hits,
        "vb4_misses":          g_vb4_misses,
        "vb4_bars_since_resolved": len(g_vb4_bars) - g_vb4_last_resolved,
        "vb4_pending":         g_vb4_pending is not None,
        "vb4_checks":          vb4_checks_detail() if len(g_vb4_bars) >= 10 else "acumulando",
    }

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


# ── SELF-PING ────────────────────────────────────────────────────────────────
async def self_ping_loop():
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not render_url:
        logger.info("RENDER_EXTERNAL_URL no configurada — self-ping desactivado")
        return
    url = f"{render_url.rstrip('/')}/ping"
    logger.info(f"Self-ping cada 14 min → {url}")
    while True:
        await asyncio.sleep(14 * 60)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping falló: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  HANDLERS TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name = message.from_user.first_name or "usuario"
    g_all_chats.add(message.chat.id)

    stats     = get_quota_stats(200)
    stats_blk = quota_stats_text(stats)
    data_info = (
        f"📡 `{len(g_mults)}/400` multiplicadores recopilados"
        if g_mults else "📡 Recopilando datos en tiempo real..."
    )

    # VB4 status
    ema4  = calc_ema_on_closes(g_vb4_bars, VB4_EMA_FAST)
    ema12 = calc_ema_on_closes(g_vb4_bars, VB4_EMA_SLOW_PRED)
    if ema4 and ema12:
        trend_lbl = "📈 alcista" if ema4[-1] > ema12[-1] else "📉 bajista"
        vb4_info  = (
            f"🧬 VB4: `{len(g_vb4_bars)}` velas | "
            f"EMA4=`{ema4[-1]:.1f}` EMA12=`{ema12[-1]:.1f}` | {trend_lbl}\n"
            f"   Aciertos/Fallos: `{g_vb4_hits}/{g_vb4_misses}`"
        )
    else:
        vb4_info = "🧬 VB4: acumulando velas..."

    await bot.reply_to(
        message,
        f"🚀 *¡Bienvenido {name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 *Bot de Señales Crash (Stake)*\n"
        "📊 Motor VB4 | Objetivo: `2.00x`\n"
        "🔄 Gestión: 3 Columnas × 2 Intentos\n"
        f"💵 Apuesta base fija: `${BASE_BET:.2f}`\n"
        "🏆 Ciclo: 10 señales exitosas\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "*Señal VB4 — 5 condiciones (≥3/5):*\n"
        "  ✅ C1 EMA4 > EMA12 y subiendo\n"
        "  ✅ C2 ≥5/8 velas alcistas\n"
        "  ✅ C3 Fuerza media mejorando\n"
        "  ✅ C4 Mini-chart en subida\n"
        "  ✅ C5 Última vela alcista\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n"
        f"{vb4_info}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{stats_blk}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ *¡Registrado!* _Recibirás señales automáticas._",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):
    g_all_chats.add(message.chat.id)

    s      = g_session
    stats  = get_quota_stats(200)
    trend  = quota_stats_text(stats)

    # VB4 detalle
    ema4  = calc_ema_on_closes(g_vb4_bars, VB4_EMA_FAST)
    ema12 = calc_ema_on_closes(g_vb4_bars, VB4_EMA_SLOW_PRED)
    ema4_str  = f"{ema4[-1]:.2f}"  if ema4  else "—"
    ema12_str = f"{ema12[-1]:.2f}" if ema12 else "—"
    checks_str = vb4_checks_detail() if len(g_vb4_bars) >= 10 else "acumulando datos..."
    last_force = g_vb4_forces[-1] if g_vb4_forces else 0
    cooldown_rem = max(0, VB4_COOLDOWN_BARS - (len(g_vb4_bars) - g_vb4_last_resolved))

    vb4_section = (
        f"🧬 *VB4 — Estado del Motor*\n"
        f"   Velas: `{len(g_vb4_bars)}` | Fuerza actual: `{last_force}`\n"
        f"   EMA4=`{ema4_str}` | EMA12=`{ema12_str}`\n"
        f"   Mini-chart: `{g_vb4_mini_cum:+.0f}` | Cooldown: `{cooldown_rem}` rondas\n"
        f"   Aciertos/Fallos: `{g_vb4_hits}/{g_vb4_misses}`\n"
        f"   _Checks: {checks_str}_\n"
    )

    fichas_recientes = s.fichas[-15:]
    if fichas_recientes:
        lineas = []
        for f in fichas_recientes:
            c1, c2, c3 = f['c1'], f['c2'], f['c3']
            total = c1 + c2 + c3
            net   = BASE_BET if f['result'] == 'win' else -total
            res   = "✅" if f['result'] == 'win' else "❌"
            hora  = f.get('ts', '--:--')
            partes = [f"C1:${c1:.2f}"]
            if c2 > 0: partes.append(f"C2:${c2:.2f}")
            if c3 > 0: partes.append(f"C3:${c3:.2f}")
            net_txt = f"+${net:.2f}" if net >= 0 else f"-${abs(net):.2f}"
            lineas.append(f"{res} #{f['n']} {hora} | {' '.join(partes)} | {net_txt}")
        fichas_txt   = "\n".join(lineas)
        total_fichas = len(s.fichas)
        wins_f  = sum(1 for f in s.fichas if f['result'] == 'win')
        loss_f  = sum(1 for f in s.fichas if f['result'] == 'loss')
        resumen = f"Total: `{total_fichas}` | ✅ `{wins_f}` | ❌ `{loss_f}`"
    else:
        fichas_txt = "_Sin fichas aún._"
        resumen    = "Total fichas: `0`"

    poller_info = (
        f"🌐 Requests: `{g_poller_status['total_requests']}` | "
        f"Giros: `{g_poller_status['total_new_rounds']}`"
    )

    await bot.reply_to(
        message,
        "📊 *ESTADÍSTICAS DEL BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{s.status_short()}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{vb4_section}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{poller_info}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Últimas fichas:*\n{fichas_txt}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{resumen}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{trend}",
        parse_mode='Markdown'
    )


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main_async():
    logger.info("🤖 Iniciando CrashBot VB4 — API HTTPS | Objetivo 2.00x")
    await bot.set_my_commands([
        types.BotCommand('start',        '🚀 Iniciar / Ver tendencia'),
        types.BotCommand('estadisticas', '📊 Estadísticas + VB4'),
    ])
    asyncio.create_task(http_poller())
    asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas iniciadas. Iniciando polling Telegram...")
    await bot.infinity_polling(skip_pending=True)


if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask en puerto {os.environ.get('PORT', 8080)}")
    asyncio.run(main_async())
