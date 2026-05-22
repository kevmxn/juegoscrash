#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   CRASH BOT — Sistema IA/ML 3.00x                           ║
║   Motor Bayesiano + Cadena de Markov + Detección Patrones   ║
║   API HTTPS Polling | Stake Crash | Render-Ready            ║
╚══════════════════════════════════════════════════════════════╝

Mejoras v3.0 sobre v2.0:
  - CrashMLEngine: motor de IA con probabilidades bayesianas
  - Cadena de Markov de orden 3 para predicción de secuencias
  - Score de confianza compuesto (0-100%) por señal
  - Detección de patrones de pérdida (evita señales en zonas peligrosas)
  - Índice de volatilidad del mercado
  - Análisis de rachas con ajuste probabilístico
  - Umbral mínimo de confianza configurable (MIN_CONFIDENCE)
  - Comandos /ia y /mercado para lectura profunda del mercado
  - Registro y calibración de precisión del modelo
"""

import asyncio
import threading
import json
import logging
import math
import os
import statistics
import sys
import time
import random
from collections import deque
from datetime import datetime, timedelta
from typing import Optional, Tuple
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
BOT_TOKEN  = os.environ.get("BOT_TOKEN", "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
API_CRASH  = "https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest"
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1003613599867"))   # Canal de señales y tendencias

WIN_TARGET    = 2.00
MAX_MULTS     = 400
TRIM_MULTS    = 200
MAX_COLS      = 3
MAX_ATTS      = 2
CYCLE_SIZE    = 10
BASE_BET      = 0.10

# ── PARÁMETROS DE IA ──────────────────────────────────────────────────────────
MIN_CONFIDENCE    = 0.58   # Confianza mínima para emitir señal (58%)
MARKOV_MIN_OBS    = 5      # Mínimas observaciones para usar Markov
ML_HISTORY_SIZE   = 500    # Historial del motor ML

# Polling
POLL_INTERVAL     = 3.0
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
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 '
    '(KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36',
]

# ─── ESTADO GLOBAL ────────────────────────────────────────────────────────────
g_mults:    list  = []
g_seen_ids: set   = set()
g_positions: list = []
g_ema4:  list     = []
g_ema8:  list     = []
g_ema20: list     = []

g_signal_state               = 'idle'
g_signal_type: Optional[str] = None
g_signal_strictness: int     = 0
g_signal_trigger_mult: float = 0.0
g_last_signal_confidence: float = 0.0  # Para calibración del modelo

g_all_chats: set                  = set()
g_trend_favorable: Optional[bool] = None
g_signal_msg_ids: dict            = {}   # {chat_id: message_id} señal activa intento 1

g_poller_status = {
    'total_requests':   0,
    'total_new_rounds': 0,
    'consecutive_errors': 0,
    'last_poll_ts':     0.0,
    'last_round_ts':    0.0,
}

bot = AsyncTeleBot(BOT_TOKEN)


# ══════════════════════════════════════════════════════════════════════════════
#  MOTOR DE INTELIGENCIA ARTIFICIAL — CrashMLEngine
# ══════════════════════════════════════════════════════════════════════════════

class CrashMLEngine:
    """
    Motor de IA/ML para lectura y predicción del mercado Crash.

    Componentes:
    ┌─────────────────────────────────────────────────────────────┐
    │ 1. Probabilidad Bayesiana adaptativa (3 ventanas temporales)│
    │ 2. Cadena de Markov orden-3 (predicción por secuencia)      │
    │ 3. Análisis de rachas con ajuste probabilístico             │
    │ 4. Índice de volatilidad (std dev normalizada)              │
    │ 5. Detección de patrones de pérdida (memory-based)          │
    │ 6. Score de confianza compuesto (0-100%)                    │
    │ 7. Calibración continua (registro de señales y resultados)  │
    └─────────────────────────────────────────────────────────────┘
    """

    def __init__(self, history_size: int = ML_HISTORY_SIZE):
        self.history_size = history_size

        # Prior teórico del juego Crash (probabilidad de >= 2.00x ≈ 49-51%)
        self.prior_win_prob = 0.50

        # Ventanas temporales (reciente / media / larga)
        self.recent_wins = deque(maxlen=50)
        self.medium_wins = deque(maxlen=100)
        self.long_wins   = deque(maxlen=200)

        # Cadena de Markov orden-3: estado = últimos 3 resultados (W/L)
        # { ('W','L','W'): {'wins': N, 'total': N}, ... }
        self.markov_table: dict = {}
        self.markov_seq         = deque(maxlen=10)

        # Historial de multiplicadores en crudo
        self.mult_history = deque(maxlen=history_size)

        # Detección de patrones de pérdida
        # Guardamos el "contexto" de multiplicadores que precedieron una pérdida
        self.loss_patterns: deque = deque(maxlen=30)
        self._cur_winning_run: list = []   # racha de ganadas antes de pérdida

        # Calibración del modelo
        # [ {'confidence': float, 'result': bool, 'ts': float}, ... ]
        self.signal_log: list = []

        logger.info("🤖 CrashMLEngine v3.0 inicializado")

    # ── UPDATE ─────────────────────────────────────────────────────────────────
    def update(self, value: float):
        """Alimenta el motor con un nuevo multiplicador."""
        win = value >= WIN_TARGET

        # Ventanas
        self.recent_wins.append(1 if win else 0)
        self.medium_wins.append(1 if win else 0)
        self.long_wins.append(1 if win else 0)
        self.mult_history.append(value)

        # Cadena de Markov
        if len(self.markov_seq) >= 3:
            state = tuple(list(self.markov_seq)[-3:])
            entry = self.markov_table.setdefault(state, {'wins': 0, 'total': 0})
            entry['total'] += 1
            if win:
                entry['wins'] += 1

        sym = 'W' if win else 'L'
        self.markov_seq.append(sym)

        # Patrones de pérdida
        if win:
            self._cur_winning_run.append(round(value, 2))
        else:
            if self._cur_winning_run:
                self.loss_patterns.append(tuple(self._cur_winning_run))
            self._cur_winning_run = []

    # ── PROBABILIDADES ─────────────────────────────────────────────────────────
    def bayesian_win_prob(self) -> float:
        """
        Probabilidad bayesiana de que la próxima ronda sea >= 2.00x.
        Mezcla ponderada de 3 ventanas temporales + prior teórico.
        """
        if len(self.recent_wins) < 5:
            return self.prior_win_prob

        w_rec = sum(self.recent_wins) / len(self.recent_wins)
        w_med = sum(self.medium_wins) / len(self.medium_wins) if self.medium_wins else self.prior_win_prob
        w_lon = sum(self.long_wins)   / len(self.long_wins)   if self.long_wins   else self.prior_win_prob

        # Ventana reciente tiene más peso
        evidence = 0.50 * w_rec + 0.30 * w_med + 0.20 * w_lon

        # Factor de confianza: más datos → más peso a la evidencia
        alpha = min(len(self.long_wins), 200) / 200.0
        posterior = alpha * evidence + (1.0 - alpha) * self.prior_win_prob

        return max(0.01, min(0.99, posterior))

    def markov_prob(self) -> Optional[float]:
        """
        P(ganar | últimos 3 resultados) según tabla de Markov.
        Retorna None si no hay suficientes observaciones.
        """
        if len(self.markov_seq) < 3:
            return None
        state = tuple(list(self.markov_seq)[-3:])
        entry = self.markov_table.get(state)
        if not entry or entry['total'] < MARKOV_MIN_OBS:
            return None
        return entry['wins'] / entry['total']

    # ── ANÁLISIS DE MERCADO ────────────────────────────────────────────────────
    def volatility_index(self) -> float:
        """
        Índice de volatilidad normalizado [0, 1].
        1.0 = mercado muy inestable  |  0.0 = mercado estable.
        Usa coeficiente de variación de los últimos 30 multiplicadores.
        """
        if len(self.mult_history) < 10:
            return 0.5
        sample = list(self.mult_history)[-30:]
        try:
            std  = statistics.stdev(sample)
            mean = statistics.mean(sample)
            cv   = std / mean if mean > 0 else 0.0
            # CV típico en Crash: 0.5 – 3.0
            return max(0.0, min(1.0, cv / 3.0))
        except Exception:
            return 0.5

    def streak_analysis(self) -> dict:
        """
        Analiza la racha actual (pérdidas / victorias consecutivas)
        y calcula un ajuste probabilístico.

        Rachas de pérdidas largas → pequeño boost (los juegos Crash
        tienen leve autocorrelación positiva tras zonas de bajas).
        Rachas de victorias → leve penalización (regresión a la media).
        """
        seq = list(self.markov_seq)
        if not seq:
            return {'type': 'neutral', 'length': 0, 'prob_boost': 0.0, 'last': '?'}

        last   = seq[-1]
        streak = 1
        for i in range(len(seq) - 2, -1, -1):
            if seq[i] == last:
                streak += 1
            else:
                break

        prob_boost  = 0.0
        streak_type = 'neutral'

        if last == 'L':
            streak_type = 'loss_streak'
            prob_boost  = min(0.15, streak * 0.025)   # máx +15%
        elif last == 'W':
            streak_type = 'win_streak'
            prob_boost  = -min(0.10, streak * 0.015)  # máx -10%

        return {
            'type': streak_type,
            'length': streak,
            'prob_boost': prob_boost,
            'last': last,
        }

    def loss_pattern_risk(self) -> float:
        """
        Detecta si el patrón actual (run ganadora antes de próxima señal)
        se parece a patrones históricos que precedieron pérdidas.
        Retorna riesgo [0.0, 1.0].
        """
        if not self.loss_patterns or not self._cur_winning_run:
            return 0.0

        cur_len     = len(self._cur_winning_run)
        similar     = 0
        sample_pool = list(self.loss_patterns)[-15:]

        for pattern in sample_pool:
            # Patrones con longitud similar ± 1 son "parecidos"
            if abs(len(pattern) - cur_len) <= 1:
                similar += 1

        return max(0.0, min(1.0, similar / len(sample_pool)))

    # ── SCORE DE CONFIANZA ─────────────────────────────────────────────────────
    def compute_signal_confidence(self, ema_signal_level: int = 1) -> dict:
        """
        Calcula el score de confianza compuesto de la señal actual.

        Factores:
          - Probabilidad bayesiana  (40%)
          - Cadena de Markov        (30% si disponible, si no se redistribuye)
          - Ajuste por racha        (±15%)
          - Penalización volatilidad (máx -20%)
          - Penalización patrón pérdida (máx -15%)
          - Bonus EMA (S2 o S3 = +5%)

        Returns dict completo con todos los componentes.
        """
        bayes_p  = self.bayesian_win_prob()
        markov_p = self.markov_prob()
        streak   = self.streak_analysis()
        vol      = self.volatility_index()
        loss_risk = self.loss_pattern_risk()

        # Combinar bayes + markov
        if markov_p is not None:
            base_prob = 0.55 * bayes_p + 0.45 * markov_p
        else:
            base_prob = bayes_p

        # Ajuste por racha
        base_prob = max(0.01, min(0.99, base_prob + streak['prob_boost']))

        # Factores multiplicativos
        vol_factor  = 1.0 - (vol       * 0.20)   # alta volatilidad penaliza
        risk_factor = 1.0 - (loss_risk * 0.15)   # patrón de riesgo penaliza

        # Bonus por nivel de señal EMA
        ema_bonus = 0.0
        if ema_signal_level >= 2:
            ema_bonus = 0.05
        if ema_signal_level >= 3:
            ema_bonus = 0.08

        confidence = base_prob * vol_factor * risk_factor + ema_bonus
        confidence = max(0.01, min(0.99, confidence))

        return {
            'confidence':     confidence,
            'pct':            round(confidence * 100, 1),
            'bayes_pct':      round(bayes_p * 100, 1),
            'markov_pct':     round(markov_p * 100, 1) if markov_p is not None else None,
            'volatility_pct': round(vol * 100, 1),
            'loss_risk_pct':  round(loss_risk * 100, 1),
            'streak':         streak,
            'recommendation': self._label(confidence),
        }

    @staticmethod
    def _label(confidence: float) -> str:
        if confidence >= 0.72:   return '🟢 ALTA'
        elif confidence >= 0.60: return '🟡 MEDIA'
        elif confidence >= 0.50: return '🟠 BAJA'
        else:                    return '🔴 MUY BAJA'

    # ── LECTURA DE MERCADO ─────────────────────────────────────────────────────
    def get_market_reading(self) -> dict:
        """Vista completa del estado del mercado para el comando /mercado."""
        bayes    = self.bayesian_win_prob()
        markov_p = self.markov_prob()
        streak   = self.streak_analysis()
        vol      = self.volatility_index()
        loss_risk = self.loss_pattern_risk()

        # Clasificación general
        if bayes >= 0.60 and vol < 0.40:
            state = '🟢 FAVORABLE'
        elif bayes >= 0.52 and vol < 0.65:
            state = '🟡 NEUTRAL'
        elif bayes < 0.44 or vol >= 0.72:
            state = '🔴 DESFAVORABLE'
        else:
            state = '🟠 PRECAUCIÓN'

        # Estadísticas de multiplicadores recientes
        if len(self.mult_history) >= 10:
            sample = list(self.mult_history)[-20:]
            avg    = round(statistics.mean(sample), 2)
            mx     = round(max(sample), 2)
            mn     = round(min(sample), 2)
            median = round(statistics.median(sample), 2)
        else:
            avg = mx = mn = median = 0.0

        # Conteo de estados Markov conocidos
        markov_states = len(self.markov_table)
        markov_obs    = sum(e['total'] for e in self.markov_table.values())

        return {
            'state':          state,
            'win_prob_pct':   round(bayes * 100, 1),
            'markov_pct':     round(markov_p * 100, 1) if markov_p else None,
            'volatility_pct': round(vol * 100, 1),
            'loss_risk_pct':  round(loss_risk * 100, 1),
            'streak':         streak,
            'avg':            avg,
            'max_r':          mx,
            'min_r':          mn,
            'median':         median,
            'total_processed': len(self.mult_history),
            'markov_states':  markov_states,
            'markov_obs':     markov_obs,
        }

    # ── CALIBRACIÓN ────────────────────────────────────────────────────────────
    def record_signal(self, confidence: float, result: bool):
        """Registra el resultado real de una señal para calibrar el modelo."""
        self.signal_log.append({
            'confidence': confidence,
            'result':     result,
            'ts':         time.time(),
        })
        if len(self.signal_log) > 500:
            self.signal_log = self.signal_log[-500:]

    def performance_stats(self) -> dict:
        """Estadísticas de precisión del modelo predictivo."""
        log = self.signal_log
        if not log:
            return {
                'total': 0, 'accuracy': None,
                'avg_conf': None, 'high_conf_acc': None,
            }
        total   = len(log)
        wins    = sum(1 for s in log if s['result'])
        acc     = wins / total
        avg_c   = sum(s['confidence'] for s in log) / total
        hi      = [s for s in log if s['confidence'] >= 0.65]
        hi_acc  = (sum(1 for s in hi if s['result']) / len(hi)) if hi else None

        return {
            'total':        total,
            'accuracy':     round(acc * 100, 1),
            'avg_conf':     round(avg_c * 100, 1),
            'high_total':   len(hi),
            'high_conf_acc': round(hi_acc * 100, 1) if hi_acc is not None else None,
        }


# ─── INSTANCIA GLOBAL ML ──────────────────────────────────────────────────────
g_ml_engine = CrashMLEngine()


# ─── HORA ARGENTINA ───────────────────────────────────────────────────────────
def argentina_time() -> str:
    now_arg = datetime.utcnow() - timedelta(hours=3)
    return now_arg.strftime("%H:%M")


# ─── BROADCAST ────────────────────────────────────────────────────────────────
async def broadcast(msg: str, parse_mode: str = None) -> dict:
    """Envía señales/tendencias SOLO al canal oficial. Retorna {CHANNEL_ID: message_id}."""
    try:
        m = await bot.send_message(CHANNEL_ID, msg, parse_mode=parse_mode)
        return {CHANNEL_ID: m.message_id}
    except Exception as e:
        logger.warning(f"Error enviando al canal {CHANNEL_ID}: {e}")
        return {}


async def broadcast_trend_change(favorable: bool):
    hora  = argentina_time()
    stats = get_quota_stats(200)
    trend = quota_stats_text(stats)

    if favorable:
        header = f"🟢 *TENDENCIA FAVORABLE — {hora}*\n"
    else:
        header = f"🔴 *TENDENCIA DESFAVORABLE — {hora}*\n"

    msg = (
        f"{header}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{trend}"
    )
    logger.info(f"📢 Broadcast tendencia: {'FAVORABLE' if favorable else 'DESFAVORABLE'} → {len(g_all_chats)} chats")
    await broadcast(msg, parse_mode='Markdown')


# ─── MOTOR DE EMAs ────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if not data:
        return []
    k   = 2 / (period + 1)
    ema = [data[0]]
    for i in range(1, len(data)):
        ema.append((data[i] - ema[i - 1]) * k + ema[i - 1])
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

    unfavorable = pct1 > 52.0 or pct2 < 29.0

    return {
        'total':           total,
        'has_enough':      total >= 200,
        'favorable':       not unfavorable,
        'count_100_199':   r1,
        'count_200_499':   r2,
        'count_500_999':   r3,
        'count_1000_plus': r4,
        'pct_100_199':     pct1,
        'pct_200_499':     pct2,
        'pct_500_999':     pct3,
        'pct_1000_plus':   pct4,
    }


def quota_stats_text(stats: dict) -> str:
    if stats['total'] == 0:
        return "📡 _Sin datos suficientes para analizar cuotas._\n"

    n_label = "200" if stats['has_enough'] else f"{stats['total']} (acumulando...)"
    r1_flag = " ✅" if stats['pct_100_199'] <= 52.0 else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= 29.0 else " ❌"

    if stats['favorable']:
        fav_line = "✅ *¡TENDENCIA FAVORABLE!*\n      _Se recomienda operar_"
    else:
        fav_line = "⚠️ *TENDENCIA DESFAVORABLE*\n      _Se recomienda esperar_"

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


# ─── DETECCIÓN DE SEÑAL EMA ───────────────────────────────────────────────────
def check_moderate_signal() -> Optional[Tuple[str, int]]:
    """
    Retorna ('alert200', strictness) o None.
    S1 → EMA8 cruza por encima de EMA20
    S2 → patrón V + precio sobre las 3 EMAs
    S3 → 2 consecutivos ≥2.00 + EMAs alineadas (4>8>20)
    """
    pos  = g_positions
    e4   = g_ema4
    e8   = g_ema8
    e20  = g_ema20
    data = g_mults

    if len(data) < 4 or len(pos) < 4:
        return None

    cur_pos = pos[-1]
    cur_e4  = e4[-1]  if e4           else cur_pos
    cur_e8  = e8[-1]  if e8           else cur_pos
    cur_e20 = e20[-1] if e20          else cur_pos
    prv_e8  = e8[-2]  if len(e8)  > 1 else cur_e8
    prv_e20 = e20[-2] if len(e20) > 1 else cur_e20

    if len(e8) >= 2 and prv_e8 <= prv_e20 and cur_e8 > cur_e20:
        return ('alert200', 1)

    if len(pos) >= 3:
        a, b, c = pos[-3], pos[-2], pos[-1]
        if (abs(a - c) <= 1 and b > a
                and cur_pos > cur_e4
                and cur_pos > cur_e8
                and cur_pos > cur_e20):
            return ('alert200', 2)

    if (len(data) >= 2
            and data[-1]['value'] >= WIN_TARGET
            and data[-2]['value'] >= WIN_TARGET
            and cur_e4 > cur_e8 > cur_e20):
        before = data[-3] if len(data) >= 3 else None
        if before is None or before['value'] < WIN_TARGET:
            return ('alert200', 3)

    return None


# ─── SESIÓN GLOBAL ────────────────────────────────────────────────────────────
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

        self.fichas: list    = carry_fichas if carry_fichas is not None else []
        self._cur_ficha: dict = None
        self._col_attempt_bets: list = []   # apuestas del intento actual de columna

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
        self.entries += 1
        prev_bet = self.cur_bet
        prev_col = self.col

        if self._cur_ficha is not None:
            col_key = f'c{prev_col}'
            self._cur_ficha[col_key] = self._cur_ficha.get(col_key, 0.0) + prev_bet

        # Registrar apuesta de este intento en la columna
        self._col_attempt_bets.append(prev_bet)

        if win:
            self.wins   += 1
            self.lost    = 0.0
            self.cur_bet = self.base_bet
            self.col     = 1
            self.attempt = 1
            self.scale  += 1
            self._col_attempt_bets = []

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
        total_f = len(self.fichas)
        wins_f  = sum(1 for f in self.fichas if f['result'] == 'win')
        pct     = round(wins_f / total_f * 100, 2) if total_f > 0 else 0.0
        return f"📈 Ganancias/Perdidas: `{pct:.2f}%`"


# ─── INSTANCIA GLOBAL ─────────────────────────────────────────────────────────
g_session: GlobalSession = GlobalSession()


def reset_global_session():
    global g_session
    old_fichas = list(g_session.fichas)
    g_session  = GlobalSession(carry_fichas=old_fichas)
    logger.info("🔄 Sesión global reiniciada — fichas preservadas")


# ─── PROCESADOR DE MULTIPLICADORES ───────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_type, g_signal_strictness, g_signal_trigger_mult
    global g_positions, g_ema4, g_ema8, g_ema20, g_mults, g_seen_ids
    global g_trend_favorable, g_session, g_last_signal_confidence

    logger.info(
        f"🎲 {value:.2f}x | ID: {round_id} | "
        f"Señal: {g_signal_state}/{g_signal_type} (S{g_signal_strictness})"
    )

    # ── FASE 1: Resultado principal ──────────────────────────────────────────
    if g_signal_state == 'evaluating':
        win = value >= WIN_TARGET
        # Calibrar modelo con el resultado real
        if g_last_signal_confidence > 0:
            g_ml_engine.record_signal(g_last_signal_confidence, win)
            g_last_signal_confidence = 0.0

        if g_session.state == GlobalSession.EVALUATING:
            tipo, bet = g_session.on_result(win)
            await _dispatch_result(value, tipo, bet, is_so=False)
            g_signal_state = 'so' if g_session.state == GlobalSession.WAITING_SO else 'idle'
            if g_signal_state != 'so':
                g_signal_type       = None
                g_signal_strictness = 0
        else:
            g_signal_state      = 'idle'
            g_signal_type       = None
            g_signal_strictness = 0

    # ── FASE 2: Resultado SO ─────────────────────────────────────────────────
    elif g_signal_state == 'so':
        win = value >= WIN_TARGET
        g_signal_state      = 'idle'
        g_signal_type       = None
        g_signal_strictness = 0
        if g_session.state == GlobalSession.WAITING_SO:
            tipo, bet = g_session.on_result(win)
            await _dispatch_result(value, tipo, bet, is_so=True)

    # ── FASE 3: Actualizar datos + EMAs + ML ─────────────────────────────────
    increment = 1 if value >= WIN_TARGET else -1
    prev = g_positions[-1] if g_positions else 0
    g_positions.append(prev + increment)
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})

    # Alimentar motor ML con el nuevo multiplicador
    g_ml_engine.update(value)

    if len(g_mults) >= MAX_MULTS:
        g_mults[:]     = g_mults[-TRIM_MULTS:]
        g_positions[:] = g_positions[-TRIM_MULTS:]
        logger.info(f"✂️ Datos recortados a {TRIM_MULTS} registros")

    g_ema4  = calc_ema(g_positions, 4)
    g_ema8  = calc_ema(g_positions, 8)
    g_ema20 = calc_ema(g_positions, 20)

    if len(g_seen_ids) > 2000:
        g_seen_ids.clear()

    # ── FASE 4: Cambio de tendencia ─────────────────────────────────────────
    stats_trend = get_quota_stats(200)
    if stats_trend['total'] >= 10:
        new_fav = stats_trend['favorable']
        if new_fav != g_trend_favorable:
            g_trend_favorable = new_fav
            asyncio.create_task(broadcast_trend_change(new_fav))

    # ── FASE 5: Detectar nueva señal con filtro ML ───────────────────────────
    if g_signal_state == 'idle' and g_session.state == GlobalSession.IDLE:
        sig_result = check_moderate_signal()
        if sig_result:
            sig_type, strictness = sig_result
            if strictness >= g_session.col:
                # Calcular confianza ML ANTES de decidir si proceder
                ml_data = g_ml_engine.compute_signal_confidence(ema_signal_level=strictness)
                confidence = ml_data['confidence']

                if g_session.col > 1:
                    # En columnas > 1 siempre se opera (ya estamos en recuperación)
                    proceed = True
                else:
                    stats_now = get_quota_stats(200)
                    trend_ok  = (stats_now['total'] == 0) or (stats_now['favorable'] is not False)
                    # FILTRO ML: solo señales con confianza suficiente
                    ml_ok     = confidence >= MIN_CONFIDENCE
                    proceed   = trend_ok and ml_ok
                    if not ml_ok:
                        logger.info(
                            f"🤖 Señal S{strictness} bloqueada por ML — "
                            f"confianza {ml_data['pct']}% < {MIN_CONFIDENCE*100:.0f}%"
                        )

                if proceed:
                    g_signal_state            = 'evaluating'
                    g_signal_type             = sig_type
                    g_signal_strictness       = strictness
                    g_signal_trigger_mult     = value
                    g_last_signal_confidence  = confidence
                    g_session.signal_trigger_mult = value
                    g_session.state = GlobalSession.EVALUATING

                    if g_session.col == 1:
                        g_session.start_ficha()

                    logger.info(
                        f"🚀 SEÑAL S{strictness} Col{g_session.col} | "
                        f"Trigger: {value:.2f}x | Confianza ML: {ml_data['pct']}%"
                    )
                    await _send_signal(value, strictness, ml_data)


# ─── MENSAJERÍA ───────────────────────────────────────────────────────────────
async def _send_signal(trigger: float, strictness: int, ml_data: dict):
    """Envía la señal de intento 1 en formato compacto y guarda IDs de mensaje."""
    global g_signal_msg_ids
    txt = (
        f"🚨 Entrar después de: `{trigger:.2f}x`\n"
        f"💎 Señal para `{WIN_TARGET:.2f}x`\n"
        f"🇺🇲 Apuesta USD: `${g_session.cur_bet:.2f}`\n"
        f"🆔 Gestión C{g_session.col} — Intento 1/{MAX_ATTS}"
    )
    g_signal_msg_ids = await broadcast(txt, parse_mode='Markdown')


async def _send_so_signal(trigger_value: float):
    """Elimina el mensaje de intento 1 del canal y envía señal de Segunda Oportunidad."""
    global g_signal_msg_ids
    # Borrar el mensaje del intento 1 en el canal
    for chat_id, msg_id in list(g_signal_msg_ids.items()):
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception as e:
            logger.warning(f"No se pudo borrar mensaje {msg_id} en canal {chat_id}: {e}")
    g_signal_msg_ids = {}

    txt = (
        f"🚨 Entrar después de: `{trigger_value:.2f}x`\n"
        f"💎 Segunda Oportunidad — `{WIN_TARGET:.2f}x`\n"
        f"🇺🇲 Apuesta USD: `${g_session.cur_bet:.2f}`\n"
        f"🆔 Gestión C{g_session.col} — Intento 2/{MAX_ATTS}"
    )
    # Los mensajes de SO NO se guardan (se mantienen en el canal si se pierde)
    await broadcast(txt, parse_mode='Markdown')


def _confidence_bar(confidence: float) -> str:
    """Genera una barra visual de confianza."""
    filled = round(confidence * 10)
    bar    = "█" * filled + "░" * (10 - filled)
    return f"`[{bar}]`"


async def _check_trend_after_cycle():
    stats = get_quota_stats(200)
    if stats['total'] > 0 and not stats['favorable']:
        hora  = argentina_time()
        trend = quota_stats_text(stats)
        await broadcast(
            f"🔴 *TENDENCIA DESFAVORABLE — {hora}*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{trend}"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⏳ _El bot esperará hasta que la tendencia mejore._\n"
            "_Se notificará automáticamente cuando sea favorable._",
            parse_mode='Markdown'
        )
    else:
        logger.info("✅ Post-ciclo: tendencia favorable — bot continúa analizando")


async def _dispatch_result(value: float, tipo: str, bet: float, is_so: bool):
    global g_session, g_signal_msg_ids

    gale_num = 1 if is_so else 0   # GALE #0 = intento 1, GALE #1 = intento 2

    if tipo in ('win', 'cycle_win'):
        win_txt = f"✅ WIN  GALE #{gale_num} ({value:.2f}x) 🇺🇲 ${BASE_BET:.2f}"
        await broadcast(win_txt)

        if tipo == 'cycle_win':
            cycle_txt = (
                "━━━━━━━━━━━━━━━━━━━━━━━\n"
                "🏆 *¡CICLO COMPLETO — 10 señales exitosas!*\n"
                f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n"
                "🔄 _Sesión reiniciada automáticamente_"
            )
            await broadcast(cycle_txt, parse_mode='Markdown')
            reset_global_session()
            await _check_trend_after_cycle()
        return

    elif tipo == 'so':
        # Guardar el resultado del intento 1 para el mensaje LOSS posterior
        g_session.attempt1_result_value = value
        # Eliminar señal intento 1 y enviar señal intento 2
        await _send_so_signal(value)
        return

    elif tipo in ('new_col', 'cycle_loss'):
        r1  = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        # La columna que falló: col ya fue incrementado en on_result
        lost_col = g_session.col - 1 if tipo == 'new_col' else MAX_COLS
        # Total perdido en esta columna = suma de apuestas registradas
        col_total = sum(g_session._col_attempt_bets) if g_session._col_attempt_bets else bet
        # Resetear apuestas de columna tras el fallo
        g_session._col_attempt_bets = []
        g_session.attempt1_result_value = 0.0

        loss_txt = (
            f"❌ LOSS C{lost_col} ({r1} | {value:.2f}x) "
            f"🇺🇲 $-{col_total:.2f}"
        )
        await broadcast(loss_txt)

        if tipo == 'cycle_loss':
            cycle_txt = (
                "━━━━━━━━━━━━━━━━━━━━━━━\n"
                "⚠️ *CICLO TERMINADO — 3 Columnas Fallidas*\n"
                f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n"
                "🔄 _Sesión reiniciada automáticamente_"
            )
            await broadcast(cycle_txt, parse_mode='Markdown')
            reset_global_session()
            await _check_trend_after_cycle()
        return

    else:
        await broadcast(f"Resultado inesperado: {tipo}")


# ─── POLLER HTTPS ─────────────────────────────────────────────────────────────
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
                    'User-Agent':     ua,
                    'Accept':         'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control':  'no-cache',
                }

                g_poller_status['total_requests'] += 1
                g_poller_status['last_poll_ts']    = time.time()

                async with session.get(
                    API_CRASH,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                    ssl=True,
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

                    api_id      = data.get('id')
                    data_inner  = data.get('data', {})
                    result      = data_inner.get('result', {})
                    max_mult    = result.get('maxMultiplier')

                    if not api_id or max_mult is None or max_mult <= 0:
                        logger.debug(f"⏳ Giro en curso o sin resultado: {data}")
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
                        f"🎰 NUEVO GIRO #{g_poller_status['total_new_rounds']} | "
                        f"ID: {round_id} | Multiplicador: {max_mult:.2f}x"
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


# ─── KEEP-ALIVE FLASK ─────────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    last_round_ago = (
        f"{int(time.time() - g_poller_status['last_round_ts'])}s atrás"
        if g_poller_status['last_round_ts'] else "sin datos aún"
    )
    ml = g_ml_engine.get_market_reading()
    return (
        f"🤖 CrashBot IA v3.0 ACTIVO | "
        f"Datos: {len(g_mults)}/400 | "
        f"Sesión: {g_session.state} | "
        f"Señal: {g_signal_state} | "
        f"Giros: {g_poller_status['total_new_rounds']} | "
        f"Último: {last_round_ago} | "
        f"Chats: {len(g_all_chats)} | "
        f"ML: {ml['state']} ({ml['win_prob_pct']}%)"
    ), 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/stats')
def stats_route():
    last5  = [f"{m['value']:.2f}x" for m in g_mults[-5:]] if g_mults else []
    ml_r   = g_ml_engine.get_market_reading()
    ml_p   = g_ml_engine.performance_stats()
    return {
        "status":             "ok",
        "mults_collected":    len(g_mults),
        "signal_state":       g_signal_state,
        "signal_type":        g_signal_type,
        "trigger_mult":       g_signal_trigger_mult,
        "session_state":      g_session.state,
        "session_col":        g_session.col,
        "wins":               g_session.wins,
        "losses":             g_session.losses,
        "fichas_total":       len(g_session.fichas),
        "registered_chats":   len(g_all_chats),
        "trend_favorable":    g_trend_favorable,
        "last_5":             last5,
        "poller_requests":    g_poller_status['total_requests'],
        "poller_new_rounds":  g_poller_status['total_new_rounds'],
        "poller_errors":      g_poller_status['consecutive_errors'],
        "ml_market":          ml_r,
        "ml_performance":     ml_p,
    }

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


# ─── SELF-PING ────────────────────────────────────────────────────────────────
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


# ─── HANDLERS DE TELEGRAM ─────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name = message.from_user.first_name or "usuario"

    stats     = get_quota_stats(200)
    stats_blk = quota_stats_text(stats)
    data_info = (
        f"📡 `{len(g_mults)}/400` multiplicadores recopilados"
        if g_mults else
        "📡 Recopilando datos en tiempo real..."
    )

    await bot.reply_to(
        message,
        f"🚀 *¡Bienvenido {name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 *Bot de Señales Crash (Stake) — IA v3.0*\n"
        "📊 Sistema Moderado | Objetivo: `2.00x`\n"
        "🔄 Gestión: 3 Columnas × 2 Intentos\n"
        f"💵 Apuesta base fija: `${BASE_BET:.2f}`\n"
        "🏆 Ciclo: 10 señales exitosas\n"
        f"🧠 Motor ML: Bayesiano + Markov + Patrones\n"
        f"🎯 Umbral mínimo de confianza: `{MIN_CONFIDENCE*100:.0f}%`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📢 *Señales y tendencias → canal oficial*\n"
        "🤖 *Comandos disponibles aquí:*\n"
        "  /estadisticas — Ver estadísticas\n"
        "  /ia — Análisis motor IA\n"
        "  /mercado — Estado del mercado\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{stats_blk}",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['ia'])
async def cmd_ia(message):
    """Muestra el análisis completo del motor de IA."""

    ml   = g_ml_engine.compute_signal_confidence(ema_signal_level=1)
    perf = g_ml_engine.performance_stats()
    streak = ml['streak']

    conf_bar = _confidence_bar(ml['confidence'])

    markov_line = (
        f"🔗 Markov P(ganar): `{ml['markov_pct']}%`\n"
        if ml['markov_pct'] is not None
        else "🔗 Markov: _acumulando datos..._\n"
    )

    perf_block = ""
    if perf['total'] > 0:
        perf_block = (
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 *Calibración del Modelo*\n"
            f"📌 Señales registradas: `{perf['total']}`\n"
            f"✅ Precisión global: `{perf['accuracy']}%`\n"
            f"🎯 Confianza promedio emitida: `{perf['avg_conf']}%`\n"
        )
        if perf['high_conf_acc'] is not None:
            perf_block += (
                f"💎 Señales alta confianza (≥65%): `{perf['high_total']}`\n"
                f"🏆 Precisión alta confianza: `{perf['high_conf_acc']}%`\n"
            )
    else:
        perf_block = (
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📋 _Sin señales registradas aún para calibración._\n"
        )

    streak_emoji = "📉" if streak['type'] == 'loss_streak' else "📈"
    streak_desc  = "pérdidas" if streak['type'] == 'loss_streak' else "victorias"

    await bot.reply_to(
        message,
        "🤖 *ANÁLISIS DE IA — Motor ML Crash*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 *Confianza actual: `{ml['pct']}%`* {ml['recommendation']}\n"
        f"{conf_bar}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📐 Prob. Bayesiana: `{ml['bayes_pct']}%`\n"
        f"{markov_line}"
        f"📊 Volatilidad mercado: `{ml['volatility_pct']}%`\n"
        f"⚠️ Riesgo patrón pérdida: `{ml['loss_risk_pct']}%`\n"
        f"{streak_emoji} Racha actual: `{streak['length']} {streak_desc}`\n"
        f"📉 Ajuste por racha: `{streak['prob_boost']:+.1%}`\n"
        f"🎯 Umbral mínimo IA: `{MIN_CONFIDENCE*100:.0f}%`\n"
        f"{'✅ IA APRUEBA señales' if ml['confidence'] >= MIN_CONFIDENCE else '⛔ IA BLOQUEA señales ahora'}\n"
        f"{perf_block}",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['mercado'])
async def cmd_mercado(message):
    """Lectura completa del estado del mercado Crash."""

    mr   = g_ml_engine.get_market_reading()
    hora = argentina_time()

    markov_line = (
        f"🔗 Prob. Markov (secuencia): `{mr['markov_pct']}%`\n"
        if mr['markov_pct'] is not None
        else "🔗 Markov: _acumulando observaciones..._\n"
    )

    streak = mr['streak']
    s_emoji = "📉" if streak['type'] == 'loss_streak' else "📈"
    s_desc  = "pérdidas" if streak['type'] == 'loss_streak' else "victorias"

    markov_db = (
        f"🗄️ Estados Markov aprendidos: `{mr['markov_states']}`\n"
        f"📊 Observaciones totales: `{mr['markov_obs']}`\n"
        if mr['markov_states'] > 0
        else ""
    )

    await bot.reply_to(
        message,
        f"📡 *ESTADO DEL MERCADO CRASH — {hora}*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 Estado general: *{mr['state']}*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 P(próxima ≥ 2.00x): `{mr['win_prob_pct']}%`\n"
        f"{markov_line}"
        f"📊 Volatilidad: `{mr['volatility_pct']}%`\n"
        f"⚠️ Riesgo patrón pérdida: `{mr['loss_risk_pct']}%`\n"
        f"{s_emoji} Racha actual: `{streak['length']} {s_desc}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 *Estadísticas últimas 20 rondas*\n"
        f"📊 Promedio: `{mr['avg']}x` | Mediana: `{mr['median']}x`\n"
        f"🔝 Máximo: `{mr['max_r']}x` | 🔻 Mínimo: `{mr['min_r']}x`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔢 Multiplicadores procesados: `{mr['total_processed']}`\n"
        f"{markov_db}",
        parse_mode='Markdown'
    )


@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):

    s      = g_session
    stats  = get_quota_stats(200)
    trend  = quota_stats_text(stats)
    perf   = g_ml_engine.performance_stats()

    # ── Ganancias/Perdidas en % ──────────────────────────────────────────────
    gp_line = s.status_short()

    # ── Últimas fichas ───────────────────────────────────────────────────────
    fichas_recientes = s.fichas[-15:]
    if fichas_recientes:
        lineas = []
        for f in fichas_recientes:
            c1    = f['c1']
            c2    = f['c2']
            c3    = f['c3']
            total = c1 + c2 + c3
            net   = BASE_BET if f['result'] == 'win' else -total
            res   = "✅" if f['result'] == 'win' else "❌"
            hora  = f.get('ts', '--:--')

            partes = [f"C1:${c1:.2f}"]
            if c2 > 0:
                partes.append(f"C2:${c2:.2f}")
            if c3 > 0:
                partes.append(f"C3:${c3:.2f}")
            cols_txt = " ".join(partes)

            net_txt = f"+${net:.2f}" if net >= 0 else f"-${abs(net):.2f}"
            lineas.append(f"{res} #{f['n']} {hora} | {cols_txt} | {net_txt}")

        fichas_txt   = "\n".join(lineas)
        total_fichas = len(s.fichas)
        wins_f  = sum(1 for f in s.fichas if f['result'] == 'win')
        loss_f  = sum(1 for f in s.fichas if f['result'] == 'loss')
        resumen = f"Total fichas: `{total_fichas}` | ✅ `{wins_f}` | ❌ `{loss_f}`"
    else:
        fichas_txt = "_Sin fichas registradas aún._"
        resumen    = "Total fichas: `0` | ✅ `0` | ❌ `0`"

    # ── Precisión IA (opcional) ──────────────────────────────────────────────
    ml_perf = ""
    if perf['total'] > 0:
        ml_perf = (
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 *Motor IA — Precisión*\n"
            f"📌 Señales analizadas: `{perf['total']}`\n"
            f"✅ Precisión: `{perf['accuracy']}%` | Conf. promedio: `{perf['avg_conf']}%`\n"
        )
        if perf['high_conf_acc'] is not None:
            ml_perf += f"💎 Alta confianza (≥65%): `{perf['high_conf_acc']}%` en `{perf['high_total']}` señales\n"

    await bot.reply_to(
        message,
        "📊 *ESTADÍSTICAS DEL BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{gp_line}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Últimas fichas (C1 + C2 + C3):*\n"
        f"{fichas_txt}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{resumen}\n"
        f"{ml_perf}"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{trend}",
        parse_mode='Markdown'
    )


# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    logger.info("🤖 Iniciando CrashBot IA v3.0 — Bayesiano + Markov + Patrones")

    await bot.set_my_commands([
        types.BotCommand('start',        '🚀 Iniciar / Ver tendencia'),
        types.BotCommand('estadisticas', '📊 Ver estadísticas'),
        types.BotCommand('ia',           '🤖 Análisis del motor IA'),
        types.BotCommand('mercado',      '📡 Estado del mercado Crash'),
    ])
    logger.info("✅ Comandos configurados: /start, /estadisticas, /ia, /mercado")

    asyncio.create_task(http_poller())
    asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas de fondo iniciadas. Iniciando polling Telegram...")
    await bot.infinity_polling(skip_pending=True)


if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask iniciado en puerto {os.environ.get('PORT', 8080)}")

    asyncio.run(main_async())
