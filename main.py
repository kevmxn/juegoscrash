#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   CRASH BOT IA v4.0 — Estrategias Duales (Stake Crash + Maestro)            ║
║   Motor Bayesiano + Cadena de Markov + Detección Patrones + Estrategia      ║
║   Maestro (rachas, soporte/resistencia, ratio verdes)                       ║
║   API HTTPS Polling | Dashboard Web | Telegram                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

Características principales:
  - Mantiene toda la funcionalidad original (EMAs, ML, gestión de columnas)
  - Añade la estrategia predictiva del juego Maestro (Galaxsys)
  - Genera señales combinadas (OR/AND) con confianza propia
  - Dashboard web /maestro con gráficos y estadísticas en tiempo real
  - Comandos de Telegram: /start, /estadisticas, /ia, /mercado, /maestro
"""

import asyncio
import threading
import json
import logging
import math
import os
import random
import statistics
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any

import aiohttp
from flask import Flask, jsonify, render_template_string
from telebot.async_telebot import AsyncTeleBot
from telebot import types

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
API_CRASH   = "https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest"
CHANNEL_ID  = int(os.environ.get("CHANNEL_ID", "-1003613599867"))

WIN_TARGET    = 2.00
MAX_MULTS     = 400
TRIM_MULTS    = 200
MAX_COLS      = 3
MAX_ATTS      = 2
CYCLE_SIZE    = 10
BASE_BET      = 0.10

# ── PARÁMETROS IA ORIGINAL ────────────────────────────────────────────────────
MIN_CONFIDENCE    = 0.58   # confianza mínima para emitir señal (ML)
MARKOV_MIN_OBS    = 5
ML_HISTORY_SIZE   = 500

# ── PARÁMETROS ESTRATEGIA MAESTRO ─────────────────────────────────────────────
MAESTRO_MIN_CONFIDENCE = 0.55      # confianza mínima desde Maestro
MAESTRO_HISTORY_SIZE   = 100       # resultados guardados para análisis
USE_MAESTRO_STRATEGY   = True      # activar/desactivar estrategia Maestro
SIGNAL_COMBINE_MODE    = "OR"      # "OR" = cualquiera, "AND" = ambas deben coincidir

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
]

# ─────────────────────────────────────────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────────────────────────────────────────
g_mults: list = []          # lista de dicts {'id', 'value', 'ts'}
g_seen_ids: set = set()
g_positions: list = []      # posición acumulada (para EMAs)
g_ema4: list = []
g_ema8: list = []
g_ema20: list = []

g_signal_state = 'idle'          # 'idle', 'evaluating', 'so'
g_signal_type: Optional[str] = None
g_signal_strictness: int = 0
g_signal_trigger_mult: float = 0.0
g_last_signal_confidence: float = 0.0   # confianza global de la señal emitida

g_all_chats: set = set()
g_trend_favorable: Optional[bool] = None
g_signal_msg_ids: dict = {}       # {chat_id: message_id}

# Estado para la estrategia Maestro
g_maestro_results: list = []      # lista de dicts {'value': float, 'win': bool}
g_maestro_last_prediction: dict = {}   # última predicción generada

g_poller_status = {
    'total_requests': 0,
    'total_new_rounds': 0,
    'consecutive_errors': 0,
    'last_poll_ts': 0.0,
    'last_round_ts': 0.0,
}

bot = AsyncTeleBot(BOT_TOKEN)

# ─────────────────────────────────────────────────────────────────────────────
# MOTOR DE IA ORIGINAL (Bayes + Markov)
# ─────────────────────────────────────────────────────────────────────────────
class CrashMLEngine:
    def __init__(self, history_size: int = ML_HISTORY_SIZE):
        self.history_size = history_size
        self.prior_win_prob = 0.50
        self.recent_wins = deque(maxlen=50)
        self.medium_wins = deque(maxlen=100)
        self.long_wins = deque(maxlen=200)
        self.markov_table: dict = {}
        self.markov_seq = deque(maxlen=10)
        self.mult_history = deque(maxlen=history_size)
        self.loss_patterns: deque = deque(maxlen=30)
        self._cur_winning_run: list = []
        self.signal_log: list = []
        logger.info("🤖 CrashMLEngine v3.0 inicializado")

    def update(self, value: float):
        win = value >= WIN_TARGET
        self.recent_wins.append(1 if win else 0)
        self.medium_wins.append(1 if win else 0)
        self.long_wins.append(1 if win else 0)
        self.mult_history.append(value)

        if len(self.markov_seq) >= 3:
            state = tuple(list(self.markov_seq)[-3:])
            entry = self.markov_table.setdefault(state, {'wins': 0, 'total': 0})
            entry['total'] += 1
            if win:
                entry['wins'] += 1

        sym = 'W' if win else 'L'
        self.markov_seq.append(sym)

        if win:
            self._cur_winning_run.append(round(value, 2))
        else:
            if self._cur_winning_run:
                self.loss_patterns.append(tuple(self._cur_winning_run))
            self._cur_winning_run = []

    def bayesian_win_prob(self) -> float:
        if len(self.recent_wins) < 5:
            return self.prior_win_prob
        w_rec = sum(self.recent_wins) / len(self.recent_wins)
        w_med = sum(self.medium_wins) / len(self.medium_wins) if self.medium_wins else self.prior_win_prob
        w_lon = sum(self.long_wins) / len(self.long_wins) if self.long_wins else self.prior_win_prob
        evidence = 0.50 * w_rec + 0.30 * w_med + 0.20 * w_lon
        alpha = min(len(self.long_wins), 200) / 200.0
        posterior = alpha * evidence + (1.0 - alpha) * self.prior_win_prob
        return max(0.01, min(0.99, posterior))

    def markov_prob(self) -> Optional[float]:
        if len(self.markov_seq) < 3:
            return None
        state = tuple(list(self.markov_seq)[-3:])
        entry = self.markov_table.get(state)
        if not entry or entry['total'] < MARKOV_MIN_OBS:
            return None
        return entry['wins'] / entry['total']

    def volatility_index(self) -> float:
        if len(self.mult_history) < 10:
            return 0.5
        sample = list(self.mult_history)[-30:]
        try:
            std = statistics.stdev(sample)
            mean = statistics.mean(sample)
            cv = std / mean if mean > 0 else 0.0
            return max(0.0, min(1.0, cv / 3.0))
        except Exception:
            return 0.5

    def streak_analysis(self) -> dict:
        seq = list(self.markov_seq)
        if not seq:
            return {'type': 'neutral', 'length': 0, 'prob_boost': 0.0, 'last': '?'}
        last = seq[-1]
        streak = 1
        for i in range(len(seq) - 2, -1, -1):
            if seq[i] == last:
                streak += 1
            else:
                break
        prob_boost = 0.0
        streak_type = 'neutral'
        if last == 'L':
            streak_type = 'loss_streak'
            prob_boost = min(0.15, streak * 0.025)
        elif last == 'W':
            streak_type = 'win_streak'
            prob_boost = -min(0.10, streak * 0.015)
        return {'type': streak_type, 'length': streak, 'prob_boost': prob_boost, 'last': last}

    def loss_pattern_risk(self) -> float:
        if not self.loss_patterns or not self._cur_winning_run:
            return 0.0
        cur_len = len(self._cur_winning_run)
        similar = 0
        sample_pool = list(self.loss_patterns)[-15:]
        for pattern in sample_pool:
            if abs(len(pattern) - cur_len) <= 1:
                similar += 1
        return max(0.0, min(1.0, similar / len(sample_pool)))

    def compute_signal_confidence(self, ema_signal_level: int = 1) -> dict:
        bayes_p = self.bayesian_win_prob()
        markov_p = self.markov_prob()
        streak = self.streak_analysis()
        vol = self.volatility_index()
        loss_risk = self.loss_pattern_risk()

        if markov_p is not None:
            base_prob = 0.55 * bayes_p + 0.45 * markov_p
        else:
            base_prob = bayes_p

        base_prob = max(0.01, min(0.99, base_prob + streak['prob_boost']))
        vol_factor = 1.0 - (vol * 0.20)
        risk_factor = 1.0 - (loss_risk * 0.15)
        ema_bonus = 0.05 if ema_signal_level >= 2 else (0.08 if ema_signal_level >= 3 else 0.0)
        confidence = base_prob * vol_factor * risk_factor + ema_bonus
        confidence = max(0.01, min(0.99, confidence))
        return {
            'confidence': confidence,
            'pct': round(confidence * 100, 1),
            'bayes_pct': round(bayes_p * 100, 1),
            'markov_pct': round(markov_p * 100, 1) if markov_p is not None else None,
            'volatility_pct': round(vol * 100, 1),
            'loss_risk_pct': round(loss_risk * 100, 1),
            'streak': streak,
            'recommendation': self._label(confidence),
        }

    @staticmethod
    def _label(confidence: float) -> str:
        if confidence >= 0.72:   return '🟢 ALTA'
        elif confidence >= 0.60: return '🟡 MEDIA'
        elif confidence >= 0.50: return '🟠 BAJA'
        else:                    return '🔴 MUY BAJA'

    def get_market_reading(self) -> dict:
        bayes = self.bayesian_win_prob()
        markov_p = self.markov_prob()
        streak = self.streak_analysis()
        vol = self.volatility_index()
        loss_risk = self.loss_pattern_risk()

        if bayes >= 0.60 and vol < 0.40:
            state = '🟢 FAVORABLE'
        elif bayes >= 0.52 and vol < 0.65:
            state = '🟡 NEUTRAL'
        elif bayes < 0.44 or vol >= 0.72:
            state = '🔴 DESFAVORABLE'
        else:
            state = '🟠 PRECAUCIÓN'

        if len(self.mult_history) >= 10:
            sample = list(self.mult_history)[-20:]
            avg = round(statistics.mean(sample), 2)
            mx = round(max(sample), 2)
            mn = round(min(sample), 2)
            median = round(statistics.median(sample), 2)
        else:
            avg = mx = mn = median = 0.0

        markov_states = len(self.markov_table)
        markov_obs = sum(e['total'] for e in self.markov_table.values())
        return {
            'state': state,
            'win_prob_pct': round(bayes * 100, 1),
            'markov_pct': round(markov_p * 100, 1) if markov_p else None,
            'volatility_pct': round(vol * 100, 1),
            'loss_risk_pct': round(loss_risk * 100, 1),
            'streak': streak,
            'avg': avg,
            'max_r': mx,
            'min_r': mn,
            'median': median,
            'total_processed': len(self.mult_history),
            'markov_states': markov_states,
            'markov_obs': markov_obs,
        }

    def record_signal(self, confidence: float, result: bool):
        self.signal_log.append({'confidence': confidence, 'result': result, 'ts': time.time()})
        if len(self.signal_log) > 500:
            self.signal_log = self.signal_log[-500:]

    def performance_stats(self) -> dict:
        if not self.signal_log:
            return {'total': 0, 'accuracy': None, 'avg_conf': None, 'high_conf_acc': None}
        total = len(self.signal_log)
        wins = sum(1 for s in self.signal_log if s['result'])
        acc = wins / total
        avg_c = sum(s['confidence'] for s in self.signal_log) / total
        hi = [s for s in self.signal_log if s['confidence'] >= 0.65]
        hi_acc = (sum(1 for s in hi if s['result']) / len(hi)) if hi else None
        return {
            'total': total,
            'accuracy': round(acc * 100, 1),
            'avg_conf': round(avg_c * 100, 1),
            'high_total': len(hi),
            'high_conf_acc': round(hi_acc * 100, 1) if hi_acc is not None else None,
        }

g_ml_engine = CrashMLEngine()

# ─────────────────────────────────────────────────────────────────────────────
# ESTRATEGIA MAESTRO (adaptada de Maestro.html)
# ─────────────────────────────────────────────────────────────────────────────
class MaestroStrategy:
    """Lógica predictiva del juego Maestro aplicada a Crash (objetivo 2.00x)"""

    @staticmethod
    def get_category(val: float) -> str:
        if val < 1.50:
            return 'low'
        if val < 2.00:
            return 'mid'
        if val < 3.00:
            return 'good'
        if val < 10.00:
            return 'high'
        return 'vhigh'

    @staticmethod
    def analyze_trend(results: List[Dict]) -> Dict[str, Any]:
        """
        Analiza la tendencia según la lógica Maestro.
        Retorna: prediction, risk, detail, confidence.
        """
        if len(results) < 3:
            return {'prediction': 'Cargando datos...', 'risk': 'wait', 'detail': 'Esperando más resultados', 'confidence': 0.0}

        vals = [r['value'] for r in results[:10]]
        avg = sum(vals) / len(vals)
        last3 = vals[:3]
        last3avg = sum(last3) / len(last3)
        green_ratio = sum(1 for v in vals if v >= WIN_TARGET) / len(vals)

        last = vals[0]
        second_last = vals[1] if len(vals) > 1 else vals[0]

        # Contar racha actual de rojos (menos de 2.00)
        streak_red = 0
        for v in vals:
            if v >= WIN_TARGET:
                break
            streak_red += 1

        # Zona de entrada: rojo seguido de verde
        if second_last < WIN_TARGET and last >= WIN_TARGET:
            confidence = 0.65 + min(0.20, streak_red * 0.05)
            return {
                'prediction': 'Zona de entrada detectada',
                'risk': 'low',
                'detail': f'Rojo seguido de verde (racha roja: {streak_red})',
                'confidence': min(0.85, confidence)
            }

        # Rachas largas de rojo (>=3) predicen posible verde
        if streak_red >= 3:
            confidence = 0.55 + min(0.25, (streak_red - 2) * 0.07)
            return {
                'prediction': f'Posible entrada tras {streak_red} rojos',
                'risk': 'low' if streak_red >= 4 else 'medium',
                'detail': f'Racha roja de {streak_red}',
                'confidence': min(0.80, confidence)
            }

        # Tendencia alcista: últimos 3 superan media general
        if last3avg > avg and last >= WIN_TARGET:
            confidence = 0.60
            return {
                'prediction': 'Tendencia alcista activa',
                'risk': 'low',
                'detail': f'Últimos 3: {last3avg:.2f}x > Media: {avg:.2f}x',
                'confidence': confidence
            }

        # Señal neutra
        return {
            'prediction': 'Monitoreando...',
            'risk': 'medium',
            'detail': f'Último: {last:.2f}x | Verdes: {green_ratio*100:.0f}%',
            'confidence': 0.35
        }

    @staticmethod
    def calculate_support_resistance(results: List[Dict]) -> Dict[str, Optional[float]]:
        values = [r['value'] for r in results]
        if len(values) < 10:
            return {'support': None, 'resistance': None}
        window = max(3, len(values) // 8)
        smoothed = []
        for i in range(len(values)):
            lo = max(0, i - 3)
            hi = min(len(values) - 1, i + 3)
            smoothed.append(sum(values[lo:hi+1]) / (hi - lo + 1))
        support = None
        resistance = None
        for i in range(window, len(smoothed) - window):
            is_min = all(smoothed[i] <= smoothed[j] for j in range(i - window, i + window + 1) if j != i)
            is_max = all(smoothed[i] >= smoothed[j] for j in range(i - window, i + window + 1) if j != i)
            if is_min and (support is None or smoothed[i] > support):
                support = smoothed[i]
            if is_max and (resistance is None or smoothed[i] < resistance):
                resistance = smoothed[i]
        return {'support': support, 'resistance': resistance}

    def should_enter(self, results: List[Dict]) -> Tuple[bool, float, str]:
        if not USE_MAESTRO_STRATEGY or len(results) < 3:
            return False, 0.0, "Estrategia desactivada o datos insuficientes"
        trend = self.analyze_trend(results)
        conf = trend['confidence']
        # Solo generar señal si la confianza supera el umbral
        if conf >= MAESTRO_MIN_CONFIDENCE and trend['risk'] in ('low', 'medium'):
            return True, conf, trend['detail']
        return False, conf, trend['detail']

maestro_strategy = MaestroStrategy()

# ─────────────────────────────────────────────────────────────────────────────
# GESTIÓN DE SESIÓN (columnas, intentos, fichas)
# ─────────────────────────────────────────────────────────────────────────────
class GlobalSession:
    IDLE = 'idle'
    EVALUATING = 'evaluating'
    WAITING_SO = 'waiting_so'
    DONE = 'done'

    def __init__(self, carry_fichas: list = None):
        self.base_bet = BASE_BET
        self.state = self.IDLE
        self.scale = 1
        self.col = 1
        self.attempt = 1
        self.lost = 0.0
        self.cur_bet = BASE_BET
        self.entries = 0
        self.wins = 0
        self.losses = 0
        self.created = datetime.now()
        self.signal_trigger_mult = 0.0
        self.attempt1_result_value = 0.0
        self.fichas: list = carry_fichas if carry_fichas is not None else []
        self._cur_ficha: dict = None
        self._col_attempt_bets: list = []

    def start_ficha(self):
        self._cur_ficha = {
            'n': len(self.fichas) + 1,
            'c1': 0.0, 'c2': 0.0, 'c3': 0.0,
            'result': None,
            'ts': argentina_time(),
        }

    def on_result(self, win: bool) -> tuple:
        self.entries += 1
        prev_bet = self.cur_bet
        prev_col = self.col
        if self._cur_ficha is not None:
            col_key = f'c{prev_col}'
            self._cur_ficha[col_key] = self._cur_ficha.get(col_key, 0.0) + prev_bet
        self._col_attempt_bets.append(prev_bet)

        if win:
            self.wins += 1
            self.lost = 0.0
            self.cur_bet = self.base_bet
            self.col = 1
            self.attempt = 1
            self.scale += 1
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
            self.losses += 1
            self.lost += prev_bet
            self.cur_bet = self.lost + self.base_bet
            self.attempt += 1
            if self.attempt > MAX_ATTS:
                self.attempt = 1
                self.col += 1
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
        wins_f = sum(1 for f in self.fichas if f['result'] == 'win')
        pct = round(wins_f / total_f * 100, 2) if total_f > 0 else 0.0
        return f"📈 Ganadas/Perdidas: `{pct:.2f}%`"

g_session = GlobalSession()

def reset_global_session():
    global g_session
    old_fichas = list(g_session.fichas)
    g_session = GlobalSession(carry_fichas=old_fichas)
    logger.info("🔄 Sesión global reiniciada — fichas preservadas")

# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────
def argentina_time() -> str:
    now_arg = datetime.utcnow() - timedelta(hours=3)
    return now_arg.strftime("%H:%M")

def calc_ema(data: list, period: int) -> list:
    if not data:
        return []
    k = 2 / (period + 1)
    ema = [data[0]]
    for i in range(1, len(data)):
        ema.append((data[i] - ema[i - 1]) * k + ema[i - 1])
    return ema

def get_quota_stats(n: int = 200) -> dict:
    data = g_mults[-n:] if len(g_mults) >= n else g_mults[:]
    total = len(data)
    if total == 0:
        return {'total': 0, 'has_enough': False, 'favorable': None,
                'count_100_199': 0, 'count_200_499': 0, 'count_500_999': 0, 'count_1000_plus': 0,
                'pct_100_199': 0.0, 'pct_200_499': 0.0, 'pct_500_999': 0.0, 'pct_1000_plus': 0.0}
    r1 = sum(1 for m in data if 1.00 <= m['value'] < 2.00)
    r2 = sum(1 for m in data if 2.00 <= m['value'] < 5.00)
    r3 = sum(1 for m in data if 5.00 <= m['value'] < 10.00)
    r4 = sum(1 for m in data if m['value'] >= 10.00)
    pct1 = r1 / total * 100
    pct2 = r2 / total * 100
    pct3 = r3 / total * 100
    pct4 = r4 / total * 100
    unfavorable = pct1 > 52.0 or pct2 < 29.0
    return {
        'total': total,
        'has_enough': total >= 200,
        'favorable': not unfavorable,
        'count_100_199': r1, 'count_200_499': r2, 'count_500_999': r3, 'count_1000_plus': r4,
        'pct_100_199': pct1, 'pct_200_499': pct2, 'pct_500_999': pct3, 'pct_1000_plus': pct4,
    }

def quota_stats_text(stats: dict) -> str:
    if stats['total'] == 0:
        return "📡 _Sin datos suficientes para analizar cuotas._\n"
    n_label = "200" if stats['has_enough'] else f"{stats['total']} (acumulando...)"
    r1_flag = " ✅" if stats['pct_100_199'] <= 52.0 else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= 29.0 else " ❌"
    fav_line = "✅ *¡TENDENCIA FAVORABLE!*\n      _Se recomienda operar_" if stats['favorable'] else "⚠️ *TENDENCIA DESFAVORABLE*\n      _Se recomienda esperar_"
    return (
        f"📈 *Análisis de la Tendencia últimos*\n"
        f"      *{n_label} multiplicadores*\n"
        f"🔵 Cuotas (1.00-1.99x): `{stats['count_100_199']}` — {stats['pct_100_199']:.2f}%{r1_flag}\n"
        f"🟣 Cuotas (2.00-4.99x): `{stats['count_200_499']}` — {stats['pct_200_499']:.2f}%{r2_flag}\n"
        f"🟡 Cuotas (5.00-9.99x): `{stats['count_500_999']}` — {stats['pct_500_999']:.2f}%\n"
        f"🔴 Cuotas (+10.00x):    `{stats['count_1000_plus']}` — {stats['pct_1000_plus']:.2f}%\n"
        " \n" + fav_line + "\n"
    )

def check_moderate_signal() -> Optional[Tuple[str, int]]:
    pos, e4, e8, e20, data = g_positions, g_ema4, g_ema8, g_ema20, g_mults
    if len(data) < 4 or len(pos) < 4:
        return None
    cur_pos = pos[-1]
    cur_e4 = e4[-1] if e4 else cur_pos
    cur_e8 = e8[-1] if e8 else cur_pos
    cur_e20 = e20[-1] if e20 else cur_pos
    prv_e8 = e8[-2] if len(e8) > 1 else cur_e8
    prv_e20 = e20[-2] if len(e20) > 1 else cur_e20
    if len(e8) >= 2 and prv_e8 <= prv_e20 and cur_e8 > cur_e20:
        return ('alert200', 1)
    if len(pos) >= 3:
        a, b, c = pos[-3], pos[-2], pos[-1]
        if abs(a - c) <= 1 and b > a and cur_pos > cur_e4 and cur_pos > cur_e8 and cur_pos > cur_e20:
            return ('alert200', 2)
    if len(data) >= 2 and data[-1]['value'] >= WIN_TARGET and data[-2]['value'] >= WIN_TARGET and cur_e4 > cur_e8 > cur_e20:
        before = data[-3] if len(data) >= 3 else None
        if before is None or before['value'] < WIN_TARGET:
            return ('alert200', 3)
    return None

# ─────────────────────────────────────────────────────────────────────────────
# BROADCAST Y MENSAJERÍA
# ─────────────────────────────────────────────────────────────────────────────
async def broadcast(msg: str, parse_mode: str = None) -> dict:
    try:
        m = await bot.send_message(CHANNEL_ID, msg, parse_mode=parse_mode)
        return {CHANNEL_ID: m.message_id}
    except Exception as e:
        logger.warning(f"Error enviando al canal {CHANNEL_ID}: {e}")
        return {}

async def broadcast_trend_change(favorable: bool):
    hora = argentina_time()
    stats = get_quota_stats(200)
    trend = quota_stats_text(stats)
    header = f"🟢 *TENDENCIA FAVORABLE — {hora}*\n" if favorable else f"🔴 *TENDENCIA DESFAVORABLE — {hora}*\n"
    msg = header + "━━━━━━━━━━━━━━━━━━━━━━━\n" + trend
    await broadcast(msg, parse_mode='Markdown')

async def _send_signal(trigger: float, strictness: int, ml_data: dict, maestro_data: dict = None):
    """Envía señal con información combinada de IA y Maestro."""
    global g_signal_msg_ids
    lines = [f"🚨 Entrar después de: `{trigger:.2f}x`", f"💎 Señal para `{WIN_TARGET:.2f}x`",
             f"🇺🇲 Apuesta USD: `${g_session.cur_bet:.2f}`",
             f"🆔 Gestión C{g_session.col} — Intento 1/{MAX_ATTS}"]
    if maestro_data and maestro_data.get('active'):
        lines.append(f"🧠 Estrategia: Maestro | razón: {maestro_data['reason']}")
    else:
        lines.append(f"🤖 Confianza IA: {ml_data['pct']}% ({ml_data['recommendation']})")
    txt = "\n".join(lines)
    g_signal_msg_ids = await broadcast(txt, parse_mode='Markdown')

async def _send_so_signal(trigger_value: float):
    global g_signal_msg_ids
    for chat_id, msg_id in list(g_signal_msg_ids.items()):
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
    g_signal_msg_ids = {}
    txt = (f"🚨 Entrar después de: `{trigger_value:.2f}x`\n"
           f"💎 Segunda Oportunidad — `{WIN_TARGET:.2f}x`\n"
           f"🇺🇲 Apuesta USD: `${g_session.cur_bet:.2f}`\n"
           f"🆔 Gestión C{g_session.col} — Intento 2/{MAX_ATTS}")
    await broadcast(txt, parse_mode='Markdown')

async def _dispatch_result(value: float, tipo: str, bet: float, is_so: bool):
    global g_session
    gale_num = 1 if is_so else 0
    if tipo in ('win', 'cycle_win'):
        win_txt = f"✅ WIN  GALE #{gale_num} ({value:.2f}x) 🇺🇲 ${BASE_BET:.2f}"
        await broadcast(win_txt)
        if tipo == 'cycle_win':
            cycle_txt = ("━━━━━━━━━━━━━━━━━━━━━━━\n"
                         "🏆 *¡CICLO COMPLETO — 10 señales exitosas!*\n"
                         f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n"
                         "🔄 _Sesión reiniciada automáticamente_")
            await broadcast(cycle_txt, parse_mode='Markdown')
            reset_global_session()
            await _check_trend_after_cycle()
        return
    elif tipo == 'so':
        g_session.attempt1_result_value = value
        await _send_so_signal(value)
        return
    elif tipo in ('new_col', 'cycle_loss'):
        r1 = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        lost_col = g_session.col - 1 if tipo == 'new_col' else MAX_COLS
        col_total = sum(g_session._col_attempt_bets) if g_session._col_attempt_bets else bet
        g_session._col_attempt_bets = []
        g_session.attempt1_result_value = 0.0
        loss_txt = f"❌ LOSS C{lost_col} ({r1} | {value:.2f}x) 🇺🇲 $-{col_total:.2f}"
        await broadcast(loss_txt)
        if tipo == 'cycle_loss':
            cycle_txt = ("━━━━━━━━━━━━━━━━━━━━━━━\n"
                         "⚠️ *CICLO TERMINADO — 3 Columnas Fallidas*\n"
                         f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n"
                         "🔄 _Sesión reiniciada automáticamente_")
            await broadcast(cycle_txt, parse_mode='Markdown')
            reset_global_session()
            await _check_trend_after_cycle()
        return
    else:
        await broadcast(f"Resultado inesperado: {tipo}")

async def _check_trend_after_cycle():
    stats = get_quota_stats(200)
    if stats['total'] > 0 and not stats['favorable']:
        hora = argentina_time()
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

def _confidence_bar(confidence: float) -> str:
    filled = round(confidence * 10)
    return f"`[{'█' * filled}{'░' * (10 - filled)}]`"

# ─────────────────────────────────────────────────────────────────────────────
# PROCESAMIENTO DE MULTIPLICADORES (NÚCLEO)
# ─────────────────────────────────────────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_type, g_signal_strictness, g_signal_trigger_mult
    global g_positions, g_ema4, g_ema8, g_ema20, g_mults, g_seen_ids
    global g_trend_favorable, g_session, g_last_signal_confidence, g_maestro_results

    logger.info(f"🎲 {value:.2f}x | ID: {round_id} | Señal: {g_signal_state}/{g_signal_type} (S{g_signal_strictness})")

    # ────────── 1. Resultado de señal activa (evaluating o so) ──────────
    if g_signal_state == 'evaluating':
        win = value >= WIN_TARGET
        if g_last_signal_confidence > 0:
            g_ml_engine.record_signal(g_last_signal_confidence, win)
            g_last_signal_confidence = 0.0

        if g_session.state == GlobalSession.EVALUATING:
            tipo, bet = g_session.on_result(win)
            await _dispatch_result(value, tipo, bet, is_so=False)
            g_signal_state = 'so' if g_session.state == GlobalSession.WAITING_SO else 'idle'
            if g_signal_state != 'so':
                g_signal_type = None
                g_signal_strictness = 0
        else:
            g_signal_state = 'idle'
            g_signal_type = None
            g_signal_strictness = 0

    elif g_signal_state == 'so':
        win = value >= WIN_TARGET
        g_signal_state = 'idle'
        g_signal_type = None
        g_signal_strictness = 0
        if g_session.state == GlobalSession.WAITING_SO:
            tipo, bet = g_session.on_result(win)
            await _dispatch_result(value, tipo, bet, is_so=True)

    # ────────── 2. Actualizar datos generales y ML ──────────
    increment = 1 if value >= WIN_TARGET else -1
    prev = g_positions[-1] if g_positions else 0
    g_positions.append(prev + increment)
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})
    g_ml_engine.update(value)

    # Actualizar historial Maestro
    g_maestro_results.insert(0, {'value': value, 'win': value >= WIN_TARGET})
    if len(g_maestro_results) > MAESTRO_HISTORY_SIZE:
        g_maestro_results.pop()

    # Podar datos si es necesario
    if len(g_mults) >= MAX_MULTS:
        g_mults[:] = g_mults[-TRIM_MULTS:]
        g_positions[:] = g_positions[-TRIM_MULTS:]
        logger.info(f"✂️ Datos recortados a {TRIM_MULTS} registros")

    g_ema4 = calc_ema(g_positions, 4)
    g_ema8 = calc_ema(g_positions, 8)
    g_ema20 = calc_ema(g_positions, 20)

    if len(g_seen_ids) > 2000:
        g_seen_ids.clear()

    # ────────── 3. Cambio de tendencia global ──────────
    stats_trend = get_quota_stats(200)
    if stats_trend['total'] >= 10:
        new_fav = stats_trend['favorable']
        if new_fav != g_trend_favorable:
            g_trend_favorable = new_fav
            asyncio.create_task(broadcast_trend_change(new_fav))

    # ────────── 4. Detectar nueva señal (combinando estrategias) ──────────
    if g_signal_state == 'idle' and g_session.state == GlobalSession.IDLE:
        sig_result = check_moderate_signal()
        if sig_result:
            sig_type, strictness = sig_result
            if strictness >= g_session.col:
                # Obtener confianza del motor ML
                ml_data = g_ml_engine.compute_signal_confidence(ema_signal_level=strictness)
                conf_ml = ml_data['confidence']

                # Evaluar estrategia Maestro
                maestro_should, conf_maestro, maestro_reason = maestro_strategy.should_enter(g_maestro_results)
                maestro_active = USE_MAESTRO_STRATEGY and maestro_should and conf_maestro >= MAESTRO_MIN_CONFIDENCE

                # Decidir si proceder según modo de combinación
                proceed = False
                used_strategy = None
                if SIGNAL_COMBINE_MODE == "OR":
                    proceed = (conf_ml >= MIN_CONFIDENCE) or maestro_active
                else:  # AND
                    proceed = (conf_ml >= MIN_CONFIDENCE) and maestro_active

                # Además, verificar tendencia general (solo si estamos en columna 1)
                if g_session.col == 1:
                    stats_now = get_quota_stats(200)
                    trend_ok = (stats_now['total'] == 0) or (stats_now['favorable'] is not False)
                    proceed = proceed and trend_ok

                if proceed:
                    # Elegir qué confianza mostrar (la mayor o la combinada)
                    if maestro_active and conf_maestro > conf_ml:
                        final_confidence = conf_maestro
                        used_strategy = "Maestro"
                    else:
                        final_confidence = conf_ml
                        used_strategy = "IA original"

                    g_signal_state = 'evaluating'
                    g_signal_type = sig_type
                    g_signal_strictness = strictness
                    g_signal_trigger_mult = value
                    g_last_signal_confidence = final_confidence
                    g_session.signal_trigger_mult = value
                    g_session.state = GlobalSession.EVALUATING

                    if g_session.col == 1:
                        g_session.start_ficha()

                    logger.info(f"🚀 SEÑAL S{strictness} Col{g_session.col} | Trigger: {value:.2f}x | "
                                f"Confianza final: {final_confidence:.2%} | Estrategia: {used_strategy}")

                    maestro_info = {'active': maestro_active, 'reason': maestro_reason} if maestro_active else None
                    await _send_signal(value, strictness, ml_data, maestro_info)

# ─────────────────────────────────────────────────────────────────────────────
# POLLER HTTPS (Stake Crash API)
# ─────────────────────────────────────────────────────────────────────────────
async def http_poller():
    consecutive_errors = 0
    sleep_next = POLL_INTERVAL_OK
    logger.info(f"📡 Iniciando poller HTTP → {API_CRASH}")
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(sleep_next)
            try:
                ua = random.choice(USER_AGENTS)
                headers = {'User-Agent': ua, 'Accept': 'application/json', 'Cache-Control': 'no-cache'}
                g_poller_status['total_requests'] += 1
                g_poller_status['last_poll_ts'] = time.time()
                async with session.get(API_CRASH, headers=headers, timeout=aiohttp.ClientTimeout(total=10), ssl=True) as resp:
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
                    api_id = data.get('id')
                    result = data.get('data', {}).get('result', {})
                    max_mult = result.get('maxMultiplier')
                    if not api_id or max_mult is None or max_mult <= 0:
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
                    g_poller_status['last_round_ts'] = time.time()
                    consecutive_errors = 0
                    sleep_next = POLL_INTERVAL_OK + random.uniform(0.3, 1.0)
                    logger.info(f"🎰 NUEVO GIRO #{g_poller_status['total_new_rounds']} | ID: {round_id} | Multiplicador: {max_mult:.2f}x")
                    await process_multiplier(float(max_mult), round_id)
            except Exception as e:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                logger.exception(f"💥 Error inesperado: {e} → backoff {backoff:.1f}s")
                sleep_next = backoff
            finally:
                g_poller_status['consecutive_errors'] = consecutive_errors

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD WEB (Maestro style)
# ─────────────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

MAESTRO_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Maestro Crash - Dashboard IA</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', sans-serif; background: #0a0e17; color: #fff; padding: 16px; }
  .container { max-width: 800px; margin: 0 auto; }
  .card { background: rgba(255,255,255,0.04); border-radius: 12px; padding: 14px; margin-bottom: 14px; border: 1px solid rgba(255,255,255,0.06); }
  .prediction { border-left: 3px solid #a855f7; }
  .stats { display: grid; grid-template-columns: repeat(3,1fr); gap: 8px; text-align: center; }
  .stat-value { font-size: 20px; font-weight: bold; font-family: monospace; }
  .result-pill { display: inline-block; padding: 4px 8px; border-radius: 6px; margin: 2px; font-family: monospace; background: rgba(255,255,255,0.05); }
  .result-pill.latest { background: #a855f7; color: white; }
  .chart-container { height: 250px; margin: 12px 0; }
  .level-row { display: flex; justify-content: space-between; margin: 12px 0; }
  .level { text-align: center; }
  .level-label { font-size: 11px; color: #aaa; }
  .level-value { font-size: 18px; font-weight: bold; }
  .support { color: #10b981; }
  .resistance { color: #ef4444; }
  button { background: #a855f7; border: none; padding: 8px 16px; border-radius: 8px; color: white; cursor: pointer; margin-top: 8px; }
  .status { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #22c55e; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1}50%{opacity:.5} }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
<div class="container">
  <h2>📡 Maestro Crash Tracker <span style="font-size:12px;">(integración IA)</span></h2>
  <div class="card prediction" id="predictionCard">
    <div><span class="status"></span> <span id="predictionText">Cargando...</span></div>
    <div id="predictionDetail" style="font-size:12px;color:#aaa;"></div>
  </div>
  <div class="level-row">
    <div class="level"><div class="level-label">Soporte</div><div class="level-value support" id="support">-</div></div>
    <div class="level"><div class="level-label">Resistencia</div><div class="level-value resistance" id="resistance">-</div></div>
  </div>
  <div class="stats">
    <div><div>Promedio</div><div class="stat-value" id="avg">-</div></div>
    <div><div>Máximo</div><div class="stat-value" id="max">-</div></div>
    <div><div>Verdes %</div><div class="stat-value" id="greenPct">-</div></div>
  </div>
  <div class="chart-container"><canvas id="trendChart"></canvas></div>
  <div id="resultsList" style="margin-top: 12px;"></div>
  <button onclick="location.reload()">Actualizar</button>
</div>
<script>
  let chart;
  async function fetchData() {
    try {
      const res = await fetch('/api/maestro_data');
      const data = await res.json();
      document.getElementById('predictionText').innerText = data.prediction;
      document.getElementById('predictionDetail').innerText = data.detail;
      document.getElementById('support').innerText = data.support !== null ? data.support.toFixed(2)+'x' : '-';
      document.getElementById('resistance').innerText = data.resistance !== null ? data.resistance.toFixed(2)+'x' : '-';
      document.getElementById('avg').innerText = data.avg.toFixed(2)+'x';
      document.getElementById('max').innerText = data.max.toFixed(2)+'x';
      document.getElementById('greenPct').innerText = data.green_pct+'%';
      const container = document.getElementById('resultsList');
      container.innerHTML = data.results.map((r,i) => `<span class="result-pill ${i===0?'latest':''}">${r.value.toFixed(2)}x</span>`).join('');
      if (chart) chart.destroy();
      const ctx = document.getElementById('trendChart').getContext('2d');
      chart = new Chart(ctx, {
        type: 'line',
        data: { labels: data.chart_labels, datasets: [{ label: 'Multiplicador', data: data.chart_values, borderColor: '#a855f7', tension: 0.1, fill: true, backgroundColor: 'rgba(168,85,247,0.1)' }] },
        options: { responsive: true, maintainAspectRatio: true }
      });
    } catch(e) { console.error(e); }
  }
  fetchData();
  setInterval(fetchData, 3000);
</script>
</body>
</html>
"""

@flask_app.route('/')
def home():
    last_round_ago = f"{int(time.time() - g_poller_status['last_round_ts'])}s atrás" if g_poller_status['last_round_ts'] else "sin datos"
    ml = g_ml_engine.get_market_reading()
    return f"🤖 CrashBot IA v4.0 ACTIVO | Datos: {len(g_mults)}/400 | Señal: {g_signal_state} | Giros: {g_poller_status['total_new_rounds']} | ML: {ml['state']} ({ml['win_prob_pct']}%)", 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/maestro')
def maestro_dashboard():
    return render_template_string(MAESTRO_HTML)

@flask_app.route('/api/maestro_data')
def api_maestro_data():
    # Preparar datos para el dashboard
    results = g_maestro_results[:30]  # últimos 30
    values = [r['value'] for r in results]
    if values:
        avg = sum(values) / len(values)
        max_val = max(values)
        green_pct = sum(1 for v in values if v >= WIN_TARGET) / len(values) * 100
    else:
        avg = max_val = green_pct = 0.0
    trend = maestro_strategy.analyze_trend(results)
    sr = maestro_strategy.calculate_support_resistance(results)
    return jsonify({
        'prediction': trend['prediction'],
        'detail': trend['detail'],
        'support': sr['support'],
        'resistance': sr['resistance'],
        'avg': avg,
        'max': max_val,
        'green_pct': round(green_pct, 1),
        'results': [{'value': r['value']} for r in results],
        'chart_values': values[::-1],   # orden cronológico para gráfico
        'chart_labels': list(range(len(values))),
    })

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ─────────────────────────────────────────────────────────────────────────────
# SELF-PING (para mantener Render activo)
# ─────────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────────
# HANDLERS DE TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name = message.from_user.first_name or "usuario"
    stats = get_quota_stats(200)
    stats_blk = quota_stats_text(stats)
    data_info = f"📡 `{len(g_mults)}/400` multiplicadores recopilados" if g_mults else "📡 Recopilando datos..."
    await bot.reply_to(message,
        f"🚀 *¡Bienvenido {name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 *Bot de Señales Crash (Stake) — IA v4.0*\n"
        "📊 Estrategias: IA Bayesiana + Markov + Maestro (rachas)\n"
        f"🎯 Objetivo: `{WIN_TARGET:.2f}x` | Gestión: 3C×2I\n"
        f"💵 Apuesta base: `${BASE_BET:.2f}`\n"
        f"🧠 Umbral IA: `{MIN_CONFIDENCE*100:.0f}%` | Maestro: `{MAESTRO_MIN_CONFIDENCE*100:.0f}%`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📢 *Señales en el canal oficial*\n"
        "🤖 *Comandos:* /estadisticas /ia /mercado /maestro\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n{stats_blk}",
        parse_mode='Markdown')

@bot.message_handler(commands=['ia'])
async def cmd_ia(message):
    ml = g_ml_engine.compute_signal_confidence(1)
    perf = g_ml_engine.performance_stats()
    conf_bar = _confidence_bar(ml['confidence'])
    markov_line = f"🔗 Markov P(ganar): `{ml['markov_pct']}%`\n" if ml['markov_pct'] else "🔗 Markov: _acumulando..._\n"
    perf_block = ""
    if perf['total'] > 0:
        perf_block = (f"━━━━━━━━━━━━━━━━━━━━━━━\n📋 *Calibración del Modelo*\n"
                      f"📌 Señales: `{perf['total']}` | Precisión: `{perf['accuracy']}%`\n"
                      f"🎯 Confianza promedio: `{perf['avg_conf']}%`\n")
        if perf['high_conf_acc']:
            perf_block += f"💎 Alta confianza (≥65%): `{perf['high_conf_acc']}%` en `{perf['high_total']}` señales\n"
    else:
        perf_block = "━━━━━━━━━━━━━━━━━━━━━━━\n📋 _Sin señales registradas aún._\n"
    await bot.reply_to(message,
        "🤖 *ANÁLISIS IA (Bayes+Markov)*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 *Confianza actual: `{ml['pct']}%`* {ml['recommendation']}\n{conf_bar}\n"
        f"📐 Bayes: `{ml['bayes_pct']}%`\n{markov_line}"
        f"📊 Volatilidad: `{ml['volatility_pct']}%` | Riesgo patrón: `{ml['loss_risk_pct']}%`\n"
        f"📉 Ajuste racha: `{ml['streak']['prob_boost']:+.1%}`\n"
        f"🎯 Umbral IA: `{MIN_CONFIDENCE*100:.0f}%` → {'✅ APRUEBA' if ml['confidence']>=MIN_CONFIDENCE else '⛔ BLOQUEA'}\n"
        f"{perf_block}",
        parse_mode='Markdown')

@bot.message_handler(commands=['mercado'])
async def cmd_mercado(message):
    mr = g_ml_engine.get_market_reading()
    hora = argentina_time()
    markov_line = f"🔗 Markov: `{mr['markov_pct']}%`\n" if mr['markov_pct'] else "🔗 Markov: _acumulando..._\n"
    streak = mr['streak']
    s_emoji = "📉" if streak['type'] == 'loss_streak' else "📈"
    await bot.reply_to(message,
        f"📡 *ESTADO MERCADO CRASH — {hora}*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌐 Estado general: *{mr['state']}*\n"
        f"🎯 P(≥{WIN_TARGET:.2f}x): `{mr['win_prob_pct']}%`\n{markov_line}"
        f"📊 Volatilidad: `{mr['volatility_pct']}%` | Riesgo patrón: `{mr['loss_risk_pct']}%`\n"
        f"{s_emoji} Racha: `{streak['length']} {'pérdidas' if streak['type']=='loss_streak' else 'victorias'}`\n"
        f"📈 Últimas 20: Prom `{mr['avg']}x` | Mediana `{mr['median']}x` | Máx `{mr['max_r']}x`\n"
        f"🔢 Procesados: `{mr['total_processed']}` | Estados Markov: `{mr['markov_states']}`",
        parse_mode='Markdown')

@bot.message_handler(commands=['maestro'])
async def cmd_maestro(message):
    """Muestra la última predicción de la estrategia Maestro."""
    if not g_maestro_results:
        await bot.reply_to(message, "📡 *Estrategia Maestro*: Aún no hay suficientes datos. Esperando multiplicadores...", parse_mode='Markdown')
        return
    trend = maestro_strategy.analyze_trend(g_maestro_results)
    sr = maestro_strategy.calculate_support_resistance(g_maestro_results)
    should, conf, reason = maestro_strategy.should_enter(g_maestro_results)
    await bot.reply_to(message,
        f"🎭 *Estrategia Maestro* (adaptada a Crash {WIN_TARGET:.2f}x)\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Predicción: `{trend['prediction']}`\n"
        f"📝 Detalle: {trend['detail']}\n"
        f"🎯 Confianza: `{conf*100:.1f}%` (umbral {MAESTRO_MIN_CONFIDENCE*100:.0f}%)\n"
        f"🚦 Señal actual: {'✅ ACTIVA' if should else '❌ INACTIVA'}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Soporte: {sr['support']:.2f}x" if sr['support'] else "🟢 Soporte: -" + "\n"
        f"🔴 Resistencia: {sr['resistance']:.2f}x" if sr['resistance'] else "🔴 Resistencia: -",
        parse_mode='Markdown')

@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):
    s = g_session
    stats = get_quota_stats(200)
    trend = quota_stats_text(stats)
    perf = g_ml_engine.performance_stats()
    gp_line = s.status_short()
    # últimas fichas
    fichas_recientes = s.fichas[-15:]
    if fichas_recientes:
        lineas = []
        for f in fichas_recientes:
            total = f['c1'] + f['c2'] + f['c3']
            net = BASE_BET if f['result'] == 'win' else -total
            res = "✅" if f['result'] == 'win' else "❌"
            cols_txt = f"C1:${f['c1']:.2f}" + (f" C2:${f['c2']:.2f}" if f['c2']>0 else "") + (f" C3:${f['c3']:.2f}" if f['c3']>0 else "")
            lineas.append(f"{res} #{f['n']} {f.get('ts','--:--')} | {cols_txt} | {'+$'+str(net) if net>=0 else '-$'+str(abs(net))}")
        fichas_txt = "\n".join(lineas)
        total_fichas = len(s.fichas)
        wins_f = sum(1 for f in s.fichas if f['result'] == 'win')
        resumen = f"Total fichas: `{total_fichas}` | ✅ `{wins_f}` | ❌ `{total_fichas-wins_f}`"
    else:
        fichas_txt = "_Sin fichas registradas aún._"
        resumen = "Total fichas: `0` | ✅ `0` | ❌ `0`"
    ml_perf = ""
    if perf['total'] > 0:
        ml_perf = (f"━━━━━━━━━━━━━━━━━━━━━━━\n🤖 *Motor IA — Precisión*\n"
                   f"📌 Señales: `{perf['total']}` | Precisión: `{perf['accuracy']}%` | Conf. prom.: `{perf['avg_conf']}%`\n")
        if perf['high_conf_acc']:
            ml_perf += f"💎 Alta confianza: `{perf['high_conf_acc']}%` en `{perf['high_total']}` señales\n"
    await bot.reply_to(message,
        "📊 *ESTADÍSTICAS DEL BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{gp_line}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Últimas fichas:*\n{fichas_txt}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{resumen}\n{ml_perf}\n{trend}",
        parse_mode='Markdown')

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
async def main_async():
    logger.info("🤖 Iniciando CrashBot IA v4.0 — Estrategias Duales (Crash + Maestro)")
    await bot.set_my_commands([
        types.BotCommand('start', '🚀 Iniciar / Ver tendencia'),
        types.BotCommand('estadisticas', '📊 Estadísticas'),
        types.BotCommand('ia', '🤖 Análisis IA (Bayes+Markov)'),
        types.BotCommand('mercado', '📡 Estado del mercado'),
        types.BotCommand('maestro', '🎭 Estrategia Maestro'),
    ])
    asyncio.create_task(http_poller())
    asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas de fondo iniciadas. Iniciando polling Telegram...")
    await bot.infinity_polling(skip_pending=True)

if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info(f"🌐 Flask iniciado en puerto {os.environ.get('PORT', 8080)}")
    asyncio.run(main_async())
