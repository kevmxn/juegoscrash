#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   CRASH BOT — Estrategia Maestro + Filtro Moderado (solo alertas 2.00x)     ║
║   Umbrales de tendencia editables al inicio del código                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import threading
import json
import logging
import os
import random
import time
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
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN PRINCIPAL (edita aquí los umbrales de tendencia)
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN",  "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
API_CRASH  = "https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest"
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1003613599867"))

WIN_TARGET  = 2.00
MAX_MULTS   = 400
TRIM_MULTS  = 200
MAX_COLS    = 3
MAX_ATTS    = 2
CYCLE_SIZE  = 10
BASE_BET    = 0.10

MAESTRO_MIN_CONFIDENCE = 0.55
MAESTRO_HISTORY_SIZE   = 100
MODERATE_MIN_DATA      = 20

# ─── UMBRALES DE TENDENCIA (ajústalos aquí) ─────────────────────────────────
UMBRAL_PCT_ROJO_MAX = 54.0   # Si % de cuotas <2.00 supera esto → tendencia desfavorable
UMBRAL_PCT_VERDE_MIN = 28.0  # Si % de cuotas 2.00-4.99 es menor a esto → tendencia desfavorable
# ─────────────────────────────────────────────────────────────────────────────

POLL_INTERVAL_OK  = 3.0
POLL_MAX_SLEEP    = 60.0
POLL_BACKOFF_BASE = 2.0

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.3 Safari/605.1.15',
]

# ─────────────────────────────────────────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────────────────────────────────────────
g_mults:     list = []
g_seen_ids:  set  = set()

g_signal_state        = 'idle'
g_signal_trigger_mult = 0.0

g_trend_favorable:    Optional[bool] = None
g_signal_msg_ids:     dict           = {}
g_last_trend_msg_id:  Optional[int]  = None

g_maestro_results: list = []
g_maestro_last_prediction: dict = {}

g_poller_status = {
    'total_requests':    0,
    'total_new_rounds':  0,
    'consecutive_errors': 0,
    'last_poll_ts':      0.0,
    'last_round_ts':     0.0,
}

daily_stats = {'date': None, 'wins': 0, 'losses': 0}

bot = AsyncTeleBot(BOT_TOKEN)

# ─────────────────────────────────────────────────────────────────────────────
# MARCADOR DIARIO
# ─────────────────────────────────────────────────────────────────────────────
def get_current_argentina_date() -> str:
    return (datetime.utcnow() - timedelta(hours=3)).strftime("%Y-%m-%d")

def reset_daily_if_needed():
    global daily_stats
    today = get_current_argentina_date()
    if daily_stats['date'] != today:
        daily_stats = {'date': today, 'wins': 0, 'losses': 0}
        logger.info(f"📆 Marcador diario reiniciado para {today}")

def update_daily_stats(win: bool):
    reset_daily_if_needed()
    if win:
        daily_stats['wins'] += 1
    else:
        daily_stats['losses'] += 1
    total = daily_stats['wins'] + daily_stats['losses']
    accuracy = daily_stats['wins'] / total * 100 if total > 0 else 0
    msg = f"📊 MARCADOR DIARIO:\n✅ GANADAS: {daily_stats['wins']}\n❌ PERDIDAS: {daily_stats['losses']}\n\n📈 ACIERTOS = {accuracy:.2f}%"
    asyncio.create_task(broadcast(msg))

# ─────────────────────────────────────────────────────────────────────────────
# ESTRATEGIA MODERADA (filtro exclusivo para alertas 2.00x)
# ─────────────────────────────────────────────────────────────────────────────
class ModerateStrategy:
    @staticmethod
    def compute_positions(values: List[float]) -> List[int]:
        positions = [0]
        for v in values[1:]:
            delta = 1 if v >= WIN_TARGET else -1
            positions.append(positions[-1] + delta)
        return positions

    @staticmethod
    def compute_emas(positions: List[int], periods: List[int]) -> Dict[int, List[float]]:
        emas = {}
        for p in periods:
            if len(positions) < p:
                emas[p] = []
                continue
            k = 2.0 / (p + 1)
            ema_vals = [positions[0]]
            for i in range(1, len(positions)):
                ema_vals.append(ema_vals[-1] + k * (positions[i] - ema_vals[-1]))
            emas[p] = ema_vals
        return emas

    @classmethod
    def check_alerts(cls, values: List[float]) -> Tuple[bool, Optional[float]]:
        if len(values) < MODERATE_MIN_DATA:
            return False, None

        positions = cls.compute_positions(values)
        emas = cls.compute_emas(positions, [4, 8, 20])

        for p in [4, 8, 20]:
            if len(emas.get(p, [])) < 2:
                return False, None

        last_pos      = positions[-1]
        last_ema4     = emas[4][-1]
        last_ema8     = emas[8][-1]
        last_ema20    = emas[20][-1]
        prev_ema8     = emas[8][-2]
        prev_ema20    = emas[20][-2]

        alert_200 = False

        if prev_ema8 <= prev_ema20 and last_ema8 > last_ema20:
            alert_200 = True

        if not alert_200 and len(positions) >= 3:
            a, b, c = positions[-3:]
            if abs(a - c) <= 1 and b > a and last_pos > last_ema4 and last_pos > last_ema8 and last_pos > last_ema20:
                alert_200 = True

        if not alert_200 and len(values) >= 2 and values[-1] >= WIN_TARGET and values[-2] >= WIN_TARGET:
            if last_ema4 > last_ema8 > last_ema20 and (len(values) < 3 or values[-3] < WIN_TARGET):
                alert_200 = True

        if alert_200:
            return True, 2.00
        return False, None


# ─────────────────────────────────────────────────────────────────────────────
# ESTRATEGIA MAESTRO (original)
# ─────────────────────────────────────────────────────────────────────────────
class MaestroStrategy:

    @staticmethod
    def _ema(values: List[float], period: int) -> List[float]:
        if not values:
            return []
        k = 2.0 / (period + 1)
        ema = [values[0]]
        for v in values[1:]:
            ema.append(ema[-1] + k * (v - ema[-1]))
        return ema

    @classmethod
    def get_ema_state(cls, results: List[Dict]) -> Dict[str, Any]:
        if len(results) < 5:
            return {
                'ema3': None, 'ema5': None,
                'prev_ema3': None, 'prev_ema5': None,
                'bullish': None, 'crossover_up': False, 'crossover_down': False,
                'price_above_ema3': None, 'ema_boost': 0.0, 'ema_label': '—',
                'ready': False,
            }
        window = results[:min(20, len(results))]
        vals = [r['value'] for r in reversed(window)]
        ema3_series = cls._ema(vals, 3)
        ema5_series = cls._ema(vals, 5)
        ema3 = ema3_series[-1]
        ema5 = ema5_series[-1]
        prev_ema3 = ema3_series[-2] if len(ema3_series) >= 2 else ema3
        prev_ema5 = ema5_series[-2] if len(ema5_series) >= 2 else ema5
        last_price = vals[-1]
        bullish = ema3 > ema5
        crossover_up = (prev_ema3 <= prev_ema5) and (ema3 > ema5)
        crossover_down = (prev_ema3 >= prev_ema5) and (ema3 < ema5)
        price_above_ema3 = last_price > ema3
        boost = 0.0
        if crossover_up:
            boost = +0.12
        elif crossover_down:
            boost = -0.15
        elif bullish and price_above_ema3:
            boost = +0.07
        elif bullish:
            boost = +0.04
        elif not bullish and not price_above_ema3:
            boost = -0.10
        elif not bullish:
            boost = -0.05
        if crossover_up:
            label = f'🟢 Cruce EMA3↑EMA5 ({ema3:.2f}/{ema5:.2f})'
        elif crossover_down:
            label = f'🔴 Cruce EMA3↓EMA5 ({ema3:.2f}/{ema5:.2f})'
        elif bullish:
            label = f'📗 EMA3 {ema3:.2f} > EMA5 {ema5:.2f}'
        else:
            label = f'📕 EMA3 {ema3:.2f} < EMA5 {ema5:.2f}'
        return {
            'ema3': ema3, 'ema5': ema5,
            'prev_ema3': prev_ema3, 'prev_ema5': prev_ema5,
            'bullish': bullish,
            'crossover_up': crossover_up, 'crossover_down': crossover_down,
            'price_above_ema3': price_above_ema3,
            'ema_boost': boost,
            'ema_label': label,
            'ready': True,
        }

    @classmethod
    def analyze_trend(cls, results: List[Dict]) -> Dict[str, Any]:
        if len(results) < 3:
            return {
                'prediction': 'Cargando datos...',
                'risk': 'wait',
                'detail': 'Esperando resultados',
                'confidence': 0.0,
                'ema': {},
            }
        recent = results[:min(10, len(results))]
        vals = [r['value'] for r in recent]
        avg = sum(vals) / len(vals)
        last3 = vals[:3]
        last3avg = sum(last3) / len(last3)
        green_ratio = sum(1 for v in vals if v >= WIN_TARGET) / len(vals)
        last_is_green = vals[0] >= WIN_TARGET
        second_last = vals[1] if len(vals) > 1 else vals[0]
        streak = 0
        for v in vals:
            if v >= WIN_TARGET:
                break
            streak += 1
        ema = cls.get_ema_state(results)
        base_result = None
        if streak >= 3 and last_is_green:
            base_result = {
                'prediction': '🎯 Zona de entrada detectada',
                'risk': 'low',
                'detail': f'{streak} rojas → verde. Posible racha.',
                'confidence': min(0.85, 0.70 + (streak - 3) * 0.05),
            }
        elif second_last < WIN_TARGET and last_is_green:
            base_result = {
                'prediction': '📍 Posible zona de entrada',
                'risk': 'low',
                'detail': 'Rojo → verde. Reversión detectada.',
                'confidence': 0.70,
            }
        elif last_is_green and last3avg > avg:
            base_result = {
                'prediction': '📈 Tendencia alcista activa',
                'risk': 'low',
                'detail': f'Últimas 3: {last3avg:.2f}x > Media: {avg:.2f}x',
                'confidence': 0.65,
            }
        elif last_is_green and second_last >= WIN_TARGET:
            base_result = {
                'prediction': '⚡ Racha verde — Precaución',
                'risk': 'medium',
                'detail': f'Tasa verdes: {green_ratio*100:.0f}%',
                'confidence': 0.45,
            }
        elif vals[0] >= 5.0:
            base_result = {
                'prediction': '🚀 ¡Multiplicador alto!',
                'risk': 'medium',
                'detail': f'{vals[0]:.2f}x registrado',
                'confidence': 0.45,
            }
        elif streak >= 2:
            risk = 'low' if streak >= 4 else 'medium'
            conf = min(0.75, 0.55 + (streak - 2) * 0.07) if streak >= 4 else 0.50
            base_result = {
                'prediction': f'⏳ Racha roja ({streak})',
                'risk': risk,
                'detail': 'Zona de entrada próxima.' if streak >= 4 else 'Esperando señal.',
                'confidence': conf,
            }
        elif last_is_green:
            base_result = {
                'prediction': '👀 Monitoreando...',
                'risk': 'medium',
                'detail': f'Último: {vals[0]:.2f}x | Avg: {avg:.2f}x',
                'confidence': 0.40,
            }
        else:
            base_result = {
                'prediction': '⌛ Esperando señal...',
                'risk': 'wait',
                'detail': f'Verdes: {green_ratio*100:.0f}% | Avg: {avg:.2f}x',
                'confidence': 0.0,
            }
        if ema['ready']:
            conf_adj = max(0.0, min(0.95, base_result['confidence'] + ema['ema_boost']))
            risk_adj = base_result['risk']
            if ema['crossover_down'] and risk_adj == 'low':
                risk_adj = 'medium'
            if (ema['crossover_up'] and risk_adj == 'medium'
                    and base_result['confidence'] >= 0.50 and conf_adj >= MAESTRO_MIN_CONFIDENCE):
                risk_adj = 'low'
            if not ema['bullish'] and not ema['price_above_ema3'] and risk_adj == 'low':
                conf_adj = max(0.0, conf_adj - 0.05)
            detail_with_ema = f"{base_result['detail']} | {ema['ema_label']}"
            return {
                'prediction': base_result['prediction'],
                'risk': risk_adj,
                'detail': detail_with_ema,
                'confidence': conf_adj,
                'ema': ema,
            }
        base_result['ema'] = ema
        return base_result

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
        support = resistance = None
        for i in range(window, len(smoothed) - window):
            is_min = all(smoothed[i] <= smoothed[j] for j in range(i - window, i + window + 1) if j != i)
            is_max = all(smoothed[i] >= smoothed[j] for j in range(i - window, i + window + 1) if j != i)
            if is_min and (support is None or smoothed[i] > support):
                support = smoothed[i]
            if is_max and (resistance is None or smoothed[i] < resistance):
                resistance = smoothed[i]
        return {'support': support, 'resistance': resistance}

    def should_enter(self, results: List[Dict]) -> Tuple[bool, float, str]:
        if len(results) < 3:
            return False, 0.0, "Datos insuficientes"
        trend = self.analyze_trend(results)
        conf = trend['confidence']
        ema = trend.get('ema', {})
        if ema.get('crossover_down', False):
            logger.info("🚫 Señal bloqueada — cruce EMA bajista activo")
            return False, conf, f"Bloqueado: cruce EMA bajista | {ema.get('ema_label','')}"
        if trend['risk'] != 'low' or conf < MAESTRO_MIN_CONFIDENCE:
            return False, conf, trend['detail']
        values = [r['value'] for r in results[:50]]
        alerta_200, target = ModerateStrategy.check_alerts(values)
        if not alerta_200:
            logger.info("🚫 Señal Maestro bloqueada: sin alerta moderada 2.00x")
            return False, conf, f"{trend['detail']} (sin alerta moderada 2.00x)"
        logger.info(f"✅ Señal Maestro CONFIRMADA por alerta moderada 2.00x")
        return True, conf, trend['detail'] + " | Confirmación moderada 2.00x"


maestro_strategy = MaestroStrategy()

# ─────────────────────────────────────────────────────────────────────────────
# SESIÓN GLOBAL — Gestión 3C × 2I
# ─────────────────────────────────────────────────────────────────────────────
class GlobalSession:
    IDLE = 'idle'
    EVALUATING = 'evaluating'
    WAITING_SIGNAL = 'waiting_signal'
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
            self._cur_ficha[f'c{prev_col}'] = self._cur_ficha.get(f'c{prev_col}', 0.0) + prev_bet
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
                else:
                    self.state = self.IDLE
                    return ('new_col', prev_bet)
            else:
                self.state = self.WAITING_SIGNAL
                return ('wait_signal', prev_bet)

    def status_short(self) -> str:
        total_f = len(self.fichas)
        wins_f = sum(1 for f in self.fichas if f['result'] == 'win')
        pct = wins_f / total_f * 100 if total_f > 0 else 0.0
        return f"📈 Ganadas/Perdidas: `{pct:.2f}%`"


g_session = GlobalSession()

def reset_global_session():
    global g_session
    old_fichas = list(g_session.fichas)
    g_session = GlobalSession(carry_fichas=old_fichas)
    logger.info("🔄 Sesión global reiniciada — fichas preservadas")

# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES (con umbrales configurables)
# ─────────────────────────────────────────────────────────────────────────────
def argentina_time() -> str:
    return (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M")

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
    pct1, pct2, pct3, pct4 = r1/total*100, r2/total*100, r3/total*100, r4/total*100
    # Uso de variables editables
    unfavorable = pct1 > UMBRAL_PCT_ROJO_MAX or pct2 < UMBRAL_PCT_VERDE_MIN
    return {
        'total': total, 'has_enough': total >= 200, 'favorable': not unfavorable,
        'count_100_199': r1, 'count_200_499': r2, 'count_500_999': r3, 'count_1000_plus': r4,
        'pct_100_199': pct1, 'pct_200_499': pct2, 'pct_500_999': pct3, 'pct_1000_plus': pct4,
    }

def quota_stats_text(stats: dict) -> str:
    if stats['total'] == 0:
        return "📡 _Sin datos suficientes para analizar cuotas._\n"
    n_label = "200" if stats['has_enough'] else f"{stats['total']} (acumulando...)"
    r1_flag = " ✅" if stats['pct_100_199'] <= UMBRAL_PCT_ROJO_MAX else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= UMBRAL_PCT_VERDE_MIN else " ❌"
    fav_line = "✅ *¡TENDENCIA FAVORABLE!*\n      _Se recomienda operar_" if stats['favorable'] else "⚠️ *TENDENCIA DESFAVORABLE*\n      _Se recomienda esperar_"
    return (f"📈 *Análisis de la Tendencia últimos*\n"
            f"      *{n_label} multiplicadores*\n"
            f"🔵 Cuotas (1.00-1.99x): `{stats['count_100_199']}` — {stats['pct_100_199']:.2f}%{r1_flag}\n"
            f"🟣 Cuotas (2.00-4.99x): `{stats['count_200_499']}` — {stats['pct_200_499']:.2f}%{r2_flag}\n"
            f"🟡 Cuotas (5.00-9.99x): `{stats['count_500_999']}` — {stats['pct_500_999']:.2f}%\n"
            f"🔴 Cuotas (+10.00x):    `{stats['count_1000_plus']}` — {stats['pct_1000_plus']:.2f}%\n"
            " \n" + fav_line + "\n")

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
    global g_last_trend_msg_id
    hora = argentina_time()
    stats = get_quota_stats(200)
    trend = quota_stats_text(stats)
    header = f"🟢 *TENDENCIA FAVORABLE — {hora}*\n" if favorable else f"🔴 *TENDENCIA DESFAVORABLE — {hora}*\n"
    msg = header + "━━━━━━━━━━━━━━━━━━━━━━━\n" + trend
    if g_last_trend_msg_id is not None:
        try:
            await bot.delete_message(CHANNEL_ID, g_last_trend_msg_id)
        except Exception:
            pass
    result = await broadcast(msg, parse_mode='Markdown')
    if CHANNEL_ID in result:
        g_last_trend_msg_id = result[CHANNEL_ID]

async def _send_signal(trigger: float, reason: str, is_second_opportunity: bool = False):
    global g_signal_msg_ids
    if is_second_opportunity:
        for chat_id, msg_id in list(g_signal_msg_ids.items()):
            try:
                await bot.delete_message(chat_id, msg_id)
            except Exception:
                pass
        g_signal_msg_ids = {}
        title = "💎 Segunda Oportunidad —"
        intento = f"2/{MAX_ATTS}"
    else:
        title = "💎 Señal para"
        intento = f"1/{MAX_ATTS}"
    txt = (f"🚨 Entrar después de: `{trigger:.2f}x`\n"
           f"{title} `{WIN_TARGET:.2f}x`\n"
           f"🇺🇲 Apuesta USD: `${g_session.cur_bet:.2f}`\n"
           f"🆔 Gestión C{g_session.col} — Intento {intento}\n"
           f"🧠 {reason}")
    g_signal_msg_ids = await broadcast(txt, parse_mode='Markdown')

async def _dispatch_result(value: float, tipo: str, bet: float):
    global g_session
    if tipo == 'win':
        await broadcast(f"✅ WIN  GALE #{1 if g_session.attempt==1 else 2} ({value:.2f}x) 🇺🇲 ${BASE_BET:.2f}")
        update_daily_stats(True)
    elif tipo == 'cycle_win':
        await broadcast(f"✅ WIN  GALE #{1 if g_session.attempt==1 else 2} ({value:.2f}x) 🇺🇲 ${BASE_BET:.2f}")
        update_daily_stats(True)
        await broadcast("━━━━━━━━━━━━━━━━━━━━━━━\n🏆 *¡CICLO COMPLETO — 10 señales exitosas!*\n"
                        f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n🔄 _Sesión reiniciada_",
                        parse_mode='Markdown')
        reset_global_session()
        await _check_trend_after_cycle()
    elif tipo == 'new_col':
        r1 = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        lost_col = g_session.col - 1
        col_total = sum(g_session._col_attempt_bets) if g_session._col_attempt_bets else bet
        g_session._col_attempt_bets = []
        g_session.attempt1_result_value = 0.0
        await broadcast(f"❌ LOSS C{lost_col} ({r1} | {value:.2f}x) 🇺🇲 $-{col_total:.2f}")
    elif tipo == 'cycle_loss':
        r1 = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        col_total = sum(g_session._col_attempt_bets) if g_session._col_attempt_bets else bet
        g_session._col_attempt_bets = []
        g_session.attempt1_result_value = 0.0
        await broadcast(f"❌ LOSS C{MAX_COLS} ({r1} | {value:.2f}x) 🇺🇲 $-{col_total:.2f}")
        update_daily_stats(False)
        await broadcast("━━━━━━━━━━━━━━━━━━━━━━━\n⚠️ *CICLO TERMINADO — 3 Columnas Fallidas*\n"
                        f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n🔄 _Sesión reiniciada_",
                        parse_mode='Markdown')
        reset_global_session()
        await _check_trend_after_cycle()
    elif tipo == 'wait_signal':
        logger.info("Esperando nueva señal Maestro para segunda oportunidad")

async def _check_trend_after_cycle():
    stats = get_quota_stats(200)
    if stats['total'] > 0 and not stats['favorable']:
        hora = argentina_time()
        trend = quota_stats_text(stats)
        await broadcast(f"🔴 *TENDENCIA DESFAVORABLE — {hora}*\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{trend}"
                        "━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "⏳ _El bot esperará hasta que la tendencia mejore._\n"
                        "_Se notificará automáticamente cuando sea favorable._",
                        parse_mode='Markdown')

# ─────────────────────────────────────────────────────────────────────────────
# PROCESAMIENTO DE MULTIPLICADORES
# ─────────────────────────────────────────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_trigger_mult, g_mults, g_seen_ids
    global g_trend_favorable, g_session, g_maestro_results

    logger.info(f"🎲 {value:.2f}x | ID: {round_id} | Señal: {g_signal_state} | Sesión: {g_session.state}")

    if g_signal_state == 'evaluating':
        win = value >= WIN_TARGET
        if g_session.state == GlobalSession.EVALUATING:
            tipo, bet = g_session.on_result(win)
            await _dispatch_result(value, tipo, bet)
            if tipo in ('new_col', 'cycle_loss', 'cycle_win', 'win'):
                g_signal_state = 'idle'
        else:
            g_signal_state = 'idle'

    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})
    g_maestro_results.insert(0, {'id': round_id, 'value': value, 'win': value >= WIN_TARGET})
    if len(g_maestro_results) > MAESTRO_HISTORY_SIZE:
        g_maestro_results.pop()
    if len(g_mults) >= MAX_MULTS:
        g_mults[:] = g_mults[-TRIM_MULTS:]
    if len(g_seen_ids) > 2000:
        g_seen_ids.clear()

    stats_trend = get_quota_stats(200)
    if stats_trend['total'] >= 10:
        new_fav = stats_trend['favorable']
        if new_fav != g_trend_favorable:
            g_trend_favorable = new_fav
            asyncio.create_task(broadcast_trend_change(new_fav))

    if g_signal_state == 'idle':
        should_enter, confidence, reason = maestro_strategy.should_enter(g_maestro_results)
        if should_enter:
            if g_session.col == 1 and g_session.state == GlobalSession.IDLE:
                stats_now = get_quota_stats(200)
                if stats_now['total'] > 0 and stats_now['favorable'] is False:
                    logger.info("Señal Maestro bloqueada — tendencia desfavorable")
                    return
            if g_session.state == GlobalSession.IDLE:
                g_signal_state = 'evaluating'
                g_signal_trigger_mult = value
                g_session.state = GlobalSession.EVALUATING
                g_session.signal_trigger_mult = value
                if g_session.col == 1:
                    g_session.start_ficha()
                await _send_signal(value, reason, is_second_opportunity=False)
                logger.info(f"🚀 1ª SEÑAL | {value:.2f}x | conf {confidence:.2%} | {reason}")
            elif g_session.state == GlobalSession.WAITING_SIGNAL and g_session.attempt == 2:
                g_signal_state = 'evaluating'
                g_signal_trigger_mult = value
                g_session.state = GlobalSession.EVALUATING
                g_session.signal_trigger_mult = value
                await _send_signal(value, reason, is_second_opportunity=True)
                logger.info(f"🔄 2ª SEÑAL | {value:.2f}x | apuesta ${g_session.cur_bet:.2f} | {reason}")

# ─────────────────────────────────────────────────────────────────────────────
# POLLER HTTP
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
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, retry_after + random.uniform(1, 5))
                        continue
                    if resp.status >= 500:
                        consecutive_errors += 1
                        backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        sleep_next = backoff
                        continue
                    if resp.status != 200:
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue
                    try:
                        data = await resp.json(content_type=None)
                    except (json.JSONDecodeError, aiohttp.ContentTypeError):
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue
                    api_id = data.get('id')
                    max_mult = data.get('data', {}).get('result', {}).get('maxMultiplier')
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
                    logger.info(f"🎰 NUEVO GIRO #{g_poller_status['total_new_rounds']} | {round_id} | {max_mult:.2f}x")
                    await process_multiplier(float(max_mult), round_id)
            except Exception as e:
                consecutive_errors += 1
                backoff = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                sleep_next = backoff
                logger.exception(f"💥 Error inesperado: {e} → backoff {backoff:.1f}s")
            finally:
                g_poller_status['consecutive_errors'] = consecutive_errors

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD WEB (sencillo)
# ─────────────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

MAESTRO_HTML = """<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Maestro Crash</title></head>
<body><h1>Maestro Crash activo</h1><p>API funcionando</p></body>
</html>"""

@flask_app.route('/')
def home():
    return "Bot activo", 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/api/maestro_data')
def api_maestro_data():
    results = g_maestro_results[:50]
    values = [r['value'] for r in results]
    avg = sum(values) / len(values) if values else 0.0
    max_val = max(values) if values else 0.0
    green_pct = sum(1 for v in values if v >= WIN_TARGET) / len(values) * 100 if values else 0.0
    trend = maestro_strategy.analyze_trend(results)
    sr = maestro_strategy.calculate_support_resistance(results)
    reset_daily_if_needed()
    return jsonify({
        'results': [{'id': r['id'], 'value': r['value'], 'win': r['win']} for r in results],
        'prediction': trend['prediction'],
        'detail': trend['detail'],
        'risk': trend['risk'],
        'confidence': trend['confidence'],
        'support': sr['support'],
        'resistance': sr['resistance'],
        'avg': avg,
        'max': max_val,
        'green_pct': round(green_pct, 1),
        'daily_wins': daily_stats['wins'],
        'daily_losses': daily_stats['losses'],
        'signal_active': g_signal_state == 'evaluating',
        'signal_trigger': g_signal_trigger_mult,
        'signal_col': g_session.col,
        'signal_attempt': g_session.attempt,
        'signal_bet': g_session.cur_bet,
        'max_attempts': MAX_ATTS,
        'total_rounds': g_poller_status['total_new_rounds'],
        'data_count': len(g_mults),
    })

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

async def self_ping_loop():
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not render_url:
        return
    url = f"{render_url.rstrip('/')}/ping"
    while True:
        await asyncio.sleep(14 * 60)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)):
                    pass
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
# HANDLERS TELEGRAM (comandos sin acentos)
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
        "🎭 *Bot de Señales Crash — Estrategia Maestro + Filtro Moderado (solo 2.00x)*\n"
        "📊 Análisis de rachas + EMAs | Detección de zonas de entrada\n"
        f"🎯 Objetivo: `{WIN_TARGET:.2f}x` | Gestión: 3C×2I\n"
        f"💰 Apuesta base: `${BASE_BET:.2f}`\n"
        f"🧠 Confianza mínima: `{MAESTRO_MIN_CONFIDENCE*100:.0f}%` | Señales solo cuando Maestro da 'low' y Moderado detecta 2.00x\n"
        f"📊 Umbrales tendencia: rojo > {UMBRAL_PCT_ROJO_MAX:.1f}% o verde < {UMBRAL_PCT_VERDE_MIN:.1f}% → desfavorable\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📢 *Señales en el canal oficial*\n"
        "🤖 *Comandos:* /senal /estadisticas /tendencia\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n{stats_blk}",
        parse_mode='Markdown')

@bot.message_handler(commands=['senal'])
async def cmd_signal(message):
    if not g_maestro_results:
        await bot.reply_to(message, "📡 *Maestro*: Aún no hay suficientes datos.", parse_mode='Markdown')
        return
    trend = maestro_strategy.analyze_trend(g_maestro_results)
    should, conf, reason = maestro_strategy.should_enter(g_maestro_results)
    sr = maestro_strategy.calculate_support_resistance(g_maestro_results)
    status_text = "✅ SEÑAL ACTIVA (Maestro low + Moderado 2.00x)" if should else "❌ Sin señal ahora"
    support_txt = f"🟢 Soporte: `{sr['support']:.2f}x`" if sr['support'] else "🟢 Soporte: `—`"
    resist_txt = f"🔴 Resistencia: `{sr['resistance']:.2f}x`" if sr['resistance'] else "🔴 Resistencia: `—`"
    await bot.reply_to(message,
        f"🎭 *Estrategia Maestro + Filtro Moderado 2.00x* (objetivo ≥ {WIN_TARGET:.2f}x)\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Predicción: `{trend['prediction']}`\n"
        f"📝 Detalle: {trend['detail']}\n"
        f"🎯 Confianza: `{conf*100:.1f}%`\n"
        f"🚦 Estado: {status_text}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{support_txt}\n{resist_txt}",
        parse_mode='Markdown')

@bot.message_handler(commands=['estadisticas'])
async def cmd_estadisticas(message):
    s = g_session
    stats = get_quota_stats(200)
    trend = quota_stats_text(stats)
    gp_line = s.status_short()
    fichas_rec = s.fichas[-15:]
    if fichas_rec:
        lineas = []
        for f in fichas_rec:
            total = f['c1'] + f['c2'] + f['c3']
            net = BASE_BET if f['result'] == 'win' else -total
            res = "✅" if f['result'] == 'win' else "❌"
            cols = f"C1:${f['c1']:.2f}" + (f" C2:${f['c2']:.2f}" if f['c2'] > 0 else "") + (f" C3:${f['c3']:.2f}" if f['c3'] > 0 else "")
            neto = f"+${net:.2f}" if net >= 0 else f"-${abs(net):.2f}"
            lineas.append(f"{res} #{f['n']} {f.get('ts','--:--')} | {cols} | {neto}")
        fichas_txt = "\n".join(lineas)
        total_f = len(s.fichas)
        wins_f = sum(1 for f in s.fichas if f['result'] == 'win')
        resumen = f"Total fichas: `{total_f}` | ✅ `{wins_f}` | ❌ `{total_f-wins_f}`"
    else:
        fichas_txt = "_Sin fichas registradas aún._"
        resumen = "Total fichas: `0` | ✅ `0` | ❌ `0`"
    await bot.reply_to(message,
        "📊 *ESTADÍSTICAS DEL BOT*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{gp_line}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"*Últimas fichas:*\n{fichas_txt}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{resumen}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{trend}",
        parse_mode='Markdown')

@bot.message_handler(commands=['tendencia'])
async def cmd_tendencia(message):
    stats = get_quota_stats(200)
    await bot.reply_to(message, quota_stats_text(stats), parse_mode='Markdown')

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
async def main_async():
    logger.info(f"🎭 Iniciando CrashBot Maestro + Filtro Moderado (solo 2.00x)")
    logger.info(f"📊 Umbrales de tendencia: rojo > {UMBRAL_PCT_ROJO_MAX}%  |  verde < {UMBRAL_PCT_VERDE_MIN}%")
    reset_daily_if_needed()
    await bot.set_my_commands([
        types.BotCommand('start', '🚀 Iniciar'),
        types.BotCommand('senal', '🎯 Última predicción Maestro'),
        types.BotCommand('estadisticas', '📊 Estadísticas y fichas'),
        types.BotCommand('tendencia', '📈 Tendencia de cuotas'),
    ])
    asyncio.create_task(http_poller())
    asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas iniciadas — polling Telegram...")
    await bot.infinity_polling(skip_pending=True)

if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main_async())
