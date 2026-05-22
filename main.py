#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║   CRASH BOT — Estrategia Maestro Unificada (Galaxsys / Stake Crash)         ║
║   + FILTRO MODERADO (gráfico moderado del HTML) para mejorar señales        ║
║   Lógica analyzeTrend idéntica al HTML · Solo señales risk='low' ≥2.00x     ║
║   Gestión 3C×2I · Marcador diario · Dashboard visual completo               ║
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
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN",  "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
API_CRASH  = "https://api-cs.casino.org/svc-evolution-game-events/api/stakecrash/latest"
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1003613599867"))

WIN_TARGET  = 2.00          # Umbral verde — igual que el HTML (>= 2.0)
MAX_MULTS   = 400
TRIM_MULTS  = 200
MAX_COLS    = 3
MAX_ATTS    = 2
CYCLE_SIZE  = 10
BASE_BET    = 0.10

# ── PARÁMETROS MAESTRO ────────────────────────────────────────────────────────
MAESTRO_MIN_CONFIDENCE = 0.55   # Confianza mínima para disparar señal
MAESTRO_HISTORY_SIZE   = 100

# ── PARÁMETROS MODERADO (filtro adicional) ─────────────────────────────────────
MODERATE_MIN_DATA = 20          # datos mínimos para analizar alerta moderada

# ── POLLING ───────────────────────────────────────────────────────────────────
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

g_signal_state        = 'idle'   # 'idle' | 'evaluating'
g_signal_trigger_mult = 0.0

g_trend_favorable:    Optional[bool] = None
g_signal_msg_ids:     dict           = {}
g_last_trend_msg_id:  Optional[int]  = None

g_maestro_results: list = []        # más nuevo primero, contiene {'id','value','win'}
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
    total    = daily_stats['wins'] + daily_stats['losses']
    accuracy = daily_stats['wins'] / total * 100 if total > 0 else 0
    msg = (
        f"📊 MARCADOR DIARIO:\n"
        f"✅ GANADAS: {daily_stats['wins']}\n"
        f"❌ PERDIDAS: {daily_stats['losses']}\n"
        f"\n📈 ACIERTOS = {accuracy:.2f}%"
    )
    asyncio.create_task(broadcast(msg))

# ─────────────────────────────────────────────────────────────────────────────
# ESTRATEGIA MODERADA (Filtro de confirmación basado en gráfico moderado del HTML)
# ─────────────────────────────────────────────────────────────────────────────
class ModerateStrategy:
    """Replica la lógica del gráfico moderado: detección de alertas 1.50 y 2.00"""

    @staticmethod
    def compute_positions(values: List[float]) -> List[int]:
        """
        Convierte lista de multiplicadores en posiciones acumuladas.
        Regla: si valor >= 2.00 → +1, si < 2.00 → -1.
        Retorna lista del mismo tamaño, partiendo de 0.
        """
        positions = [0]
        for v in values[1:]:
            delta = 1 if v >= WIN_TARGET else -1
            positions.append(positions[-1] + delta)
        return positions

    @staticmethod
    def compute_emas(positions: List[int], periods: List[int]) -> Dict[int, List[float]]:
        """Calcula EMAs para cada periodo sobre la lista de posiciones."""
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
        """
        Detecta si en el último valor hay una alerta moderada activa.
        Retorna (hay_alerta, target) donde target puede ser 1.50 o 2.00.
        """
        if len(values) < MODERATE_MIN_DATA:
            return False, None

        positions = cls.compute_positions(values)
        emas = cls.compute_emas(positions, [4, 8, 20])

        # Asegurar que tenemos todas las EMAs con al menos 2 valores
        for p in [4, 8, 20]:
            if len(emas.get(p, [])) < 2:
                return False, None

        last_pos      = positions[-1]
        last_ema4     = emas[4][-1]
        last_ema8     = emas[8][-1]
        last_ema20    = emas[20][-1]
        prev_ema4     = emas[4][-2]
        prev_ema8     = emas[8][-2]
        prev_ema20    = emas[20][-2]

        # ─── Alerta 1.50 ──────────────────────────────────────────────
        alert_150 = False

        # Condición A: patrón de cambios -1, -1, -1, +1 y precio debajo de EMA4 y EMA8
        if len(values) >= 4:
            cambios = [1 if v >= WIN_TARGET else -1 for v in values[-4:]]
            if cambios == [-1, -1, -1, 1] and last_pos <= last_ema4 and last_pos <= last_ema8:
                alert_150 = True

        # Condición B: cruce EMA4 por encima de EMA8
        if not alert_150 and prev_ema4 <= prev_ema8 and last_ema4 > last_ema8:
            alert_150 = True

        # Condición C: precio cerca de soporte mínimo y por encima de las tres EMAs
        if not alert_150 and len(positions) >= 20:
            soporte = min(positions[-20:])
            if last_pos <= soporte * 1.01 and last_pos > last_ema4 and last_pos > last_ema8 and last_pos > last_ema20:
                alert_150 = True

        # ─── Alerta 2.00 ──────────────────────────────────────────────
        alert_200 = False

        # Condición D: cruce EMA8 por encima de EMA20
        if prev_ema8 <= prev_ema20 and last_ema8 > last_ema20:
            alert_200 = True

        # Condición E: patrón de tres puntos consecutivos (valle) con precio sobre EMAs
        if not alert_200 and len(positions) >= 3:
            a, b, c = positions[-3:]
            if abs(a - c) <= 1 and b > a and last_pos > last_ema4 and last_pos > last_ema8 and last_pos > last_ema20:
                alert_200 = True

        # Condición F: dos verdes consecutivos y EMAs en orden (4>8>20)
        if not alert_200 and len(values) >= 2 and values[-1] >= WIN_TARGET and values[-2] >= WIN_TARGET:
            if last_ema4 > last_ema8 > last_ema20 and (len(values) < 3 or values[-3] < WIN_TARGET):
                alert_200 = True

        # Prioridad: alerta 2.00 tiene prioridad sobre 1.50
        if alert_200:
            return True, 2.00
        if alert_150:
            return True, 1.50
        return False, None


# ─────────────────────────────────────────────────────────────────────────────
# ESTRATEGIA MAESTRO (original) — analyzeTrend() + EMA3/EMA5 reales en decisiones
# ─────────────────────────────────────────────────────────────────────────────
class MaestroStrategy:

    # ── EMA ──────────────────────────────────────────────────────────────────
    @staticmethod
    def _ema(values: List[float], period: int) -> List[float]:
        if not values:
            return []
        k   = 2.0 / (period + 1)
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
        vals   = [r['value'] for r in reversed(window)]

        ema3_series = cls._ema(vals, 3)
        ema5_series = cls._ema(vals, 5)

        ema3      = ema3_series[-1]
        ema5      = ema5_series[-1]
        prev_ema3 = ema3_series[-2] if len(ema3_series) >= 2 else ema3
        prev_ema5 = ema5_series[-2] if len(ema5_series) >= 2 else ema5
        last_price = vals[-1]

        bullish          = ema3 > ema5
        crossover_up     = (prev_ema3 <= prev_ema5) and (ema3 > ema5)
        crossover_down   = (prev_ema3 >= prev_ema5) and (ema3 < ema5)
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

    # ── ANÁLISIS PRINCIPAL ────────────────────────────────────────────────────
    @classmethod
    def analyze_trend(cls, results: List[Dict]) -> Dict[str, Any]:
        if len(results) < 3:
            return {
                'prediction': 'Cargando datos...',
                'risk':       'wait',
                'detail':     'Esperando resultados',
                'confidence': 0.0,
                'ema': {},
            }

        recent      = results[:min(10, len(results))]
        vals        = [r['value'] for r in recent]
        avg         = sum(vals) / len(vals)
        last3       = vals[:3]
        last3avg    = sum(last3) / len(last3)
        green_ratio = sum(1 for v in vals if v >= WIN_TARGET) / len(vals)
        last_is_green = vals[0] >= WIN_TARGET
        second_last   = vals[1] if len(vals) > 1 else vals[0]

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
                'risk':       'low',
                'detail':     f'{streak} rojas → verde. Posible racha.',
                'confidence': min(0.85, 0.70 + (streak - 3) * 0.05),
            }
        elif second_last < WIN_TARGET and last_is_green:
            base_result = {
                'prediction': '📍 Posible zona de entrada',
                'risk':       'low',
                'detail':     'Rojo → verde. Reversión detectada.',
                'confidence': 0.70,
            }
        elif last_is_green and last3avg > avg:
            base_result = {
                'prediction': '📈 Tendencia alcista activa',
                'risk':       'low',
                'detail':     f'Últimas 3: {last3avg:.2f}x > Media: {avg:.2f}x',
                'confidence': 0.65,
            }
        elif last_is_green and second_last >= WIN_TARGET:
            base_result = {
                'prediction': '⚡ Racha verde — Precaución',
                'risk':       'medium',
                'detail':     f'Tasa verdes: {green_ratio*100:.0f}%',
                'confidence': 0.45,
            }
        elif vals[0] >= 5.0:
            base_result = {
                'prediction': '🚀 ¡Multiplicador alto!',
                'risk':       'medium',
                'detail':     f'{vals[0]:.2f}x registrado',
                'confidence': 0.45,
            }
        elif streak >= 2:
            risk = 'low' if streak >= 4 else 'medium'
            conf = min(0.75, 0.55 + (streak - 2) * 0.07) if streak >= 4 else 0.50
            base_result = {
                'prediction': f'⏳ Racha roja ({streak})',
                'risk':       risk,
                'detail':     'Zona de entrada próxima.' if streak >= 4 else 'Esperando señal.',
                'confidence': conf,
            }
        elif last_is_green:
            base_result = {
                'prediction': '👀 Monitoreando...',
                'risk':       'medium',
                'detail':     f'Último: {vals[0]:.2f}x | Avg: {avg:.2f}x',
                'confidence': 0.40,
            }
        else:
            base_result = {
                'prediction': '⌛ Esperando señal...',
                'risk':       'wait',
                'detail':     f'Verdes: {green_ratio*100:.0f}% | Avg: {avg:.2f}x',
                'confidence': 0.0,
            }

        # Aplicar ajuste EMA
        if ema['ready']:
            conf_adj = max(0.0, min(0.95, base_result['confidence'] + ema['ema_boost']))
            risk_adj = base_result['risk']

            if ema['crossover_down'] and risk_adj == 'low':
                risk_adj = 'medium'
                logger.info(f"⚠️ EMA cruce bajista — riesgo degradado a 'medium'")

            if (ema['crossover_up']
                    and risk_adj == 'medium'
                    and base_result['confidence'] >= 0.50
                    and conf_adj >= MAESTRO_MIN_CONFIDENCE):
                risk_adj = 'low'
                logger.info(f"✅ EMA cruce alcista — riesgo promovido a 'low'")

            if not ema['bullish'] and not ema['price_above_ema3'] and risk_adj == 'low':
                conf_adj = max(0.0, conf_adj - 0.05)

            detail_with_ema = f"{base_result['detail']} | {ema['ema_label']}"
            return {
                'prediction': base_result['prediction'],
                'risk':       risk_adj,
                'detail':     detail_with_ema,
                'confidence': conf_adj,
                'ema':        ema,
            }

        base_result['ema'] = ema
        return base_result

    # ── SOPORTE / RESISTENCIA ─────────────────────────────────────────────────
    @staticmethod
    def calculate_support_resistance(results: List[Dict]) -> Dict[str, Optional[float]]:
        values = [r['value'] for r in results]
        if len(values) < 10:
            return {'support': None, 'resistance': None}
        window   = max(3, len(values) // 8)
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

    # ── DECISIÓN DE ENTRADA (con filtro moderado) ──────────────────────────────
    def should_enter(self, results: List[Dict]) -> Tuple[bool, float, str]:
        """
        Señal SÓLO si:
          • risk == 'low'
          • confidence >= MAESTRO_MIN_CONFIDENCE
          • EMA no está en cruce bajista activo (crossover_down bloquea)
          • **ADICIONALMENTE: existe una alerta moderada activa (1.50 o 2.00)**
        """
        if len(results) < 3:
            return False, 0.0, "Datos insuficientes"

        trend = self.analyze_trend(results)
        conf  = trend['confidence']
        ema   = trend.get('ema', {})

        # Bloqueo duro: cruce bajista activo
        if ema.get('crossover_down', False):
            logger.info("🚫 Señal bloqueada — cruce EMA bajista activo")
            return False, conf, f"Bloqueado: cruce EMA bajista | {ema.get('ema_label','')}"

        # Condiciones Maestro
        if trend['risk'] != 'low' or conf < MAESTRO_MIN_CONFIDENCE:
            return False, conf, trend['detail']

        # ─── FILTRO MODERADO (confirmación adicional) ─────────────────────────
        values = [r['value'] for r in results[:50]]  # últimos 50 multiplicadores
        alerta_moderada, target_mod = ModerateStrategy.check_alerts(values)

        if not alerta_moderada:
            logger.info(f"🚫 Señal Maestro bloqueada por filtro moderado: sin alerta activa")
            return False, conf, f"{trend['detail']} (sin alerta moderada)"

        logger.info(f"✅ Señal Maestro CONFIRMADA por alerta moderada {target_mod:.2f}x")
        motivo_extra = f" | Confirmación moderada {target_mod:.2f}x"
        return True, conf, trend['detail'] + motivo_extra


maestro_strategy = MaestroStrategy()

# ─────────────────────────────────────────────────────────────────────────────
# SESIÓN GLOBAL — Gestión 3C × 2I
# ─────────────────────────────────────────────────────────────────────────────
class GlobalSession:
    IDLE           = 'idle'
    EVALUATING     = 'evaluating'
    WAITING_SIGNAL = 'waiting_signal'
    DONE           = 'done'

    def __init__(self, carry_fichas: list = None):
        self.base_bet              = BASE_BET
        self.state                 = self.IDLE
        self.scale                 = 1
        self.col                   = 1
        self.attempt               = 1
        self.lost                  = 0.0
        self.cur_bet               = BASE_BET
        self.entries               = 0
        self.wins                  = 0
        self.losses                = 0
        self.created               = datetime.now()
        self.signal_trigger_mult   = 0.0
        self.attempt1_result_value = 0.0
        self.fichas: list          = carry_fichas if carry_fichas is not None else []
        self._cur_ficha: dict      = None
        self._col_attempt_bets: list = []

    def start_ficha(self):
        self._cur_ficha = {
            'n': len(self.fichas) + 1,
            'c1': 0.0, 'c2': 0.0, 'c3': 0.0,
            'result': None,
            'ts': argentina_time(),
        }

    def on_result(self, win: bool) -> tuple:
        """Retorna (tipo, monto_apostado)."""
        self.entries  += 1
        prev_bet       = self.cur_bet
        prev_col       = self.col
        if self._cur_ficha is not None:
            self._cur_ficha[f'c{prev_col}'] = self._cur_ficha.get(f'c{prev_col}', 0.0) + prev_bet
        self._col_attempt_bets.append(prev_bet)

        if win:
            self.wins        += 1
            self.lost         = 0.0
            self.cur_bet      = self.base_bet
            self.col          = 1
            self.attempt      = 1
            self.scale       += 1
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
            self.losses   += 1
            self.lost     += prev_bet
            self.cur_bet   = self.lost + self.base_bet
            self.attempt  += 1

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
                else:
                    self.state = self.IDLE
                    return ('new_col', prev_bet)
            else:
                self.state = self.WAITING_SIGNAL
                return ('wait_signal', prev_bet)

    def status_short(self) -> str:
        total_f = len(self.fichas)
        wins_f  = sum(1 for f in self.fichas if f['result'] == 'win')
        pct     = wins_f / total_f * 100 if total_f > 0 else 0.0
        return f"📈 Ganadas/Perdidas: `{pct:.2f}%`"


g_session = GlobalSession()

def reset_global_session():
    global g_session
    old_fichas = list(g_session.fichas)
    g_session  = GlobalSession(carry_fichas=old_fichas)
    logger.info("🔄 Sesión global reiniciada — fichas preservadas")

# ─────────────────────────────────────────────────────────────────────────────
# FUNCIONES AUXILIARES
# ─────────────────────────────────────────────────────────────────────────────
def argentina_time() -> str:
    return (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M")

def get_quota_stats(n: int = 200) -> dict:
    data  = g_mults[-n:] if len(g_mults) >= n else g_mults[:]
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
    unfavorable = pct1 > 54.0 or pct2 < 28.0
    return {
        'total': total, 'has_enough': total >= 200, 'favorable': not unfavorable,
        'count_100_199': r1, 'count_200_499': r2, 'count_500_999': r3, 'count_1000_plus': r4,
        'pct_100_199': pct1, 'pct_200_499': pct2, 'pct_500_999': pct3, 'pct_1000_plus': pct4,
    }

def quota_stats_text(stats: dict) -> str:
    if stats['total'] == 0:
        return "📡 _Sin datos suficientes para analizar cuotas._\n"
    n_label = "200" if stats['has_enough'] else f"{stats['total']} (acumulando...)"
    r1_flag = " ✅" if stats['pct_100_199'] <= 54.0 else " ❌"
    r2_flag = " ✅" if stats['pct_200_499'] >= 28.0 else " ❌"
    fav_line = "✅ *¡TENDENCIA FAVORABLE!*\n      _Se recomienda operar_" if stats['favorable'] \
               else "⚠️ *TENDENCIA DESFAVORABLE*\n      _Se recomienda esperar_"
    return (
        f"📈 *Análisis de la Tendencia últimos*\n"
        f"      *{n_label} multiplicadores*\n"
        f"🔵 Cuotas (1.00-1.99x): `{stats['count_100_199']}` — {stats['pct_100_199']:.2f}%{r1_flag}\n"
        f"🟣 Cuotas (2.00-4.99x): `{stats['count_200_499']}` — {stats['pct_200_499']:.2f}%{r2_flag}\n"
        f"🟡 Cuotas (5.00-9.99x): `{stats['count_500_999']}` — {stats['pct_500_999']:.2f}%\n"
        f"🔴 Cuotas (+10.00x):    `{stats['count_1000_plus']}` — {stats['pct_1000_plus']:.2f}%\n"
        " \n" + fav_line + "\n"
    )

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
    hora  = argentina_time()
    stats = get_quota_stats(200)
    trend = quota_stats_text(stats)
    header = f"🟢 *TENDENCIA FAVORABLE — {hora}*\n" if favorable else f"🔴 *TENDENCIA DESFAVORABLE — {hora}*\n"
    msg    = header + "━━━━━━━━━━━━━━━━━━━━━━━\n" + trend

    if g_last_trend_msg_id is not None:
        try:
            await bot.delete_message(CHANNEL_ID, g_last_trend_msg_id)
            logger.info(f"🗑️ Mensaje anterior de tendencia eliminado (ID: {g_last_trend_msg_id})")
        except Exception as e:
            logger.warning(f"No se pudo eliminar mensaje de tendencia: {e}")
        g_last_trend_msg_id = None

    result = await broadcast(msg, parse_mode='Markdown')
    if CHANNEL_ID in result:
        g_last_trend_msg_id = result[CHANNEL_ID]
        logger.info(f"📢 Nuevo mensaje de tendencia enviado (ID: {g_last_trend_msg_id})")

async def _send_signal(trigger: float, reason: str, is_second_opportunity: bool = False):
    global g_signal_msg_ids
    if is_second_opportunity:
        for chat_id, msg_id in list(g_signal_msg_ids.items()):
            try:
                await bot.delete_message(chat_id, msg_id)
            except Exception:
                pass
        g_signal_msg_ids = {}
        title   = "💎 Segunda Oportunidad —"
        intento = f"2/{MAX_ATTS}"
    else:
        title   = "💎 Señal para"
        intento = f"1/{MAX_ATTS}"

    txt = (
        f"🚨 Entrar después de: `{trigger:.2f}x`\n"
        f"{title} `{WIN_TARGET:.2f}x`\n"
        f"🇺🇲 Apuesta USD: `${g_session.cur_bet:.2f}`\n"
        f"🆔 Gestión C{g_session.col} — Intento {intento}\n"
        f"🧠 {reason}"
    )
    g_signal_msg_ids = await broadcast(txt, parse_mode='Markdown')

async def _dispatch_result(value: float, tipo: str, bet: float):
    global g_session
    if tipo == 'win':
        await broadcast(f"✅ WIN  GALE #{1 if g_session.attempt==1 else 2} ({value:.2f}x) 🇺🇲 ${BASE_BET:.2f}")
        update_daily_stats(win=True)
    elif tipo == 'cycle_win':
        await broadcast(f"✅ WIN  GALE #{1 if g_session.attempt==1 else 2} ({value:.2f}x) 🇺🇲 ${BASE_BET:.2f}")
        update_daily_stats(win=True)
        await broadcast(
            "━━━━━━━━━━━━━━━━━━━━━━━\n🏆 *¡CICLO COMPLETO — 10 señales exitosas!*\n"
            f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n🔄 _Sesión reiniciada_",
            parse_mode='Markdown')
        reset_global_session()
        await _check_trend_after_cycle()
    elif tipo == 'new_col':
        r1       = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        lost_col = g_session.col - 1
        col_total = sum(g_session._col_attempt_bets) if g_session._col_attempt_bets else bet
        g_session._col_attempt_bets        = []
        g_session.attempt1_result_value    = 0.0
        await broadcast(f"❌ LOSS C{lost_col} ({r1} | {value:.2f}x) 🇺🇲 $-{col_total:.2f}")
    elif tipo == 'cycle_loss':
        r1        = f"{g_session.attempt1_result_value:.2f}x" if g_session.attempt1_result_value else "—"
        col_total = sum(g_session._col_attempt_bets) if g_session._col_attempt_bets else bet
        g_session._col_attempt_bets        = []
        g_session.attempt1_result_value    = 0.0
        await broadcast(f"❌ LOSS C{MAX_COLS} ({r1} | {value:.2f}x) 🇺🇲 $-{col_total:.2f}")
        update_daily_stats(win=False)
        await broadcast(
            "━━━━━━━━━━━━━━━━━━━━━━━\n⚠️ *CICLO TERMINADO — 3 Columnas Fallidas*\n"
            f"📊 G/P: `{g_session.wins}/{g_session.losses}`\n🔄 _Sesión reiniciada_",
            parse_mode='Markdown')
        reset_global_session()
        await _check_trend_after_cycle()
    elif tipo == 'wait_signal':
        logger.info("Esperando nueva señal Maestro para segunda oportunidad")

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
            parse_mode='Markdown')

# ─────────────────────────────────────────────────────────────────────────────
# PROCESAMIENTO DE MULTIPLICADORES — NÚCLEO
# ─────────────────────────────────────────────────────────────────────────────
async def process_multiplier(value: float, round_id: str):
    global g_signal_state, g_signal_trigger_mult, g_mults, g_seen_ids
    global g_trend_favorable, g_session, g_maestro_results

    logger.info(f"🎲 {value:.2f}x | ID: {round_id} | Señal: {g_signal_state} | Sesión: {g_session.state}")

    # ── 1. Resultado de señal activa ──────────────────────────────────────────
    if g_signal_state == 'evaluating':
        win = value >= WIN_TARGET
        if g_session.state == GlobalSession.EVALUATING:
            tipo, bet = g_session.on_result(win)
            await _dispatch_result(value, tipo, bet)
            if tipo == 'wait_signal':
                g_signal_state = 'idle'
            elif tipo in ('new_col', 'cycle_loss', 'cycle_win', 'win'):
                g_signal_state = 'idle'
        else:
            g_signal_state = 'idle'

    # ── 2. Actualizar datos generales ─────────────────────────────────────────
    g_mults.append({'id': round_id, 'value': value, 'ts': time.time()})
    g_maestro_results.insert(0, {'id': round_id, 'value': value, 'win': value >= WIN_TARGET})
    if len(g_maestro_results) > MAESTRO_HISTORY_SIZE:
        g_maestro_results.pop()
    if len(g_mults) >= MAX_MULTS:
        g_mults[:] = g_mults[-TRIM_MULTS:]
        logger.info(f"✂️ Datos recortados a {TRIM_MULTS} registros")
    if len(g_seen_ids) > 2000:
        g_seen_ids.clear()

    # ── 3. Cambio de tendencia global ─────────────────────────────────────────
    stats_trend = get_quota_stats(200)
    if stats_trend['total'] >= 10:
        new_fav = stats_trend['favorable']
        if new_fav != g_trend_favorable:
            g_trend_favorable = new_fav
            asyncio.create_task(broadcast_trend_change(new_fav))

    # ── 4. Detectar nueva señal Maestro (con filtro moderado) ─────────────────
    if g_signal_state == 'idle':
        should_enter, confidence, reason = maestro_strategy.should_enter(g_maestro_results)
        if should_enter:
            # Bloqueo por tendencia desfavorable (solo columna 1, sesión nueva)
            if g_session.col == 1 and g_session.state == GlobalSession.IDLE:
                stats_now = get_quota_stats(200)
                if stats_now['total'] > 0 and stats_now['favorable'] is False:
                    logger.info("Señal Maestro bloqueada — tendencia desfavorable")
                    return

            # CASO 1: Primera oportunidad
            if g_session.state == GlobalSession.IDLE:
                g_signal_state               = 'evaluating'
                g_signal_trigger_mult        = value
                g_session.state              = GlobalSession.EVALUATING
                g_session.signal_trigger_mult = value
                if g_session.col == 1:
                    g_session.start_ficha()
                await _send_signal(value, reason, is_second_opportunity=False)
                logger.info(f"🚀 1ª SEÑAL | {value:.2f}x | conf {confidence:.2%} | {reason}")

            # CASO 2: Segunda oportunidad (esperando nueva señal Maestro)
            elif g_session.state == GlobalSession.WAITING_SIGNAL and g_session.attempt == 2:
                g_signal_state               = 'evaluating'
                g_signal_trigger_mult        = value
                g_session.state              = GlobalSession.EVALUATING
                g_session.signal_trigger_mult = value
                await _send_signal(value, reason, is_second_opportunity=True)
                logger.info(f"🔄 2ª SEÑAL | {value:.2f}x | apuesta ${g_session.cur_bet:.2f} | {reason}")

# ─────────────────────────────────────────────────────────────────────────────
# POLLER HTTPS
# ─────────────────────────────────────────────────────────────────────────────
async def http_poller():
    consecutive_errors = 0
    sleep_next         = POLL_INTERVAL_OK
    logger.info(f"📡 Iniciando poller HTTP → {API_CRASH}")
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(sleep_next)
            try:
                ua      = random.choice(USER_AGENTS)
                headers = {'User-Agent': ua, 'Accept': 'application/json', 'Cache-Control': 'no-cache'}
                g_poller_status['total_requests']  += 1
                g_poller_status['last_poll_ts']     = time.time()

                async with session.get(
                    API_CRASH, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10), ssl=True
                ) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 30))
                        logger.warning(f"⚠️ Rate limited (429) → esperando {retry_after}s")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, retry_after + random.uniform(1, 5))
                        continue
                    if resp.status >= 500:
                        consecutive_errors += 1
                        backoff    = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        sleep_next = backoff
                        logger.error(f"❌ Error servidor {resp.status} → backoff {backoff:.1f}s")
                        continue
                    if resp.status != 200:
                        logger.warning(f"⚠️ Código inesperado: {resp.status}")
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue

                    try:
                        data = await resp.json(content_type=None)
                    except (json.JSONDecodeError, aiohttp.ContentTypeError):
                        consecutive_errors += 1
                        sleep_next = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                        continue

                    api_id   = data.get('id')
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
                    g_poller_status['last_round_ts']     = time.time()
                    consecutive_errors = 0
                    sleep_next = POLL_INTERVAL_OK + random.uniform(0.3, 1.0)
                    logger.info(f"🎰 NUEVO GIRO #{g_poller_status['total_new_rounds']} | {round_id} | {max_mult:.2f}x")
                    await process_multiplier(float(max_mult), round_id)

            except Exception as e:
                consecutive_errors += 1
                backoff    = min(POLL_MAX_SLEEP, POLL_BACKOFF_BASE ** consecutive_errors)
                sleep_next = backoff
                logger.exception(f"💥 Error inesperado: {e} → backoff {backoff:.1f}s")
            finally:
                g_poller_status['consecutive_errors'] = consecutive_errors

# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD WEB — Maestro.html unificado (conectado al backend por polling)
# ─────────────────────────────────────────────────────────────────────────────
flask_app = Flask(__name__)

MAESTRO_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Maestro Crash - Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: 'Segoe UI', Roboto, sans-serif;
  min-height: 100vh; background: #0a0e17; color: #fff; overflow-x: hidden;
}
body::before {
  content: ''; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
  background:
    radial-gradient(ellipse at 20% 50%, rgba(168,85,247,0.12), transparent 50%),
    radial-gradient(ellipse at 80% 20%, rgba(59,130,246,0.10), transparent 50%),
    radial-gradient(ellipse at 50% 80%, rgba(16,185,129,0.08), transparent 50%);
  animation: bgShift 12s ease-in-out infinite alternate; z-index: -2;
}
body::after {
  content: ''; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
  background-image:
    radial-gradient(1px 1px at 10% 20%, rgba(255,255,255,0.4), transparent),
    radial-gradient(1px 1px at 30% 60%, rgba(255,255,255,0.3), transparent),
    radial-gradient(1px 1px at 50% 10%, rgba(255,255,255,0.35), transparent),
    radial-gradient(1px 1px at 70% 80%, rgba(255,255,255,0.25), transparent),
    radial-gradient(1px 1px at 90% 40%, rgba(255,255,255,0.3), transparent);
  background-size: 250px 250px; animation: stars 6s linear infinite; opacity: 0.3; z-index: -1;
}
@keyframes stars { from{transform:translateY(0)} to{transform:translateY(-250px)} }
@keyframes bgShift { 0%{filter:hue-rotate(0deg)} 100%{filter:hue-rotate(20deg)} }

.container { max-width: 640px; margin: 0 auto; padding: 16px; }
.header { text-align: center; padding: 16px 0 10px; }
.header h1 {
  font-size: 22px; font-weight: 700;
  background: linear-gradient(135deg,#a855f7,#3b82f6,#10b981);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
  margin-bottom: 4px;
}
.badge {
  display: inline-block; background: linear-gradient(135deg,#a855f7,#6366f1);
  color:#fff; font-size:10px; font-weight:700; padding:2px 8px;
  border-radius:10px; letter-spacing:1px; vertical-align:middle; margin-left:6px;
}
.subtitle { font-size: 12px; color: rgba(255,255,255,0.5); margin-top: 4px; }

.status-bar {
  display:flex; justify-content:space-between; align-items:center;
  padding:10px 14px; background:rgba(255,255,255,0.04);
  border-radius:10px; border:1px solid rgba(255,255,255,0.06); margin-bottom:12px;
}
.status-indicator { display:flex; align-items:center; gap:6px; }
.status-dot {
  width:7px; height:7px; border-radius:50%; background:#ef4444; transition:background 0.3s;
}
.status-dot.connected { background:#22c55e; box-shadow:0 0 6px #22c55e; animation:pulseG 2s infinite; }
.status-dot.waiting   { background:#f59e0b; animation:pulseY 1s infinite; }
@keyframes pulseG { 0%,100%{opacity:1}50%{opacity:.5} }
@keyframes pulseY { 0%,100%{opacity:1}50%{opacity:.3} }
.status-text { font-size:11px; color:rgba(255,255,255,0.6); }
#clock { font-family:'Courier New',monospace; font-size:13px; color:rgba(255,255,255,0.6); }

/* Señal activa */
.signal-banner {
  display:none; padding:12px 14px; border-radius:10px; margin-bottom:12px;
  background:rgba(168,85,247,0.12); border:1px solid rgba(168,85,247,0.4);
  animation:signalPulse 1.5s ease-in-out infinite;
}
.signal-banner.show { display:block; }
@keyframes signalPulse { 0%,100%{box-shadow:0 0 0 rgba(168,85,247,0)} 50%{box-shadow:0 0 18px rgba(168,85,247,0.4)} }
.signal-title { font-size:13px; font-weight:700; color:#c084fc; margin-bottom:4px; }
.signal-detail { font-size:11px; color:rgba(255,255,255,0.6); }

/* Prediction Card */
.prediction-card {
  background:rgba(255,255,255,0.03); border-radius:12px; padding:14px;
  margin-bottom:14px; border:1px solid rgba(255,255,255,0.06);
  transition:all 0.4s ease; position:relative; overflow:hidden;
}
.prediction-card::before {
  content:''; position:absolute; top:0; left:0; width:100%; height:3px;
  background:rgba(255,255,255,0.1); transition:background 0.4s;
}
.prediction-card[data-risk="low"]    { border-color:rgba(16,185,129,0.3);  box-shadow:0 0 20px rgba(16,185,129,0.08); }
.prediction-card[data-risk="low"]::before { background:linear-gradient(90deg,#10b981,#34d399); }
.prediction-card[data-risk="medium"] { border-color:rgba(245,158,11,0.3);  box-shadow:0 0 20px rgba(245,158,11,0.08); }
.prediction-card[data-risk="medium"]::before { background:linear-gradient(90deg,#f59e0b,#fbbf24); }
.prediction-card[data-risk="high"]   { border-color:rgba(239,68,68,0.3);   box-shadow:0 0 20px rgba(239,68,68,0.08); }
.prediction-card[data-risk="high"]::before { background:linear-gradient(90deg,#ef4444,#f87171); }
.prediction-card[data-risk="wait"]   { border-color:rgba(100,116,139,0.2); }
.prediction-card[data-risk="wait"]::before { background:rgba(100,116,139,0.4); }
.prediction-label  { font-size:10px; color:rgba(255,255,255,0.4); text-transform:uppercase; letter-spacing:1.5px; margin-bottom:6px; }
.prediction-text   { font-size:15px; font-weight:600; color:#fff; line-height:1.4; }
.prediction-detail { font-size:11px; color:rgba(255,255,255,0.45); margin-top:5px; }
.confidence-bar    { margin-top:8px; background:rgba(255,255,255,0.07); border-radius:4px; height:4px; overflow:hidden; }
.confidence-fill   { height:100%; background:linear-gradient(90deg,#a855f7,#10b981); border-radius:4px; transition:width 0.5s; }

/* Chart tabs */
.chart-tabs { display:flex; gap:6px; margin-bottom:10px; }
.chart-tab {
  flex:1; padding:8px; background:rgba(255,255,255,0.03);
  border:1px solid rgba(255,255,255,0.06); border-radius:8px;
  color:rgba(255,255,255,0.5); font-size:11px; font-weight:600;
  text-align:center; cursor:pointer; transition:all 0.2s;
}
.chart-tab.active { background:rgba(168,85,247,0.12); border-color:rgba(168,85,247,0.3); color:#c084fc; }

/* Chart */
.chart-container {
  position:relative; width:100%; height:260px;
  background:rgba(255,255,255,0.02); border-radius:12px;
  border:1px solid rgba(255,255,255,0.05); margin-bottom:14px; overflow:hidden;
}
.chart-container canvas { position:absolute; top:0; left:0; width:100%!important; height:100%!important; }
#lineChartCanvas { opacity:0; transition:opacity 0.4s; pointer-events:none; }
#lineChartCanvas.active { opacity:1; pointer-events:auto; }

/* Results */
.results-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; }
.results-header h3 { font-size:12px; color:rgba(255,255,255,0.5); font-weight:500; }
.round-info { font-size:11px; color:rgba(255,255,255,0.3); font-family:monospace; }
.results-container {
  display:flex; gap:5px; overflow-x:auto; padding:8px 0;
  scrollbar-width:thin; scrollbar-color:rgba(255,255,255,0.1) transparent;
}
.results-container::-webkit-scrollbar { height:3px; }
.results-container::-webkit-scrollbar-thumb { background:rgba(255,255,255,0.1); border-radius:3px; }
.result-pill {
  flex-shrink:0; padding:6px 10px; border-radius:6px; font-size:13px; font-weight:600;
  background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.06);
  font-family:'Courier New',monospace; min-width:52px; text-align:center; transition:all 0.3s;
}
.result-pill.latest { animation:latestPulse 1.5s infinite; font-weight:700; border-width:1.5px; }
.result-pill.latest.positive { background:rgba(16,185,129,0.12); border-color:rgba(16,185,129,0.4); color:#34d399; }
.result-pill.latest.negative { background:rgba(239,68,68,0.12);  border-color:rgba(239,68,68,0.4);  color:#f87171; }
.result-pill.latest.high     { background:rgba(245,158,11,0.15); border-color:rgba(245,158,11,0.5); color:#fbbf24; }
.result-pill.cat-low  { color:#f87171; }
.result-pill.cat-mid  { color:#60a5fa; }
.result-pill.cat-good { color:#34d399; }
.result-pill.cat-high { color:#f472b6; }
.result-pill.cat-vhigh{ color:#c084fc; }
@keyframes latestPulse { 0%,100%{filter:brightness(1)} 50%{filter:brightness(1.2)} }

/* Levels */
.levels-row { display:flex; justify-content:space-around; padding:10px 0; margin-top:8px; }
.level-item { text-align:center; }
.level-label { font-size:9px; text-transform:uppercase; letter-spacing:1px; margin-bottom:2px; }
.level-label.support { color:#22c55e; } .level-label.resistance { color:#ef4444; }
.level-value { font-size:14px; font-weight:700; font-family:'Courier New',monospace; }
.level-value.support { color:#4ade80; } .level-value.resistance { color:#f87171; }

/* Stats */
.stats-grid {
  display:grid; grid-template-columns:repeat(3,1fr); gap:8px;
  margin-top:14px; padding-top:12px; border-top:1px solid rgba(255,255,255,0.05);
}
.stat-box { text-align:center; padding:10px 6px; background:rgba(255,255,255,0.02); border-radius:8px; border:1px solid rgba(255,255,255,0.04); }
.stat-label { font-size:9px; color:rgba(255,255,255,0.35); text-transform:uppercase; letter-spacing:0.8px; margin-bottom:4px; }
.stat-value { font-size:15px; font-weight:700; font-family:'Courier New',monospace; }
.stat-value.green { color:#34d399; } .stat-value.red { color:#f87171; }
.stat-value.blue  { color:#60a5fa; } .stat-value.gold { color:#fbbf24; }

/* Marcador */
.marcador {
  display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-top:14px;
  padding:12px; background:rgba(255,255,255,0.02); border-radius:10px;
  border:1px solid rgba(255,255,255,0.06);
}
.marc-item { text-align:center; }
.marc-label { font-size:9px; color:rgba(255,255,255,0.35); text-transform:uppercase; letter-spacing:0.8px; }
.marc-value { font-size:18px; font-weight:700; font-family:'Courier New',monospace; margin-top:2px; }

.footer { text-align:center; padding:18px 0 10px; font-size:10px; color:rgba(255,255,255,0.2); }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>Maestro Crash <span class="badge">LIVE</span></h1>
    <div class="subtitle">Stake Crash · Bot Automático</div>
  </div>

  <div class="status-bar">
    <div class="status-indicator">
      <div class="status-dot waiting" id="status-dot"></div>
      <span class="status-text" id="status-text">CONECTANDO...</span>
    </div>
    <div id="clock"></div>
  </div>

  <!-- Señal activa -->
  <div class="signal-banner" id="signalBanner">
    <div class="signal-title" id="signalTitle">💎 Señal activa</div>
    <div class="signal-detail" id="signalDetail">—</div>
  </div>

  <!-- Prediction -->
  <div class="prediction-card" id="prediction" data-risk="wait">
    <div class="prediction-label">Análisis Maestro</div>
    <div class="prediction-text" id="predText">Cargando datos...</div>
    <div class="prediction-detail" id="predDetail">Esperando resultados del juego</div>
    <div class="confidence-bar"><div class="confidence-fill" id="confFill" style="width:0%"></div></div>
  </div>

  <!-- Chart tabs -->
  <div class="chart-tabs">
    <button class="chart-tab active" data-type="line" id="tabLine">Tendencia</button>
    <button class="chart-tab" data-type="bar" id="tabBar">Barras</button>
  </div>

  <!-- Charts -->
  <div class="chart-container">
    <canvas id="lineChartCanvas" class="active"></canvas>
    <canvas id="barChartCanvas"></canvas>
  </div>

  <!-- Results -->
  <div class="results-header">
    <h3>Últimos Multiplicadores</h3>
    <span class="round-info" id="roundInfo">Ronda #0</span>
  </div>
  <div class="results-container" id="results"></div>

  <!-- Levels -->
  <div class="levels-row">
    <div class="level-item">
      <div class="level-label support">Soporte</div>
      <div class="level-value support" id="support-val">—</div>
    </div>
    <div class="level-item">
      <div class="level-label resistance">Resistencia</div>
      <div class="level-value resistance" id="resistance-val">—</div>
    </div>
  </div>

  <!-- Stats -->
  <div class="stats-grid">
    <div class="stat-box">
      <div class="stat-label">Promedio</div>
      <div class="stat-value blue" id="stat-avg">0.00x</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">Más Alto</div>
      <div class="stat-value gold" id="stat-high">0.00x</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">Verdes %</div>
      <div class="stat-value green" id="stat-green">0%</div>
    </div>
  </div>

  <!-- Marcador diario -->
  <div class="marcador">
    <div class="marc-item">
      <div class="marc-label">✅ Ganadas</div>
      <div class="marc-value" style="color:#34d399" id="marc-wins">0</div>
    </div>
    <div class="marc-item">
      <div class="marc-label">❌ Perdidas</div>
      <div class="marc-value" style="color:#f87171" id="marc-losses">0</div>
    </div>
  </div>

  <div class="footer">Maestro Crash Bot &bull; Stake &bull; Auto Mode</div>
</div>

<script>
// ─── CONFIG ────────────────────────────────────────────────────────────────
const WIN_TARGET = 2.00;
const POLL_MS    = 3000;

// ─── STATE ─────────────────────────────────────────────────────────────────
let localResults    = [];   // [{id, value, win}] más nuevo primero
let lastKnownId     = null;
let roundCount      = 0;
let highestMult     = 0;
let cumulativeScore = 0;

let lineChart, barChart;

// ─── CLOCK ─────────────────────────────────────────────────────────────────
function updateClock() {
  const now = new Date();
  document.getElementById('clock').textContent =
    String(now.getHours()).padStart(2,'0') + ':' +
    String(now.getMinutes()).padStart(2,'0') + ':' +
    String(now.getSeconds()).padStart(2,'0');
}
setInterval(updateClock, 1000);
updateClock();

// ─── STATUS ────────────────────────────────────────────────────────────────
function setStatus(s) {
  const dot  = document.getElementById('status-dot');
  const text = document.getElementById('status-text');
  dot.className  = 'status-dot ' + s;
  text.textContent = s === 'connected' ? 'EN VIVO' : s === 'waiting' ? 'ESPERANDO...' : 'DESCONECTADO';
}

// ─── HELPERS ────────────────────────────────────────────────────────────────
function getCategory(v) {
  if (v < 1.50) return 'low';
  if (v < 2.00) return 'mid';
  if (v < 3.00) return 'good';
  if (v < 10.0) return 'high';
  return 'vhigh';
}

// ─── CHARTS ─────────────────────────────────────────────────────────────────
function initCharts() {
  const lineCtx = document.getElementById('lineChartCanvas').getContext('2d');
  lineChart = new Chart(lineCtx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Score acumulado',
        data: [],
        borderColor: '#a855f7',
        borderWidth: 2,
        fill: { target: 'origin', above: 'rgba(16,185,129,0.06)', below: 'rgba(239,68,68,0.06)' },
        pointRadius: 0,
        tension: 0.2,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { display: false },
        y: { grid: { color: 'rgba(255,255,255,0.04)' }, ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 9 } } }
      }
    }
  });

  const barCtx = document.getElementById('barChartCanvas').getContext('2d');
  barChart = new Chart(barCtx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Multiplicador',
        data: [],
        backgroundColor: [],
        borderRadius: 3,
        borderSkipped: false,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { display: false },
        y: {
          grid: { color: 'rgba(255,255,255,0.04)' },
          ticks: { color: 'rgba(255,255,255,0.3)', font: { size: 9 } },
          min: 0
        }
      }
    }
  });
}

function updateCharts() {
  const pts    = localResults.slice(0, 30).reverse();
  const labels = pts.map((_, i) => i);
  const vals   = pts.map(r => parseFloat(r.value));
  const colors = vals.map(v => v >= WIN_TARGET ? 'rgba(16,185,129,0.6)' : 'rgba(239,68,68,0.5)');

  // Line chart (cumulative score)
  let cum = 0, cumData = [];
  for (const v of vals) { cum += v >= WIN_TARGET ? 1 : -1; cumData.push(cum); }
  lineChart.data.labels                        = labels;
  lineChart.data.datasets[0].data             = cumData;
  lineChart.update('none');

  // Bar chart
  barChart.data.labels                             = labels;
  barChart.data.datasets[0].data                  = vals;
  barChart.data.datasets[0].backgroundColor       = colors;
  barChart.update('none');
}

// ─── RESULTS DISPLAY ────────────────────────────────────────────────────────
function updateResultsDisplay() {
  const container = document.getElementById('results');
  container.innerHTML = '';
  localResults.forEach((item, i) => {
    const pill  = document.createElement('div');
    const v     = parseFloat(item.value);
    pill.className = `result-pill cat-${getCategory(v)}`;
    if (i === 0) {
      pill.classList.add('latest');
      if (v >= 5)       pill.classList.add('high');
      else if (v >= 2)  pill.classList.add('positive');
      else              pill.classList.add('negative');
    }
    pill.textContent = v.toFixed(2) + 'x';
    container.appendChild(pill);
  });
  document.getElementById('roundInfo').textContent = `Ronda #${roundCount}`;
}

// ─── PREDICTION UI ──────────────────────────────────────────────────────────
function updatePredictionUI(data) {
  const card = document.getElementById('prediction');
  card.setAttribute('data-risk', data.risk || 'wait');
  document.getElementById('predText').textContent   = data.prediction   || '—';
  document.getElementById('predDetail').textContent = data.detail       || '—';
  const conf = Math.round((data.confidence || 0) * 100);
  document.getElementById('confFill').style.width   = conf + '%';

  // Support / Resistance
  document.getElementById('support-val').textContent    = data.support    != null ? data.support.toFixed(2)    + 'x' : '—';
  document.getElementById('resistance-val').textContent = data.resistance != null ? data.resistance.toFixed(2) + 'x' : '—';
}

// ─── STATS UI ───────────────────────────────────────────────────────────────
function updateStatsUI(data) {
  document.getElementById('stat-avg').textContent   = (data.avg   || 0).toFixed(2) + 'x';
  document.getElementById('stat-high').textContent  = (data.max   || 0).toFixed(2) + 'x';
  const pct = data.green_pct || 0;
  const el  = document.getElementById('stat-green');
  el.textContent = pct.toFixed(0) + '%';
  el.className   = 'stat-value ' + (pct >= 50 ? 'green' : 'red');

  document.getElementById('marc-wins').textContent   = data.daily_wins   || 0;
  document.getElementById('marc-losses').textContent = data.daily_losses || 0;
}

// ─── SIGNAL BANNER ──────────────────────────────────────────────────────────
function updateSignalBanner(data) {
  const banner = document.getElementById('signalBanner');
  if (data.signal_active) {
    banner.classList.add('show');
    document.getElementById('signalTitle').textContent  = `💎 Señal activa — C${data.signal_col || 1} Int.${data.signal_attempt || 1}/${data.max_attempts || 2}`;
    document.getElementById('signalDetail').textContent = `Trigger: ${(data.signal_trigger || 0).toFixed(2)}x | Apuesta: $${(data.signal_bet || 0).toFixed(2)}`;
  } else {
    banner.classList.remove('show');
  }
}

// ─── MAIN POLL LOOP ─────────────────────────────────────────────────────────
async function poll() {
  try {
    const res  = await fetch('/api/maestro_data');
    const data = await res.json();

    if (data.results && data.results.length > 0) {
      const apiResults = data.results;
      const latestId   = apiResults[0].id;

      if (latestId !== lastKnownId) {
        // Detectar cuántos resultados nuevos hay
        const prevIdx = lastKnownId != null
          ? apiResults.findIndex(r => r.id === lastKnownId)
          : -1;
        const newCount = prevIdx === -1 ? apiResults.length : prevIdx;

        // Actualizar lista local (completa desde API)
        localResults = apiResults;
        roundCount  += newCount;
        if (apiResults[0].value > highestMult) highestMult = apiResults[0].value;
        lastKnownId  = latestId;

        updateResultsDisplay();
        updateCharts();
        setStatus('connected');
      }

      updatePredictionUI(data);
      updateStatsUI(data);
      updateSignalBanner(data);
    }
  } catch(e) {
    setStatus('disconnected');
  }
  setTimeout(poll, POLL_MS);
}

// ─── CHART TABS ─────────────────────────────────────────────────────────────
document.querySelectorAll('.chart-tab').forEach(tab => {
  tab.addEventListener('click', e => {
    document.querySelectorAll('.chart-tab').forEach(t => t.classList.remove('active'));
    e.target.classList.add('active');
    const isLine = e.target.dataset.type === 'line';
    document.getElementById('lineChartCanvas').classList.toggle('active', isLine);
    document.getElementById('barChartCanvas').style.display  = isLine ? 'none' : 'block';
    document.getElementById('lineChartCanvas').style.display = isLine ? 'block' : 'none';
  });
});

// ─── INIT ────────────────────────────────────────────────────────────────────
initCharts();
// barChart oculto por defecto
document.getElementById('barChartCanvas').style.display = 'none';
setStatus('waiting');
poll();
</script>
</body>
</html>
"""

# ─────────────────────────────────────────────────────────────────────────────
# RUTAS FLASK
# ─────────────────────────────────────────────────────────────────────────────
@flask_app.route('/')
def home():
    elapsed = f"{int(time.time()-g_poller_status['last_round_ts'])}s" if g_poller_status['last_round_ts'] else "—"
    return (
        f"🤖 CrashBot Maestro ACTIVO | "
        f"Datos: {len(g_mults)}/400 | Señal: {g_signal_state} | "
        f"Giros: {g_poller_status['total_new_rounds']} | Último: {elapsed}"
    ), 200

@flask_app.route('/ping')
def ping():
    return "pong", 200

@flask_app.route('/maestro')
def maestro_dashboard():
    return render_template_string(MAESTRO_HTML)

@flask_app.route('/api/maestro_data')
def api_maestro_data():
    results = g_maestro_results[:50]
    values  = [r['value'] for r in results]

    avg       = sum(values) / len(values) if values else 0.0
    max_val   = max(values)              if values else 0.0
    green_pct = sum(1 for v in values if v >= WIN_TARGET) / len(values) * 100 if values else 0.0

    trend = maestro_strategy.analyze_trend(results)
    sr    = maestro_strategy.calculate_support_resistance(results)

    reset_daily_if_needed()

    return jsonify({
        # Resultados para el dashboard
        'results': [{'id': r['id'], 'value': r['value'], 'win': r['win']} for r in results],

        # Predicción
        'prediction': trend['prediction'],
        'detail':     trend['detail'],
        'risk':       trend['risk'],
        'confidence': trend['confidence'],

        # Niveles
        'support':    sr['support'],
        'resistance': sr['resistance'],

        # Stats
        'avg':       avg,
        'max':       max_val,
        'green_pct': round(green_pct, 1),

        # Marcador diario
        'daily_wins':   daily_stats['wins'],
        'daily_losses': daily_stats['losses'],

        # Estado de señal activa
        'signal_active':  g_signal_state == 'evaluating',
        'signal_trigger': g_signal_trigger_mult,
        'signal_col':     g_session.col,
        'signal_attempt': g_session.attempt,
        'signal_bet':     g_session.cur_bet,
        'max_attempts':   MAX_ATTS,

        # Poller status
        'total_rounds':  g_poller_status['total_new_rounds'],
        'data_count':    len(g_mults),
    })

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ─────────────────────────────────────────────────────────────────────────────
# SELF-PING (Render keep-alive)
# ─────────────────────────────────────────────────────────────────────────────
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
# HANDLERS TELEGRAM
# ─────────────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name       = message.from_user.first_name or "usuario"
    stats      = get_quota_stats(200)
    stats_blk  = quota_stats_text(stats)
    data_info  = f"📡 `{len(g_mults)}/400` multiplicadores recopilados" if g_mults else "📡 Recopilando datos..."
    await bot.reply_to(message,
        f"🚀 *¡Bienvenido {name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🎭 *Bot de Señales Crash — Estrategia Maestro + Filtro Moderado*\n"
        "📊 Análisis de rachas + EMAs | Detección de zonas de entrada\n"
        f"🎯 Objetivo: `{WIN_TARGET:.2f}x` | Gestión: 3C×2I\n"
        f"💰 Apuesta base: `${BASE_BET:.2f}`\n"
        f"🧠 Confianza mínima: `{MAESTRO_MIN_CONFIDENCE*100:.0f}%` | Solo señales `risk=low` + alerta moderada\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📢 *Señales en el canal oficial*\n"
        "🤖 *Comandos:* /señal /estadisticas /tendencia\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{data_info}\n\n{stats_blk}",
        parse_mode='Markdown')

@bot.message_handler(commands=['señal'])
async def cmd_signal(message):
    if not g_maestro_results:
        await bot.reply_to(message, "📡 *Maestro*: Aún no hay suficientes datos.", parse_mode='Markdown')
        return
    trend        = maestro_strategy.analyze_trend(g_maestro_results)
    should, conf, reason = maestro_strategy.should_enter(g_maestro_results)
    sr           = maestro_strategy.calculate_support_resistance(g_maestro_results)
    status_text  = "✅ SEÑAL ACTIVA (risk=low + moderado)" if should else "❌ Sin señal ahora"
    support_txt  = f"🟢 Soporte: `{sr['support']:.2f}x`"    if sr['support']    else "🟢 Soporte: `—`"
    resist_txt   = f"🔴 Resistencia: `{sr['resistance']:.2f}x`" if sr['resistance'] else "🔴 Resistencia: `—`"
    await bot.reply_to(message,
        f"🎭 *Estrategia Maestro + Filtro Moderado* (objetivo ≥ {WIN_TARGET:.2f}x)\n"
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
    s              = g_session
    stats          = get_quota_stats(200)
    trend          = quota_stats_text(stats)
    gp_line        = s.status_short()
    fichas_rec     = s.fichas[-15:]
    if fichas_rec:
        lineas = []
        for f in fichas_rec:
            total  = f['c1'] + f['c2'] + f['c3']
            net    = BASE_BET if f['result'] == 'win' else -total
            res    = "✅" if f['result'] == 'win' else "❌"
            cols   = f"C1:${f['c1']:.2f}" + (f" C2:${f['c2']:.2f}" if f['c2'] > 0 else "") + (f" C3:${f['c3']:.2f}" if f['c3'] > 0 else "")
            neto   = f"+${net:.2f}" if net >= 0 else f"-${abs(net):.2f}"
            lineas.append(f"{res} #{f['n']} {f.get('ts','--:--')} | {cols} | {neto}")
        fichas_txt = "\n".join(lineas)
        total_f    = len(s.fichas)
        wins_f     = sum(1 for f in s.fichas if f['result'] == 'win')
        resumen    = f"Total fichas: `{total_f}` | ✅ `{wins_f}` | ❌ `{total_f-wins_f}`"
    else:
        fichas_txt = "_Sin fichas registradas aún._"
        resumen    = "Total fichas: `0` | ✅ `0` | ❌ `0`"
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
    logger.info("🎭 Iniciando CrashBot Maestro Unificado + Filtro Moderado")
    reset_daily_if_needed()
    await bot.set_my_commands([
        types.BotCommand('start',        '🚀 Iniciar'),
        types.BotCommand('señal',        '🎯 Última predicción Maestro'),
        types.BotCommand('estadisticas', '📊 Estadísticas y fichas'),
        types.BotCommand('tendencia',    '📈 Tendencia de cuotas'),
    ])
    asyncio.create_task(http_poller())
    asyncio.create_task(self_ping_loop())
    logger.info("✅ Tareas iniciadas — polling Telegram...")
    await bot.infinity_polling(skip_pending=True)

if __name__ == '__main__':
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    asyncio.run(main_async())
