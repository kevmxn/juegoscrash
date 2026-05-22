#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║   SPACEMAN BOT — Señales de Continuación (2 intentos)       ║
║   WebSocket real | Solo CANAL | HTML | Safe 1.50x           ║
║   Eliminación de mensajes de tendencia en el canal          ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import threading
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Set
from flask import Flask
import websockets
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from telebot.asyncio_helper import ApiTelegramException
import aiohttp

# ─── LOGGING ──────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class Config:
    """Configuración global del bot (modificable manualmente)."""
    BOT_TOKEN = "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo"
    CHANNEL_ID = -1003613599867      # Canal fijo

    WS_URL = "wss://dga.pragmaticplaylive.net/ws"
    CASINO_ID = "ppcdk00000005349"
    CURRENCY = "BRL"
    GAME_ID = 1301

    WIN_TARGET = 2.00          # Para cálculo de posición (+1/-1)
    SAFE_TARGET = 1.50         # A partir de este valor se considera ganada la señal
    MAX_MULTS = 400
    TRIM_MULTS = 200

    THRESHOLD_1_199_MAX = 54.0
    THRESHOLD_2_499_MIN = 28.0

    SIGNAL_MAX_BARS = 2
    EMA_FAST = 4
    EMA_SLOW = 12


class TrendAnalyzer:
    # ... (sin cambios, igual que antes)
    def __init__(self, config: Config):
        self.config = config
        self.mults: List[Dict] = []
        self.current_favorable: Optional[bool] = None

    def add_multiplier(self, value: float, round_id: str):
        self.mults.append({'value': value, 'id': round_id, 'ts': time.time()})
        if len(self.mults) > self.config.MAX_MULTS:
            self.mults = self.mults[-self.config.TRIM_MULTS:]

    def get_stats(self, n: int = 200) -> dict:
        data = self.mults[-n:] if len(self.mults) >= n else self.mults
        total = len(data)
        if total == 0:
            return {
                'total': 0, 'has_enough': False, 'favorable': None,
                'count_100_199': 0, 'count_200_499': 0,
                'count_500_999': 0, 'count_1000_plus': 0,
                'pct_100_199': 0.0, 'pct_200_499': 0.0,
                'pct_500_999': 0.0, 'pct_1000_plus': 0.0,
            }
        r1 = sum(1 for m in data if 1.00 <= m['value'] < 2.00)
        r2 = sum(1 for m in data if 2.00 <= m['value'] < 5.00)
        r3 = sum(1 for m in data if 5.00 <= m['value'] < 10.00)
        r4 = sum(1 for m in data if m['value'] >= 10.00)
        pct1 = r1 / total * 100
        pct2 = r2 / total * 100
        pct3 = r3 / total * 100
        pct4 = r4 / total * 100
        unfavorable = (pct1 > self.config.THRESHOLD_1_199_MAX) or (pct2 < self.config.THRESHOLD_2_499_MIN)
        favorable = not unfavorable
        return {
            'total': total,
            'has_enough': total >= n,
            'favorable': favorable,
            'count_100_199': r1,
            'count_200_499': r2,
            'count_500_999': r3,
            'count_1000_plus': r4,
            'pct_100_199': pct1,
            'pct_200_499': pct2,
            'pct_500_999': pct3,
            'pct_1000_plus': pct4,
        }

    def update_trend(self) -> Optional[bool]:
        stats = self.get_stats(200)
        if stats['total'] < 10:
            return None
        new_fav = stats['favorable']
        if new_fav != self.current_favorable:
            self.current_favorable = new_fav
            return new_fav
        return None


class SignalEngine:
    # ... (sin cambios, igual que antes)
    def __init__(self, config: Config, trend_analyzer: TrendAnalyzer):
        self.config = config
        self.trend = trend_analyzer
        self.positions: List[int] = []
        self.ema4: List[float] = []
        self.ema12: List[float] = []
        self.signal_pending = {
            'active': False,
            'created_index': -1,
            'trigger_value': 0.0,
            'observed_values': [],
            'max_bars': config.SIGNAL_MAX_BARS,
            'second_attempt_sent': False
        }
        self.daily_wins = 0
        self.daily_losses = 0
        self.last_reset_date = None

    def argentina_now(self) -> datetime:
        return datetime.utcnow() - timedelta(hours=3)

    def reset_daily_if_needed(self):
        today = self.argentina_now().date()
        if self.last_reset_date is None:
            self.last_reset_date = today
            return
        if today != self.last_reset_date:
            self.daily_wins = 0
            self.daily_losses = 0
            self.last_reset_date = today
            logger.info("📆 Marcador diario reiniciado")

    def classify(self, value: float) -> int:
        v = value
        if 1.00 <= v <= 1.09: return -10
        if 1.10 <= v <= 1.19: return -9
        if 1.20 <= v <= 1.29: return -8
        if 1.30 <= v <= 1.39: return -7
        if 1.40 <= v <= 1.49: return -6
        if 1.50 <= v <= 1.59: return -5
        if 1.60 <= v <= 1.69: return -4
        if 1.70 <= v <= 1.79: return -3
        if 1.80 <= v <= 1.89: return -2
        if 1.90 <= v <= 1.99: return -1
        if 2.00 <= v <= 2.99: return 1
        if 3.00 <= v <= 3.99: return 2
        if 4.00 <= v <= 4.99: return 3
        if 5.00 <= v <= 5.99: return 4
        if 6.00 <= v <= 6.99: return 5
        if 7.00 <= v <= 7.99: return 6
        if 8.00 <= v <= 8.99: return 7
        if 9.00 <= v <= 9.99: return 8
        if 10.00 <= v <= 14.99: return 9
        if 15.00 <= v <= 19.99: return 10
        return 0

    def update_position(self, value: float) -> int:
        return 1 if value >= self.config.WIN_TARGET else -1

    def calc_ema(self, data: List[int], period: int) -> List[float]:
        if not data:
            return []
        k = 2 / (period + 1)
        ema = [float(data[0])]
        for i in range(1, len(data)):
            ema.append((data[i] - ema[i-1]) * k + ema[i-1])
        return ema

    def check_continuation_signal(self) -> bool:
        if len(self.positions) < 12 or len(self.ema4) < 12 or len(self.ema12) < 12:
            return False
        conditions = []
        ema4_curr = self.ema4[-1]
        ema4_prev = self.ema4[-2] if len(self.ema4) > 1 else ema4_curr
        ema12_curr = self.ema12[-1]
        conditions.append(ema4_curr > ema12_curr and (ema4_curr - ema4_prev) > 0)
        last8 = self.positions[-8:] if len(self.positions) >= 8 else self.positions
        bullish_count = sum(1 for p in last8 if p == 1)
        conditions.append(bullish_count >= 5)
        if len(self.trend.mults) >= 8:
            recent = [self.classify(m['value']) for m in self.trend.mults[-4:]]
            prev = [self.classify(m['value']) for m in self.trend.mults[-8:-4]]
            conditions.append(sum(recent)/4 > sum(prev)/4 - 0.1)
        else:
            conditions.append(False)
        if len(self.positions) >= 5:
            conditions.append(self.positions[-1] - self.positions[-5] > 0)
        else:
            conditions.append(False)
        if len(self.positions) >= 2:
            inc = self.positions[-1] - self.positions[-2]
            last_force = self.classify(self.trend.mults[-1]['value']) if self.trend.mults else 0
            conditions.append(inc > 0 and last_force > 0)
        else:
            conditions.append(False)
        return sum(conditions) >= 3

    def add_multiplier(self, value: float, round_id: str):
        inc = self.update_position(value)
        self.positions.append(inc if self.positions else inc)
        self.trend.add_multiplier(value, round_id)
        if len(self.positions) > self.config.MAX_MULTS:
            self.positions = self.positions[-self.config.TRIM_MULTS:]
        self.ema4 = self.calc_ema(self.positions, self.config.EMA_FAST)
        self.ema12 = self.calc_ema(self.positions, self.config.EMA_SLOW)

        if self.signal_pending['active']:
            self.signal_pending['observed_values'].append(value)
            observed_len = len(self.signal_pending['observed_values'])
            if value >= self.config.SAFE_TARGET:
                return self._resolve_signal(True)
            if observed_len == 1 and not self.signal_pending.get('second_attempt_sent', False):
                self.signal_pending['second_attempt_sent'] = True
                return {
                    'type': 'second_attempt',
                    'trigger': self.signal_pending['trigger_value'],
                    'attempt': 2
                }
            if observed_len >= self.signal_pending['max_bars']:
                return self._resolve_signal(False)
            return None

        if self.check_continuation_signal():
            self.signal_pending = {
                'active': True,
                'created_index': len(self.positions) - 1,
                'trigger_value': value,
                'observed_values': [],
                'max_bars': self.config.SIGNAL_MAX_BARS,
                'second_attempt_sent': False
            }
            return {'type': 'signal', 'trigger': value, 'attempt': 1}
        return None

    def _resolve_signal(self, is_win: bool):
        trigger = self.signal_pending['trigger_value']
        observed = self.signal_pending['observed_values'][:]
        self.signal_pending['active'] = False
        self.signal_pending['observed_values'] = []
        self.signal_pending['second_attempt_sent'] = False
        self.reset_daily_if_needed()
        if is_win:
            self.daily_wins += 1
        else:
            self.daily_losses += 1
        return {'type': 'resolution', 'is_win': is_win, 'trigger': trigger, 'observed': observed}

    def get_daily_stats(self):
        self.reset_daily_if_needed()
        total = self.daily_wins + self.daily_losses
        win_rate = (self.daily_wins / total * 100) if total > 0 else 0.0
        return {
            'wins': self.daily_wins,
            'losses': self.daily_losses,
            'win_rate': win_rate,
            'last_reset': str(self.last_reset_date) if self.last_reset_date else None
        }


class TelegramBotHandler:
    """
    Maneja la interacción con Telegram.
    - Los mensajes de señales, resoluciones y cambios de tendencia se envían SOLO al canal.
    - Los comandos responden a los usuarios individuales (no afectan al canal).
    """

    def __init__(self, config: Config, signal_engine: SignalEngine, trend_analyzer: TrendAnalyzer):
        self.config = config
        self.signal_engine = signal_engine
        self.trend = trend_analyzer
        self.bot = AsyncTeleBot(config.BOT_TOKEN)
        self.registered_chats: Set[int] = set()   # solo para comandos, no para señales
        self.last_trend_msg_id: Optional[int] = None   # ID del último mensaje de tendencia en el canal
        self._setup_handlers()

    def _setup_handlers(self):
        @self.bot.message_handler(commands=['start'])
        async def start_cmd(message):
            await self.cmd_start(message)

        @self.bot.message_handler(commands=['estadisticas'])
        async def stats_cmd(message):
            await self.cmd_estadisticas(message)

        @self.bot.message_handler(commands=['tendencia'])
        async def trend_cmd(message):
            await self.cmd_tendencia(message)

    async def cmd_start(self, message):
        name = message.from_user.first_name or "usuario"
        self.registered_chats.add(message.chat.id)
        self.signal_engine.reset_daily_if_needed()
        msg = (
            f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
            "🤖 <b>Bot de Señales Spaceman</b>\n"
            "🎯 Sistema de continuación alcista | 2 intentos\n"
            f"🛡️ <b>Objetivo seguro: ≥{self.config.SAFE_TARGET:.2f}x</b>\n"
            "<b>📊 Umbrales de tendencia configurados:</b>\n"
            f"   • 1.00-1.99x: ≤{self.config.THRESHOLD_1_199_MAX:.0f}%\n"
            f"   • 2.00-4.99x: ≥{self.config.THRESHOLD_2_499_MIN:.0f}%\n"
            "🔔 Las señales se envían al canal.\n"
            "📊 Usa /estadisticas o /tendencia para consultar."
        )
        await self.bot.reply_to(message, msg, parse_mode='HTML')

    async def cmd_estadisticas(self, message):
        self.registered_chats.add(message.chat.id)
        stats = self.signal_engine.get_daily_stats()
        marcador = (
            f"<b>📊 MARCADOR DIARIO</b>\n"
            f"✅ GANADAS: <code>{stats['wins']}</code>\n"
            f"❌ PERDIDAS: <code>{stats['losses']}</code>\n"
            f"📈 ACIERTOS = <code>{stats['win_rate']:.2f}%</code>\n"
            f"🕒 Último reinicio: {stats['last_reset']}"
        )
        await self.bot.reply_to(message, marcador, parse_mode='HTML')

    async def cmd_tendencia(self, message):
        self.registered_chats.add(message.chat.id)
        stats = self.trend.get_stats(200)
        if stats['total'] == 0:
            await self.bot.reply_to(message, "⏳ Aún no hay suficientes datos (mínimo 200 multiplicadores).", parse_mode='HTML')
            return
        fav = stats['favorable']
        estado = "🟢 FAVORABLE" if fav else "🔴 DESFAVORABLE"
        if fav is None:
            estado = "⚪ INDEFINIDA"
        respuesta = (
            f"<b>📡 TENDENCIA ACTUAL</b> (<code>{stats['total']}</code> últimos multiplicadores)\n"
            f"{estado}\n\n"
            f"🔵 1.00-1.99x: <code>{stats['pct_100_199']:.1f}%</code> (límite ≤{self.config.THRESHOLD_1_199_MAX:.0f}%)\n"
            f"🟣 2.00-4.99x: <code>{stats['pct_200_499']:.1f}%</code> (mínimo ≥{self.config.THRESHOLD_2_499_MIN:.0f}%)\n"
            f"🟡 5.00-9.99x: <code>{stats['pct_500_999']:.1f}%</code>\n"
            f"🔴 +10.00x: <code>{stats['pct_1000_plus']:.1f}%</code>\n\n"
            f"<b>Umbrales configurables:</b>\n• Para 1.00-1.99x: ≤{self.config.THRESHOLD_1_199_MAX:.0f}%\n• Para 2.00-4.99x: ≥{self.config.THRESHOLD_2_499_MIN:.0f}%"
        )
        await self.bot.reply_to(message, respuesta, parse_mode='HTML')

    async def send_to_channel(self, msg: str, delete_previous_trend: bool = False):
        """
        Envía un mensaje al canal fijo.
        Si delete_previous_trend es True, elimina el último mensaje de tendencia antes de enviar el nuevo.
        """
        chat_id = self.config.CHANNEL_ID
        try:
            if delete_previous_trend and self.last_trend_msg_id is not None:
                try:
                    await self.bot.delete_message(chat_id, self.last_trend_msg_id)
                    logger.debug(f"Mensaje de tendencia anterior eliminado en canal {chat_id}")
                except Exception as e:
                    logger.debug(f"No se pudo eliminar mensaje anterior en canal: {e}")
            sent = await self.bot.send_message(chat_id, msg, parse_mode='HTML')
            if delete_previous_trend:
                self.last_trend_msg_id = sent.message_id
        except Exception as e:
            logger.error(f"Error enviando mensaje al canal {chat_id}: {e}")

    async def send_signal_message(self, trigger: float, attempt: int):
        msg = (
            f"🚨 <b>Entrar después de:</b> <code>{trigger:.2f}x</code>\n"
            f"💎 <b>Señal para 2.00x</b>\n"
            f"⚪ <b>Seguro en 1.50x</b>\n"
            f"🆔 <b>Intento {attempt}/2</b>"
        )
        await self.send_to_channel(msg)

    async def send_resolution_message(self, is_win: bool, trigger: float, observed: List[float]):
        if is_win:
            winning_value = observed[-1]
            attempt_index = len(observed)  # 1 o 2
            result_line = f"✅ WIN GALE #{attempt_index-1} — {winning_value:.2f}x"
        else:
            if len(observed) >= 2:
                result_line = f"❌ LOSS {observed[0]:.2f}x — {observed[1]:.2f}x"
            else:
                result_line = f"❌ LOSS {observed[0]:.2f}x"
        stats = self.signal_engine.get_daily_stats()
        marcador = (
            f"<b>📊 MARCADOR DIARIO:</b>\n"
            f"✅ GANADAS: <code>{stats['wins']}</code>\n"
            f"❌ PERDIDAS: <code>{stats['losses']}</code>\n"
            f"📈 ACIERTOS = <code>{stats['win_rate']:.2f}%</code>"
        )
        full_msg = f"{result_line}\n\n{marcador}"
        await self.send_to_channel(full_msg)

    async def send_trend_change(self, favorable: bool):
        hora = (datetime.utcnow() - timedelta(hours=3)).strftime("%H:%M")
        if favorable:
            msg = f"🟢 <b>TENDENCIA FAVORABLE</b> {hora}\nUmbrales: ≤{self.config.THRESHOLD_1_199_MAX:.0f}% para 1.00-1.99x | ≥{self.config.THRESHOLD_2_499_MIN:.0f}% para 2.00-4.99x"
        else:
            msg = f"🔴 <b>TENDENCIA DESFAVORABLE</b> {hora}\nUmbrales: ≤{self.config.THRESHOLD_1_199_MAX:.0f}% para 1.00-1.99x | ≥{self.config.THRESHOLD_2_499_MIN:.0f}% para 2.00-4.99x"
        # Enviar al canal y eliminar el mensaje anterior de tendencia
        await self.send_to_channel(msg, delete_previous_trend=True)

    async def set_commands(self):
        await self.bot.set_my_commands([
            types.BotCommand('start', '🚀 Iniciar / recibir señales'),
            types.BotCommand('estadisticas', '📊 Ver marcador diario'),
            types.BotCommand('tendencia', '📈 Ver estado de la tendencia'),
        ])

    async def infinity_polling(self, max_retries=5):
        try:
            await self.bot.delete_webhook()
            await asyncio.sleep(2)
            await self.bot.get_updates(offset=-1, timeout=1)
            logger.info("✅ Webhook y updates limpiados")
        except Exception as e:
            logger.warning(f"Limpieza inicial falló: {e}")

        retry = 0
        while retry < max_retries:
            try:
                logger.info(f"Iniciando polling (intento {retry+1}/{max_retries})...")
                await self.bot.infinity_polling(skip_pending=True)
                return
            except ApiTelegramException as e:
                if "Conflict" in str(e) and "getUpdates" in str(e):
                    retry += 1
                    logger.warning(f"Conflicto 409 - Reintento {retry}/{max_retries}")
                    if retry < max_retries:
                        try:
                            await self.bot.delete_webhook()
                            await self.bot.get_updates(offset=-1, timeout=1)
                        except:
                            pass
                        await asyncio.sleep(12)
                    else:
                        logger.critical("No se pudo resolver el conflicto. Revisa que no haya otra instancia corriendo.")
                        raise
                else:
                    logger.error(f"Error de Telegram no recuperable: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error inesperado en polling: {e}")
                raise


class WebSocketCollector:
    def __init__(self, config: Config, signal_engine: SignalEngine, bot_handler: TelegramBotHandler):
        self.config = config
        self.signal_engine = signal_engine
        self.bot = bot_handler
        self.seen_ids: Set[str] = set()
        self.last_value: Optional[float] = None

    async def run(self):
        while True:
            try:
                logger.info("🔌 Conectando al WebSocket de Spaceman...")
                async with websockets.connect(
                    self.config.WS_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10
                ) as ws:
                    subscribe_msg = {
                        "type": "subscribe",
                        "casinoId": self.config.CASINO_ID,
                        "currency": self.config.CURRENCY,
                        "key": [self.config.GAME_ID]
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info("✅ Suscrito a Spaceman")

                    async for raw_msg in ws:
                        try:
                            data = json.loads(raw_msg)
                            game_results = data.get('gameResult', [])
                            if not game_results:
                                continue
                            first = game_results[0]
                            value = float(first.get('result', 0))
                            if value <= 0:
                                continue

                            round_id = str(
                                first.get('roundId') or
                                first.get('gameRoundId') or
                                first.get('id') or
                                f"{value}_{int(time.time()*1000)}"
                            )

                            if round_id in self.seen_ids:
                                continue
                            if value == self.last_value:
                                continue

                            self.seen_ids.add(round_id)
                            self.last_value = value

                            event = self.signal_engine.add_multiplier(value, round_id)
                            if event:
                                if event['type'] == 'signal':
                                    await self.bot.send_signal_message(event['trigger'], event['attempt'])
                                elif event['type'] == 'second_attempt':
                                    await self.bot.send_signal_message(event['trigger'], event['attempt'])
                                elif event['type'] == 'resolution':
                                    await self.bot.send_resolution_message(
                                        event['is_win'], event['trigger'], event['observed']
                                    )

                            if len(self.seen_ids) > 2000:
                                oldest = sorted(self.seen_ids)[:1000]
                                for oid in oldest:
                                    self.seen_ids.discard(oid)

                            if len(self.signal_engine.trend.mults) % 10 == 0:
                                new_trend = self.signal_engine.trend.update_trend()
                                if new_trend is not None:
                                    await self.bot.send_trend_change(new_trend)

                        except (json.JSONDecodeError, KeyError, ValueError) as e:
                            logger.debug(f"Mensaje ignorado: {e}")
                        except Exception as e:
                            logger.error(f"Error procesando mensaje WS: {e}")

            except websockets.ConnectionClosed as e:
                logger.warning(f"WebSocket cerrado ({e.code}): {e.reason}")
            except Exception as e:
                logger.error(f"Error de WebSocket: {e}")

            logger.info("🔄 Reconectando en 5 segundos...")
            await asyncio.sleep(5)


class SpacemanBot:
    def __init__(self):
        self.config = Config()
        self.trend_analyzer = TrendAnalyzer(self.config)
        self.signal_engine = SignalEngine(self.config, self.trend_analyzer)
        self.bot_handler = TelegramBotHandler(self.config, self.signal_engine, self.trend_analyzer)
        self.ws_collector = WebSocketCollector(self.config, self.signal_engine, self.bot_handler)
        self.flask_app = Flask(__name__)
        self._setup_flask()

    def _setup_flask(self):
        @self.flask_app.route('/')
        def home():
            return (
                f"🤖 SpacemanBot | Velas: {len(self.trend_analyzer.mults)} | "
                f"Señal pendiente: {self.signal_engine.signal_pending['active']} | "
                f"Marcador: {self.signal_engine.daily_wins}/{self.signal_engine.daily_losses}"
            ), 200

        @self.flask_app.route('/ping')
        def ping():
            return "pong", 200

        @self.flask_app.route('/stats')
        def stats():
            return {
                "status": "ok",
                "mults_collected": len(self.trend_analyzer.mults),
                "signal_pending": self.signal_engine.signal_pending['active'],
                "daily_wins": self.signal_engine.daily_wins,
                "daily_losses": self.signal_engine.daily_losses,
                "registered_chats": len(self.bot_handler.registered_chats),
                "last_reset": str(self.signal_engine.last_reset_date) if self.signal_engine.last_reset_date else None,
                "trend_favorable": self.trend_analyzer.current_favorable,
                "thresholds": {
                    "1.00-1.99_max": self.config.THRESHOLD_1_199_MAX,
                    "2.00-4.99_min": self.config.THRESHOLD_2_499_MIN
                },
                "safe_target": self.config.SAFE_TARGET
            }

    def run_flask(self):
        port = int(os.environ.get('PORT', 8080))
        self.flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

    async def self_ping_loop(self):
        render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
        if not render_url:
            logger.info("RENDER_EXTERNAL_URL no configurada — self-ping desactivado")
            return
        url = f"{render_url.rstrip('/')}/ping"
        while True:
            await asyncio.sleep(14 * 60)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        logger.info(f"Self-ping OK: {resp.status}")
            except Exception as e:
                logger.warning(f"Self-ping falló: {e}")

    async def run(self):
        logger.info("🤖 Iniciando SpacemanBot (Solo canal, Safe Target 1.50x)")
        logger.info(f"📊 Umbrales: 1.00-1.99x ≤{self.config.THRESHOLD_1_199_MAX}% | 2.00-4.99x ≥{self.config.THRESHOLD_2_499_MIN}%")
        await self.bot_handler.set_commands()
        asyncio.create_task(self.ws_collector.run())
        asyncio.create_task(self.self_ping_loop())
        flask_thread = threading.Thread(target=self.run_flask, daemon=True)
        flask_thread.start()
        logger.info(f"🌐 Flask iniciado en puerto {os.environ.get('PORT', 8080)}")
        await self.bot_handler.infinity_polling()


if __name__ == '__main__':
    bot = SpacemanBot()
    asyncio.run(bot.run())
