#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
==========================================================
APPALÍ SCANNER - Version 7.5 (Production Engine)
==========================================================
Sincronización Horaria y Operativa con IQ Option.
- Nomenclatura Oficial de Activos IQ Option: EUR/USD (OTC) en paréntesis.
- Engine de Tiempo Centralizado (BrokerClock con zonas IANA via zoneinfo).
- Verificación de Activos Operables mediante antigüedad de velas (STALE_DATA) y tipo de activo.
- Separación Rigurosa Real/OTC y Detección Automática de Sesiones.
- Dashboard Integrado con Panel del Reloj del Broker y Métricas en Tiempo Real.
- Estrategia GSR, Indicadores Matemáticos y Persistencia CSV Intactos.
"""

import asyncio
import csv
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import ta
import uvicorn

# ==========================================================
# CONFIGURACIÓN Y CREDENCIALES DE ENTORNO
# ==========================================================
APP_NAME = "AppALÍ"
VERSION = "7.5-PROD"
HOST = "0.0.0.0"
PORT = 8000

# Carga de credenciales desde un archivo .env LOCAL (ignorado por git).
# Las credenciales NO van hardcodeadas en el código: se leen del entorno o del
# .env (ver .env.example). Si faltan, la app corre en simulación.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except Exception:
    pass

IQ_EMAIL = os.environ.get("IQ_EMAIL", "")
IQ_PASSWORD = os.environ.get("IQ_PASSWORD", "")
IQ_MODE = os.environ.get("IQ_ACCOUNT_TYPE", "PRACTICE")

# Puente local de WhatsApp (publica las señales en el grupo de la comunidad).
# Proyecto: whatsapp_bridge/  |  Endpoint HTTP del puente.
WHATSAPP_BRIDGE_URL = os.environ.get("WHATSAPP_BRIDGE_URL", "http://127.0.0.1:8120")
WHATSAPP_BRIDGE_ENABLED = os.environ.get("WHATSAPP_BRIDGE_ENABLED", "1") == "1"

# Modo diagnóstico: BROKER_DIAGNOSTIC=1 imprime el estado real del broker al conectar.
BROKER_DIAGNOSTIC = os.environ.get("BROKER_DIAGNOSTIC", "0") == "1"

# Parámetros calibrados de la estrategia GSR (INTACTOS)
RSI_PERIOD = 14
RSI_OVERBOUGHT = 80
RSI_OVERSOLD = 20
BB_PERIOD = 20
BB_STD = 2.0
DAMOA_PERIOD = 5
DAMOA_HIGH = 10
DAMOA_LOW = -10
BODY_OUTSIDE = 40.0

# --- Mejoras P1 / P3 / P4 / P5 ---
# P1: piso de volatilidad y límite para DAMOA (evita que explote a miles en mercado plano).
DAMOA_VOL_FLOOR_REL = 1e-6   # piso relativo al precio (RMS >= precio * 1e-6)
DAMOA_CLAMP = 50.0           # acota |DAMOA| a [±50] para descartar outliers
# P3: ventana (en velas M1) para esperar la vela que confirma la reversión.
REVERSAL_CONFIRM_WINDOW = 3
# P4: filtro de tendencia. Si la separación EMA(fast)-EMA(slow), normalizada por
# la desviación estándar, supera este umbral el mercado está en tendencia fuerte
# y NO es idóneo para una reversión a la media (se descarta la señal).
TREND_FAST = 50
TREND_SLOW = 150
TREND_FILTER_Z = 1.0
# P5: filtro de sesión/liquidez. Solo se "arma" el patrón de reversión en horas
# de alta liquidez (apertura/cruce Londres–Nueva York). Evita velas erráticas de
# rollover (21:00–22:00 UTC) y el cierre de fin de semana. Se puede desactivar
# con SESSION_FILTER_ENABLED=0.
SESSION_FILTER_ENABLED = os.environ.get("SESSION_FILTER_ENABLED", "1") == "1"
LIQUID_SESSION_START = 8     # hora UTC (apertura Londres)
LIQUID_SESSION_END = 17      # hora UTC (cierre Nueva York)

# --- Ventanas de "activo vivo" por tipo (segundos) ---
# El broker IQ publica velas de 60 s; si un activo esta realmente operando,
# la ultima vela siempre es reciente. Si el broker lo cierra/pausa, la edad
# de la ultima vela crece y el activo se marca CERRADO (no perder tiempo).
LIVE_WINDOW_REAL = 90    # Forex REAL: nueva vela cada ~60 s
LIVE_WINDOW_OTC = 180    # OTC: tolerancia por menor frecuencia / rollovers

# --- Horario oficial del mercado Forex REAL (interbancario) ---
# Abre: domingo 21:00 UTC  |  Cierra: viernes 21:00 UTC
FX_FRIDAY_CLOSE_HOUR = 21
FX_SUNDAY_OPEN_HOUR = 21

AUDIT_FILE = "signals_audit.csv"

# ==========================================================
# FORMATEADOR DE NOMBRES DE ACTIVOS (IQ OPTION STYLE)
# ==========================================================
def format_asset_display_name(raw_asset: str) -> str:
    """
    Convierte el símbolo interno de IQ Option al formato oficial visual del broker.
    Ejemplos:
      'EURUSD-OTC' -> 'EUR/USD (OTC)'
      'USDCHF-OTC' -> 'USD/CHF (OTC)'
      'EURUSD'     -> 'EUR/USD'
    """
    asset = raw_asset.upper().strip()
    is_otc = False
    is_op = False

    if "-OTC" in asset:
        asset = asset.replace("-OTC", "")
        is_otc = True
    elif "(OTC)" in asset:
        asset = asset.replace("(OTC)", "").strip()
        is_otc = True
    if "-OP" in asset:
        asset = asset.replace("-OP", "")
        is_op = True

    # Si tiene 6 caracteres (ej. EURUSD), agregar la barra '/'
    if len(asset) == 6:
        base_pair = f"{asset[:3]}/{asset[3:]}"
    else:
        base_pair = asset

    if is_otc:
        return f"{base_pair} (OTC)"
    if is_op:
        return f"{base_pair} (OP)"
    return base_pair


def broker_search_name(raw_asset: str) -> str:
    """Nombre para PEGAR en el buscador del broker.
    En IQ Option el mercado REAL se busca por el par simple ('EUR/USD') y el OTC
    por el par + '(OTC)'. El sufijo interno '(OP)' NO existe en el broker, por lo
    que se omite al copiar (así no hay que borrar nada a mano)."""
    asset = str(raw_asset).upper().strip()
    is_otc = asset.endswith("-OTC")
    for suf in ("-OTC", "-OP"):
        if asset.endswith(suf):
            asset = asset[:-len(suf)]
    if len(asset) == 6:
        asset = f"{asset[:3]}/{asset[3:]}"
    return f"{asset} (OTC)" if is_otc else asset

# ==========================================================
# GESTOR DE BITÁCORAS
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(APP_NAME)

try:
    from iqoptionapi.stable_api import IQ_Option
    IQ_AVAILABLE = True
except Exception:
    try:
        from pyiqoptionapi import IQOption as IQ_Option
        IQ_AVAILABLE = True
    except Exception:
        IQ_AVAILABLE = False
        log.warning("⚠️ Módulo iqoptionapi ausente en el entorno local. Ejecutando en simulación.")

# ==========================================================
# ZONAS HORARIAS Y MOTOR DE TIEMPO CENTRALIZADO (BROKER CLOCK)
# ==========================================================
TZ_UTC = ZoneInfo("UTC")
TZ_COLOMBIA = ZoneInfo("America/Bogota")
TZ_NY = ZoneInfo("America/New_York")
TZ_LONDON = ZoneInfo("Europe/London")

class ClockStatus(Enum):
    SYNCED = "SYNCED"
    DESYNCED = "DESYNCED"
    LOCAL_FALLBACK = "LOCAL_FALLBACK"

class BrokerClock:
    """Fuente Única Centralizada de Tiempo para todo el Scanner."""
    def __init__(self):
        self._offset: float = 0.0  # broker_time - local_time
        self._last_sync_local: float = 0.0
        self.status: ClockStatus = ClockStatus.LOCAL_FALLBACK
        self._lock = threading.Lock()

    def sync_with_broker(self, broker_timestamp: float) -> None:
        with self._lock:
            now_local = time.time()
            self._offset = broker_timestamp - now_local
            self._last_sync_local = now_local
            self.status = ClockStatus.SYNCED
            log.info(f"[CLOCK] Broker time synchronized. Offset: {self._offset:.3f}s")

    def now_ts(self) -> float:
        with self._lock:
            return time.time() + self._offset

    def now_utc(self) -> datetime:
        return datetime.fromtimestamp(self.now_ts(), tz=TZ_UTC)

    def now_colombia(self) -> datetime:
        return datetime.fromtimestamp(self.now_ts(), tz=TZ_COLOMBIA)

    def last_sync_time_str(self) -> str:
        if self._last_sync_local == 0.0:
            return "N/A"
        dt = datetime.fromtimestamp(self._last_sync_local, tz=TZ_COLOMBIA)
        return dt.strftime("%H:%M:%S")

    def get_telemetry(self) -> dict:
        now_b = self.now_utc()
        now_c = self.now_colombia()
        return {
            "broker_time": now_b.strftime("%H:%M:%S"),
            "utc_time": now_b.strftime("%H:%M:%S"),
            "colombia_time": now_c.strftime("%H:%M:%S"),
            "status": self.status.value,
            "last_sync": self.last_sync_time_str()
        }

broker_clock = BrokerClock()

# ==========================================================
# VALIDACIÓN DE SESIÓN FOREX POR ZONA HORARIA
# ==========================================================

def get_forex_session(utc_hour: int) -> str:
    """
    Determina la sesión Forex activa según la hora UTC.
    - Sydney: 22:00 - 07:00 UTC
    - Tokyo: 00:00 - 09:00 UTC
    - London: 08:00 - 17:00 UTC
    - New York: 13:00 - 22:00 UTC
    """
    if 22 <= utc_hour or utc_hour < 7:
        return "Sydney"
    elif 0 <= utc_hour < 9:
        return "Tokyo"
    elif 8 <= utc_hour < 17:
        return "London"
    elif 13 <= utc_hour < 22:
        return "New York"
    return "Closed"

def is_asset_available_in_session(asset: str, asset_type: str, last_candle_age: int) -> bool:
    """
    Disponibilidad por tipo según la REALIDAD del broker (velas frescas).
    El estado OPEN/CLOSED ya lo decide IQ Option (BrokerMarketState); aquí solo
    se exige que la última vela sea actual (sin inventar horarios).
    """
    if asset_type == "REAL":
        return last_candle_age < LIVE_WINDOW_REAL
    else:
        return last_candle_age < LIVE_WINDOW_OTC


def is_forex_real_schedule_open(now_ts: int) -> bool:
    """
    Horario oficial del mercado Forex REAL (el mismo que muestra el broker IQ):
    cerrado sábado y domingo antes de las 21:00 UTC, y viernes desde las 21:00 UTC.
    """
    dt = datetime.fromtimestamp(int(now_ts), tz=TZ_UTC)
    if dt.weekday() == 5:                                  # sábado: cerrado todo el día
        return False
    if dt.weekday() == 6 and dt.hour < FX_SUNDAY_OPEN_HOUR:  # domingo antes de las 21:00 UTC
        return False
    if dt.weekday() == 4 and dt.hour >= FX_FRIDAY_CLOSE_HOUR:  # viernes desde las 21:00 UTC
        return False
    return True


def is_liquid_session(now_ts: int) -> bool:
    """P5: True si estamos en una ventana de alta liquidez apta para la reversión.
    Descarta el rollover bancario (21:00–22:00 UTC) y el cierre de fin de semana,
    y acota a la sesión más líquida (LIQUID_SESSION_START–LIQUID_SESSION_END UTC).
    Útil también para el backtest (P7): usa el timestamp de cada vela, no el real."""
    dt = datetime.fromtimestamp(int(now_ts), tz=TZ_UTC)
    if dt.weekday() == 5:                                        # sábado
        return False
    if dt.weekday() == 6 and dt.hour < FX_SUNDAY_OPEN_HOUR:      # domingo antes de apertura
        return False
    if dt.weekday() == 4 and dt.hour >= FX_FRIDAY_CLOSE_HOUR:    # viernes tras cierre
        return False
    if dt.hour == 21:                                            # rollover bancario
        return False
    return LIQUID_SESSION_START <= dt.hour < LIQUID_SESSION_END


# Catálogo real de activos que soporta la librería (consts de iqoptionapi).
# Los pares que no estén aquí no se consultan (evita llamadas perdidas).
try:
    from iqoptionapi.constants import ACTIVES as _IQ_ACTIVES_DICT
    # Activos que el broker ofrece pero la librería clásica no trae en consts:
    # USD/COP (OTC) -> id real 2299 (descubierto vía get_all_init del broker).
    _IQ_ACTIVES_DICT.setdefault("USDCOP-OTC", 2299)
    IQ_ACTIVES_CATALOG = set(_IQ_ACTIVES_DICT)
except Exception:
    IQ_ACTIVES_CATALOG = set()

# ==========================================================
# ESTRUCTURAS DE DATOS E INDICADORES
# ==========================================================
@dataclass
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class IndicatorSnapshot:
    index: int
    bb_upper: float = 0.0
    bb_middle: float = 0.0
    bb_lower: float = 0.0
    rsi: float = 50.0
    damoa: float = 0.0
    body: float = 0.0
    outside: float = 0.0
    trend_ok: bool = True   # P4: True si NO hay tendencia fuerte (apto para reversión)

@dataclass
class Signal:
    asset: str
    direction: str
    timestamp: int
    rsi: float
    damoa: float
    strength: float
    message: str

SIGNALS = []
LOCK = threading.Lock()

class RuntimeLog:
    def __init__(self):
        self.rows = []
    def add(self, message):
        self.rows.append({"time": broker_clock.now_colombia().strftime("%H:%M:%S"), "message": message})
        if len(self.rows) > 300:
            self.rows.pop(0)
    def json(self):
        return self.rows

runtime_log = RuntimeLog()

class ScanMetrics:
    def __init__(self):
        self.cycles = 0
        self.candles_processed = 0
        self.assets_discarded = 0
        self.avg_latency_per_asset = 0.0
        self.real_open_count = 0
        self.otc_open_count = 0
        self.closed_count = 0
        self.no_data_count = 0

scan_metrics = ScanMetrics()

# ==========================================================
# AUDITORÍA LOCAL DE SEÑALES (CSV)
# ==========================================================
class SignalAuditLogger:
    def __init__(self, filename=AUDIT_FILE):
        self.filename = filename
        self._initialize_csv()

    def _initialize_csv(self):
        if not os.path.exists(self.filename):
            with open(self.filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp_UTC", "Fecha_Hora_COT", "Activo", "Direccion", "RSI", "DAMOA"])

    def record(self, asset, direction, timestamp, rsi, damoa):
        try:
            display_name = format_asset_display_name(asset)
            dt_col = datetime.fromtimestamp(timestamp, tz=TZ_COLOMBIA)
            fecha_str = dt_col.strftime('%Y-%m-%d %H:%M:%S')
            with open(self.filename, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, fecha_str, display_name, direction, round(rsi, 2), round(damoa, 2)])
            log.info(f"💾 Señal guardada en {self.filename}: {display_name}")
        except Exception as e:
            log.error(f"Error escribiendo en CSV: {e}")

audit_logger = SignalAuditLogger()

# ==========================================================
# ESTADO DE TRADING DE ACTIVOS (VALIDACIÓN POR VELAS)
# ==========================================================
class AssetStatusEnum(Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    NO_DATA = "NO_DATA"
    STALE_DATA = "STALE_DATA"
    BROKER_ERROR = "BROKER_ERROR"
    RECONNECTING = "RECONNECTING"

@dataclass
class AssetTradingStatus:
    asset: str
    asset_type: str  # "REAL" o "OTC"
    tradable: bool
    status: AssetStatusEnum
    last_candle_time: int = 0
    checked_at: int = 0
    reason: str = ""

# Lista Maestra de Candidatos (Pares Forex Real y OTC)
MASTER_CANDIDATE_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "EURJPY", "EURGBP", "GBPJPY"
]

# --- Filtro robusto de "par de divisas" ---
# En IQ Option el mercado REAL (forex vivo) usa el sufijo "-OP" y el feed
# sintético usa "-OTC" (ej. EURUSD-OP = EUR/USD real; EURUSD-OTC = OTC).
# Un instrumento es un par de divisas si su código (sin sufijo) son 6 letras y
# sus dos mitades de 3 letras están en FX_CURRENCIES. Esto excluye acciones
# (AMAZON, APPLE...), crypto (BTCUSD, ETHUSD...), metales (XAU, XAG...) e
# índices, e incluye tanto el mercado REAL (-OP) como el OTC (-OTC).
FOREX_CODE_RE = re.compile(r'^[A-Z]{6}(-OTC|-OP)?$')
FX_CURRENCIES = {
    "EUR", "USD", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD",
    "COP", "PEN", "MXN", "BRL", "ZAR", "INR", "HKD", "SGD", "THB",
    "TRY", "PLN", "NOK", "SEK", "DKK", "ILS", "CZK", "HUF", "RON",
    "IDR", "PHP", "KRW", "TWD", "MYR", "CNY", "CLP", "ARS", "UYU",
    "EGP", "NGN", "KES", "MAD", "PKR", "LKR", "VND", "KZT", "UAH",
    "BGN", "SAR", "AED", "QAR", "KWD", "JOD", "BHD", "OMR", "HRK",
}

# --- Lista curada: pares MÁS COMUNES (por liquidez/uso) ---
# Se priorizan estos pares para no saturar con 80+ instrumentos: cada par puede
# aparecer como REAL (-OP) y OTC (-OTC), ~50 activos en total. Ordenados por
# relevancia; los primeros tienen prioridad si hay que recortar.
COMMON_PAIR_RANK = {code: i for i, code in enumerate([
    # 1) Pares mayores (los más operados)
    "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    # 2) Mercados emergentes / LatAm (relevantes para este proyecto)
    "USDCOP", "USDBRL", "USDMXN", "USDZAR", "USDTRY", "USDINR",
    # 3) Cruces principales
    "EURJPY", "EURGBP", "GBPJPY", "EURCHF", "AUDJPY", "EURAUD", "EURCAD",
    "GBPCHF", "CADJPY", "CHFJPY", "AUDCAD", "AUDNZD", "NZDJPY", "EURNZD",
    "GBPAUD", "GBPCAD", "GBPNZD", "NZDCAD", "NZDCHF",
    # 4) Otros líquidos (Asia/EM)
    "USDSGD", "USDHKD", "USDTHB", "USDCLP", "USDPHP", "USDPLN",
])}
MAX_UNIVERSE = 50   # tope de activos monitoreados (~50 más comunes)

class LiveAssetManager:
    """Gestor Dinámico de Activos."""
    def __init__(self):
        self.active_tradable_assets: List[str] = []
        self.status_map: Dict[str, AssetTradingStatus] = {}
        self._lock = threading.Lock()

    def get_candidate_list(self) -> List[Tuple[str, str]]:
        # UNIVERSO CURADO desde el snapshot del broker: solo los pares más
        # comunes (COMMON_PAIR_RANK), REAL (-OP) y OTC (-OTC), con tope ~50.
        # Reduce streams y carga sobre la conexión.
        if not broker_market.assets:
            return []
        ranked = []
        for name, info in broker_market.assets.items():
            if not FOREX_CODE_RE.match(name):
                continue
            code = name[:6]            # el código de 6 letras (EURUSD)
            rank = COMMON_PAIR_RANK.get(code)
            if rank is None:
                continue               # no está en la lista de pares comunes
            base, quote = code[:3], code[3:]
            if (base not in FX_CURRENCIES) or (quote not in FX_CURRENCIES):
                continue
            tipo = "OTC" if name.endswith("-OTC") else "REAL"
            ranked.append((name, tipo, rank))
        ranked.sort(key=lambda x: (x[2], x[0]))   # prioridad y orden alfabético
        return [(n, t) for n, t, _ in ranked[:MAX_UNIVERSE]]

    def update_status(self, status: AssetTradingStatus):
        with self._lock:
            self.status_map[status.asset] = status

    def rebuild_active_assets(self):
        with self._lock:
            tradable = []
            real_cnt = 0
            otc_cnt = 0
            closed_cnt = 0
            nodata_cnt = 0

            for asset, st in self.status_map.items():
                if st.tradable and st.status == AssetStatusEnum.OPEN:
                    tradable.append(asset)
                    if st.asset_type == "REAL":
                        real_cnt += 1
                    else:
                        otc_cnt += 1
                elif st.status == AssetStatusEnum.CLOSED:
                    closed_cnt += 1
                elif st.status in (AssetStatusEnum.NO_DATA, AssetStatusEnum.STALE_DATA):
                    nodata_cnt += 1

            self.active_tradable_assets = tradable
            scan_metrics.real_open_count = real_cnt
            scan_metrics.otc_open_count = otc_cnt
            scan_metrics.closed_count = closed_cnt
            scan_metrics.no_data_count = nodata_cnt

            log.info(f"[MARKET] Activos filtrados operables: {len(tradable)} (REAL: {real_cnt}, OTC: {otc_cnt})")

    def all_tradable(self) -> List[str]:
        with self._lock:
            return self.active_tradable_assets.copy()

    def get_status(self, asset: str) -> Optional[AssetTradingStatus]:
        with self._lock:
            return self.status_map.get(asset)

asset_manager = LiveAssetManager()

# ==========================================================
# CACHÉ DE VELAS Y FEED DE MERCADO
# ==========================================================
class CandleCache:
    MAX = 250
    def __init__(self):
        self.cache = {}
    def initialize(self, asset, candles):
        self.cache[asset] = deque(candles, maxlen=self.MAX)
    def append(self, asset, candle):
        if asset not in self.cache:
            return
        last = self.cache[asset][-1]
        if last.timestamp == candle.timestamp:
            return
        self.cache[asset].append(candle)
    def get(self, asset):
        return list(self.cache.get(asset, []))

candle_cache = CandleCache()

class LiveMarketFeed:
    CANDLES = 250
    PERIOD = 60

    def __init__(self):
        self.market = {}

    def validate_and_fetch_asset(self, asset: str, asset_type: str) -> AssetTradingStatus:
        now_broker = int(broker_clock.now_ts())

        # === GATE: DISPONIBILIDAD OFICIAL DEL BROKER ===
        # Sin snapshot válido (UNKNOWN/STALE) NO se considera el activo operativo.
        if not broker_market.valid():
            status = AssetTradingStatus(
                asset=asset,
                asset_type=asset_type,
                tradable=False,
                status=AssetStatusEnum.CLOSED,
                last_candle_time=0,
                checked_at=now_broker,
                reason="Disponibilidad del broker no válida (UNKNOWN/STALE)"
            )
            self.market.pop(asset, None)
            log.warning(f"[MARKET] {format_asset_display_name(asset)} -> CERRADO (disponibilidad del broker no válida)")
            return status

        # Disponibilidad del activo según el broker (enabled && !is_suspended).
        info = broker_market.get(asset)
        if info is None or not info.get("open"):
            status = AssetTradingStatus(
                asset=asset,
                asset_type=asset_type,
                tradable=False,
                status=AssetStatusEnum.CLOSED,
                last_candle_time=0,
                checked_at=now_broker,
                reason="Broker: activo no disponible/cerrado"
            )
            self.market.pop(asset, None)
            log.warning(f"[MARKET] {format_asset_display_name(asset)} -> CERRADO (broker: no disponible)")
            return status

        candles = iq.get_candles(asset, self.PERIOD, self.CANDLES, now_broker)

        if candles is None or len(candles) < 50:
            status = AssetTradingStatus(
                asset=asset,
                asset_type=asset_type,
                tradable=False,
                status=AssetStatusEnum.NO_DATA,
                last_candle_time=0,
                checked_at=now_broker,
                reason="El broker no devolvió suficientes velas."
            )
            self.market.pop(asset, None)
            log.warning(f"[MARKET] {format_asset_display_name(asset)} -> NO_DATA")
            return status

        last_candle_ts = candles[-1].timestamp
        age = now_broker - last_candle_ts

        # === VALIDACIÓN POR SESIÓN Y TIPO DE ACTIVO ===
        if not is_asset_available_in_session(asset, asset_type, age):
            status = AssetTradingStatus(
                asset=asset,
                asset_type=asset_type,
                tradable=False,
                status=AssetStatusEnum.CLOSED,
                last_candle_time=last_candle_ts,
                checked_at=now_broker,
                reason=f"Activo no disponible en esta sesión (edad vela: {age}s)"
            )
            self.market.pop(asset, None)
            log.warning(f"[MARKET] {format_asset_display_name(asset)} -> CERRADO (edad: {age}s)")
            return status

        # Si pasó la validación, está abierto
        status = AssetTradingStatus(
            asset=asset,
            asset_type=asset_type,
            tradable=True,
            status=AssetStatusEnum.OPEN,
            last_candle_time=last_candle_ts,
            checked_at=now_broker,
            reason="OPEN"
        )

        if asset not in candle_cache.cache:
            candle_cache.initialize(asset, candles)
        else:
            candle_cache.append(asset, candles[-1])

        self.market[asset] = candle_cache.get(asset)
        scan_metrics.candles_processed += len(candles)
        log.info(f"[MARKET] {format_asset_display_name(asset)} -> OPEN (Vela viva hace {age}s)")
        return status

    def refresh_market_sessions(self):
        if not iq.is_connected():
            log.warning("[MARKET] Sin conexión con el broker. Reintentando...")
            if not ensure_connected():
                return

        candidates = asset_manager.get_candidate_list()
        for asset, asset_type in candidates:
            status = self.validate_and_fetch_asset(asset, asset_type)
            asset_manager.update_status(status)

        asset_manager.rebuild_active_assets()

        # Detección de "media conexión": si había activos operables y de pronto
        # son 0 (todo NO_DATA con el broker "conectado"), el canal está muerto:
        # forzamos una reconexión para no quedarnos escaneando nada.
        prev_count = getattr(self, "_last_tradable_count", 0)
        cur_count = len(asset_manager.active_tradable_assets)
        if prev_count > 0 and cur_count == 0:
            log.warning("[MARKET] Se perdieron activos que antes operaban (posible canal muerto). Forzando reconexión IQ...")
            ensure_connected()
        self._last_tradable_count = cur_count

    def fast_update_active(self):
        active = asset_manager.all_tradable()
        now_broker = int(broker_clock.now_ts())

        for asset in active:
            st = asset_manager.get_status(asset)
            asset_type = st.asset_type if st else "REAL"
            # TIEMPO REAL: preferir el stream (realtime) cuando esté listo;
            # get_candles() queda solo como histórico/fallback inicial.
            candles = candle_streams.get_realtime(asset)
            if not candles or len(candles) < 50:
                candles = iq.get_candles(asset, self.PERIOD, self.CANDLES, now_broker)

            if candles and len(candles) >= 50:
                last_ts = candles[-1].timestamp
                age = now_broker - last_ts
                # Misma ventana por tipo que la validación principal:
                max_age = LIVE_WINDOW_OTC if asset_type == "OTC" else LIVE_WINDOW_REAL
                if age <= max_age:
                    candle_cache.append(asset, candles[-1])
                    self.market[asset] = candle_cache.get(asset)
                    continue
                # La vela dejó de refrescar: datos atrasados (stale).
                if st:
                    st.tradable = False
                    st.status = AssetStatusEnum.STALE_DATA
                    st.reason = f"Vela sin refrescar (edad {age}s > {max_age}s)"
                    asset_manager.update_status(st)
                self.market.pop(asset, None)

            log.warning(f"[MARKET] {format_asset_display_name(asset)} presentó anomalía en Fast Loop.")

    def get(self, asset):
        return self.market.get(asset)

    def active_assets(self):
        return list(self.market.keys())

market_feed = LiveMarketFeed()

# ==========================================================
# CÁLCULOS MATEMÁTICOS E INDICADORES GSR
# ==========================================================
class CandleValidator:
    MINIMUM = 200
    def validate(self, candles):
        return candles is not None and len(candles) >= self.MINIMUM

candle_validator = CandleValidator()

class NewCandleDetector:
    def __init__(self):
        self.timestamps = {}
    def detect(self, asset, candle):
        last = self.timestamps.get(asset)
        if last == candle.timestamp:
            return False
        self.timestamps[asset] = candle.timestamp
        return True

new_candle = NewCandleDetector()

class IndicatorCache:
    def __init__(self):
        self.rows = {}
    def update(self, asset, indicator):
        self.rows[asset] = indicator
    def get(self, asset):
        return self.rows.get(asset)

indicator_cache = IndicatorCache()

class IncrementalIndicatorEngine:
    def __init__(self):
        self.timestamps = {}
    def update(self, asset, candles):
        last_closed = candles[-2].timestamp
        if self.timestamps.get(asset) == last_closed:
            return indicator_cache.get(asset)
        self.timestamps[asset] = last_closed
        indicator = calculate_indicators(candles)
        indicator_cache.update(asset, indicator)
        return indicator

indicator_engine = IncrementalIndicatorEngine()

def _trend_ok(close) -> bool:
    """P4: devuelve True si el mercado NO está en tendencia fuerte (apto para
    la reversión a la media de GSR). Se bloquea cuando la separación entre una
    EMA rápida y una lenta, normalizada por la desviación estándar, es grande."""
    s = pd.Series(close)
    fast = s.ewm(span=TREND_FAST, adjust=False).mean()
    slow = s.ewm(span=TREND_SLOW, adjust=False).mean()
    sep = float((fast - slow).iloc[-1])
    sd = float(s.rolling(100).std().iloc[-1])
    if not np.isfinite(sd) or sd <= 0:
        return True   # sin volatilidad medible -> no bloquear
    return abs(sep) / sd < TREND_FILTER_Z


def calculate_indicators(candles) -> IndicatorSnapshot:
    # P2: trabajar SOLO con velas cerradas (descartar la vela aún en formación,
    # candles[-1]) para que BB/RSI/DAMOA y el body-outside apunten al MISMO cierre.
    closed = candles[:-1]
    close = np.array([x.close for x in closed], dtype=float)

    bb = ta.volatility.BollingerBands(pd.Series(close), window=20, window_dev=2)
    upper = bb.bollinger_hband().iloc[-1]
    middle = bb.bollinger_mavg().iloc[-1]
    lower = bb.bollinger_lband().iloc[-1]
    rsi_val = ta.momentum.RSIIndicator(pd.Series(close), window=14).rsi().iloc[-1]

    df_close = pd.Series(close)
    ema = df_close.ewm(span=5, adjust=False).mean()
    volatilidad_damoa = np.sqrt((df_close.diff() ** 2).rolling(5).mean())
    # P1: piso de volatilidad para que DAMOA no explote cuando la RMS tiende a 0,
    # y acotado a ±DAMOA_CLAMP para descartar outliers (ej. los 9564 del CSV).
    vol_piso = volatilidad_damoa.where(
        volatilidad_damoa > (df_close.abs() * DAMOA_VOL_FLOOR_REL),
        df_close.abs() * DAMOA_VOL_FLOOR_REL
    )
    damoa_serie = ((df_close - ema) / vol_piso) * 10
    damoa_val = float(np.clip(damoa_serie.fillna(0.0).iloc[-1], -DAMOA_CLAMP, DAMOA_CLAMP))

    indicator = IndicatorSnapshot(index=len(closed)-1)
    indicator.bb_upper = upper
    indicator.bb_middle = middle
    indicator.bb_lower = lower
    indicator.rsi = rsi_val
    indicator.damoa = damoa_val
    indicator.trend_ok = _trend_ok(close)

    current_candle = closed[-1]
    indicator.body = abs(current_candle.close - current_candle.open)
    indicator.outside = body_outside_percent(current_candle, indicator)
    return indicator

class IndicatorValidator:
    def validate(self, indicator):
        return (indicator is not None and
                indicator.rsi is not None and
                indicator.bb_upper is not None and
                indicator.bb_lower is not None and
                indicator.damoa is not None)

indicator_validator = IndicatorValidator()

def candle_color(candle):
    if candle.close > candle.open: return "GREEN"
    if candle.close < candle.open: return "RED"
    return "DOJI"

def body_outside_upper(candle, upper):
    body_top = max(candle.open, candle.close)
    body_bottom = min(candle.open, candle.close)
    body = body_top - body_bottom
    if body <= 0: return 0
    return (max(body_top - max(upper, body_bottom), 0) / body) * 100

def body_outside_lower(candle, lower):
    body_top = max(candle.open, candle.close)
    body_bottom = min(candle.open, candle.close)
    body = body_top - body_bottom
    if body <= 0: return 0
    return (max(min(lower, body_top) - body_bottom, 0) / body) * 100

def body_outside_percent(candle, indicator):
    return max(body_outside_upper(candle, indicator.bb_upper), body_outside_lower(candle, indicator.bb_lower))

# ==========================================================
# MÁQUINA DE ESTADOS Y REGLAS GSR (INTACTAS)
# ==========================================================
class GSRPhase(Enum):
    OBSERVANDO = 0
    PRIMERA_VELA = 1
    SEGUNDA_VELA = 2
    LISTA = 3
    ENTRADA = 4

@dataclass
class AssetState:
    asset: str
    phase: GSRPhase = GSRPhase.OBSERVANDO
    direction: str = ""
    progress: int = 0
    first_candle: int = 0
    second_candle: int = 0
    first_close: float = 0.0
    second_close: float = 0.0
    bb1: float = 0.0
    bb2: float = 0.0
    rsi: float = 50.0
    damoa: float = 0.0
    ready: bool = False
    signal_sent: bool = False
    updated: int = 0
    pattern_id: str = ""
    # P3: confirmación de reversión (espera de una vela que vaya en contra del
    # patrón) antes de disparar el mensaje al grupo.
    reversal_pending: bool = False
    confirm_deadline: int = 0

class GSRMemory:
    def __init__(self):
        self.assets = {}
    def get(self, asset) -> AssetState:
        if asset not in self.assets:
            self.assets[asset] = AssetState(asset=asset)
        return self.assets[asset]
    def reset(self, asset):
        self.assets[asset] = AssetState(asset=asset)

gsr_memory = GSRMemory()

class PatternEngine:
    def assign(self, state: AssetState):
        key = f"{state.asset}|{state.first_candle}|{state.second_candle}|{state.direction}"
        return hashlib.md5(key.encode()).hexdigest()

pattern_engine = PatternEngine()
pattern_history = set()

def detect_first_candle(asset, candle, indicator):
    state = gsr_memory.get(asset)
    if candle_color(candle) == "GREEN":
        outside = body_outside_upper(candle, indicator.bb_upper)
        if outside >= BODY_OUTSIDE:
            state.phase = GSRPhase.PRIMERA_VELA
            state.direction = "PUT"
            state.progress = 25
            state.first_candle = candle.timestamp
            state.first_close = candle.close
            gsr_events.add(asset, "PRIMERA_VELA", "PUT")
            return True
    elif candle_color(candle) == "RED":
        outside = body_outside_lower(candle, indicator.bb_lower)
        if outside >= BODY_OUTSIDE:
            state.phase = GSRPhase.PRIMERA_VELA
            state.direction = "CALL"
            state.progress = 25
            state.first_candle = candle.timestamp
            state.first_close = candle.close
            gsr_events.add(asset, "PRIMERA_VELA", "CALL")
            return True
    return False

def detect_second_candle(asset, candle, indicator):
    state = gsr_memory.get(asset)
    if state.phase != GSRPhase.PRIMERA_VELA or candle.timestamp == state.first_candle:
        return False

    if state.direction == "PUT":
        if candle_color(candle) != "GREEN":
            gsr_memory.reset(asset)
            return False
        outside = body_outside_upper(candle, indicator.bb_upper)
    else:
        if candle_color(candle) != "RED":
            gsr_memory.reset(asset)
            return False
        outside = body_outside_lower(candle, indicator.bb_lower)

    if outside < BODY_OUTSIDE:
        gsr_memory.reset(asset)
        return False

    state.phase = GSRPhase.SEGUNDA_VELA
    state.progress = 50
    state.second_candle = candle.timestamp
    state.second_close = candle.close
    state.pattern_id = pattern_engine.assign(state)
    gsr_events.add(asset, "SEGUNDA_VELA", state.direction)
    return True

class GSRReadyEngine:
    def confirm(self, asset, indicator):
        state = gsr_memory.get(asset)
        if state.phase != GSRPhase.SEGUNDA_VELA:
            return False

        # P4: filtro de tendencia. Si el mercado está en tendencia fuerte, la
        # ruptura de 2 velas tiende a CONTINUAR (no a revertir) -> se descarta.
        if not indicator.trend_ok:
            gsr_memory.reset(asset)
            return False

        # P5: filtro de sesión/liquidez. Solo se arma la reversión en horas líquidas
        # (evita el rollover y las velas erráticas de baja liquidez).
        if SESSION_FILTER_ENABLED and not is_liquid_session(int(broker_clock.now_ts())):
            gsr_memory.reset(asset)
            return False

        if state.direction == "PUT":
            if indicator.rsi < RSI_OVERBOUGHT or indicator.damoa < DAMOA_HIGH:
                gsr_memory.reset(asset)
                return False
        else:
            if indicator.rsi > RSI_OVERSOLD or indicator.damoa > DAMOA_LOW:
                gsr_memory.reset(asset)
                return False

        state.phase = GSRPhase.LISTA
        state.progress = 75
        state.ready = True
        state.rsi = indicator.rsi
        state.damoa = indicator.damoa
        # P3: entrar en espera de la vela que confirme la reversión.
        state.reversal_pending = True
        state.confirm_deadline = int(broker_clock.now_ts()) + REVERSAL_CONFIRM_WINDOW * 60
        gsr_events.add(asset, "LISTA", state.direction)
        return True

ready_engine = GSRReadyEngine()

def enviar_alerta_whatsapp(mensaje: str):
    """Envía una alerta al grupo vía el puente local (whatsapp_bridge).
    Si el puente está apagado, solo se registra un aviso (no rompe el escáner)."""
    if not WHATSAPP_BRIDGE_ENABLED:
        return
    try:
        import json as _json
        import urllib.request as _urllib
        payload = _json.dumps({"mensaje": mensaje}).encode("utf-8")
        req = _urllib.Request(
            WHATSAPP_BRIDGE_URL + "/enviar",
            data=payload,
            headers={"Content-Type": "application/json"}
        )
        with _urllib.urlopen(req, timeout=3) as resp:
            resp.read()
    except Exception as e:
        log.warning(f"[WA] Alerta al grupo no enviada (¿puente apagado?): {e}")


def validate_signal_eligibility(asset) -> tuple:
    """Devuelve (True, 'OK') solo si TODO es válido antes de emitir señal.
    Orden: conexión → disponibilidad → activo OPEN → velas actuales."""
    if not iq.is_connected():
        return False, "BROKER_DISCONNECTED"
    if not broker_market.valid():
        return False, "STALE_AVAILABILITY"
    if str(asset).upper().endswith("-OTC") and not broker_market.is_open(asset):
        return False, "ASSET_CLOSED"
    st = asset_manager.get_status(asset)
    if not st or not st.tradable or st.status != AssetStatusEnum.OPEN:
        return False, "ASSET_CLOSED"
    candles = market_feed.get(asset)
    if not candles:
        return False, "NO_CANDLE_DATA"
    last_ts = candles[-1].timestamp
    age = int(broker_clock.now_ts()) - int(last_ts)
    max_age = LIVE_WINDOW_OTC if str(asset).upper().endswith("-OTC") else LIVE_WINDOW_REAL
    if age > max_age:
        return False, "STALE_CANDLES"
    return True, "OK"


def fire_signal(asset, opening, indicator):
    state = gsr_memory.get(asset)
    if not state.ready or state.signal_sent:
        return
    if state.pattern_id in pattern_history:
        return

    # ===== DOBLE VALIDACIÓN ANTES DE EMITIR =====
    elegible, motivo = validate_signal_eligibility(asset)
    if not elegible:
        log.warning(f"[SIGNAL_BLOCKED] {format_asset_display_name(asset)} -> {motivo}")
        return

    with LOCK:
        SIGNALS.insert(0, Signal(
            asset=asset,
            direction=state.direction,
            timestamp=opening.timestamp,
            rsi=state.rsi,
            damoa=state.damoa,
            strength=75.0,
            message="GSR Síncrono Activado"
        ))
        state.signal_sent = True
        pattern_history.add(state.pattern_id)
        audit_logger.record(asset, state.direction, opening.timestamp, state.rsi, state.damoa)

        # === ALERTA AL GRUPO DE WHATSAPP (puente local) ===
        try:
            hora_utc = datetime.fromtimestamp(int(opening.timestamp), tz=TZ_UTC).strftime("%H:%M")
        except Exception:
            hora_utc = str(int(opening.timestamp))
        alerta = (
            f"⚡ SEÑAL GSR - {format_asset_display_name(asset)}\n"
            f"Dirección: {state.direction}\n"
            f"RSI: {state.rsi:.1f} | DAMOA: {state.damoa:.1f}\n"
            f"Hora (UTC): {hora_utc}\n"
            f"⚠️ Señal automática de estrategia. No es consejo financiero."
        )
        threading.Thread(target=enviar_alerta_whatsapp, args=(alerta,), daemon=True).start()

class GSREventEngine:
    def __init__(self):
        self.events = []
    def add(self, asset, phase, direction):
        self.events.append({
            "time": int(broker_clock.now_ts()),
            "asset": format_asset_display_name(asset),
            "phase": phase,
            "direction": direction
        })
        if len(self.events) > 200:
            self.events.pop(0)

gsr_events = GSREventEngine()

class ReasonEngine:
    def explain(self, indicator):
        reasons = []
        if indicator.outside < 40: reasons.append(f"BB {indicator.outside:.1f}%")
        if 20 < indicator.rsi < 80: reasons.append(f"RSI {indicator.rsi:.1f}")
        if abs(indicator.damoa) < 10: reasons.append(f"DAMOA {indicator.damoa:.1f}")
        return reasons

reason_engine = ReasonEngine()

class GSRConditionEngine:
    def evaluate(self, state, indicator):
        bb = indicator.outside >= 40
        if state.direction == "PUT":
            rsi = indicator.rsi >= 80
            damoa = indicator.damoa >= 10
        else:
            rsi = indicator.rsi <= 20
            damoa = indicator.damoa <= -10
        return {"bb": bool(bb), "rsi": bool(rsi), "damoa": bool(damoa)}

condition_engine = GSRConditionEngine()

class ProgressEngine:
    def calculate(self, row):
        value = 0
        if row["bb_ok"]: value += 40
        if row["rsi_ok"]: value += 30
        if row["damoa_ok"]: value += 30
        return value

progress_engine = ProgressEngine()

class GSRStage(Enum):
    SEARCH = 0
    FIRST = 1
    SECOND = 2
    BB_OK = 3
    RSI_OK = 4
    DAMOA_OK = 5
    READY = 6

class StageEngine:
    def calculate(self, row):
        if not row["bb_ok"]: return GSRStage.SECOND
        if not row["rsi_ok"]: return GSRStage.RSI_OK
        if not row["damoa_ok"]: return GSRStage.DAMOA_OK
        return GSRStage.READY

stage_engine = StageEngine()

class StageColor:
    COLORS = {
        "SEARCH": "#6b7280", "FIRST": "#3b82f6", "SECOND": "#f59e0b",
        "BB_OK": "#22c55e", "RSI_OK": "#06b6d4", "DAMOA_OK": "#a855f7",
        "READY": "#ef4444"
    }
    def get(self, stage):
        return self.COLORS.get(stage, "#999")

stage_color = StageColor()

# ==========================================================
# MOTOR DE PROXIMIDAD
# ==========================================================
class ProximityEngine:
    def __init__(self):
        self.rows = {}

    def update(self, asset, state, indicator):
        st = asset_manager.get_status(asset)
        asset_type = st.asset_type if st else "REAL"
        now_b = int(broker_clock.now_ts())
        age = now_b - st.last_candle_time if st and st.last_candle_time > 0 else 0

        score = 0
        score += min(indicator.outside, 40)
        if state.direction == "PUT":
            score += max(0, indicator.rsi - 50) * 0.6
            score += max(0, indicator.damoa)
        else:
            score += max(0, 50 - indicator.rsi) * 0.6
            score += abs(min(indicator.damoa, 0))

        conditions = condition_engine.evaluate(state, indicator)

        display_name = format_asset_display_name(asset)

        self.rows[asset] = {
            "asset": display_name,
            "copy_name": broker_search_name(asset),
            "raw_asset": asset,
            "type": asset_type,
            "status": "OPEN",
            "age": f"{age}s",
            "score": round(score, 2),
            "phase": state.phase.name,
            "direction": state.direction,
            "outside": round(indicator.outside, 2),
            "rsi": round(indicator.rsi, 2),
            "damoa": round(indicator.damoa, 2),
            "bb_ok": conditions["bb"],
            "rsi_ok": conditions["rsi"],
            "damoa_ok": conditions["damoa"],
            "reason": reason_engine.explain(indicator)
        }

        # Progreso/etapa HONESTOS: 'READY' (100%) solo cuando el patrón secuencial
        # está validado (fase LISTA). Antes se mostraba READY cuando una sola vela
        # coincidía con condiciones extremas (parecía señal completa sin serlo) ->
        # causaba 'señales falsas'. Las fases intermedias muestran su avance real.
        if state.phase in (GSRPhase.LISTA, GSRPhase.ENTRADA):
            stage = GSRStage.READY
            progress = 100
        elif state.phase == GSRPhase.PRIMERA_VELA:
            stage = GSRStage.FIRST
            progress = 25
        elif state.phase == GSRPhase.SEGUNDA_VELA:
            stage = GSRStage.SECOND
            progress = 50
        else:
            stage = GSRStage.SEARCH
            progress = 0
        self.rows[asset]["progress"] = progress
        self.rows[asset]["stage"] = stage.name
        self.rows[asset]["color"] = stage_color.get(stage.name)

    def top(self):
        # La tabla muestra SIEMPRE los 18 activos monitoreados (candidatos):
        # los abiertos con su %/etapa en vivo, y los cerrados/suspendidos por el
        # broker en gris (0%) al final. Ordenados de mayor a menor %/score.
        salida = []
        for code, _tipo in asset_manager.get_candidate_list():
            st = asset_manager.get_status(code)
            abierto = bool(st and st.tradable and st.status == AssetStatusEnum.OPEN)
            row = self.rows.get(code)

            if abierto and row is not None:
                fila = dict(row)
                fila["status"] = "OPEN"
            else:
                estado = st.status.name if st else "CLOSED"
                fila = {
                    "asset": format_asset_display_name(code),
                    "copy_name": broker_search_name(code),
                    "raw_asset": code,
                    "type": (st.asset_type if st else "REAL"),
                    "status": estado,
                    "age": "-",
                    "score": 0.0,
                    "phase": "OBSERVANDO",
                    "direction": "",
                    "outside": 0.0,
                    "rsi": 0.0,
                    "damoa": 0.0,
                    "bb_ok": False,
                    "rsi_ok": False,
                    "damoa_ok": False,
                    "reason": [],
                    "progress": 0,
                    "stage": "CERRADO",
                    "color": "#8b949e"
                }
            salida.append(fila)
        # Orden: primero los ABIERTOS por % de progreso (mayor→menor), luego score
        # como desempate, y al final los CERRADOS.
        return sorted(
            salida,
            key=lambda x: (
                0 if x.get("status") == "OPEN" else 1,
                -int(x.get("progress", 0)),
                -float(x.get("score", 0)),
                x.get("asset", "")
            )
        )

proximity_engine = ProximityEngine()


def _confirm_reversal(asset, candle, state) -> bool:
    """P3 reforzado: la reversión se confirma con una vela CERRADA que vaya EN
    CONTRA del patrón y que además ROMPA el cierre de la 2ª vela del patrón.
    Un PUT (2 velas verdes) exige una vela roja que cierre por debajo de la 2ª
    vela; un CALL exige una verde que cierre por encima. Así se descartan giros
    débiles (mini-velas contrarias) que provocaban señales falsas."""
    if state.direction == "PUT":
        if not (candle.close < candle.open):      # debe ser vela roja
            return False
        if state.second_close and candle.close >= state.second_close:
            return False                          # no rompió el cierre de la 2ª vela
        return True
    else:
        if not (candle.close > candle.open):      # debe ser vela verde
            return False
        if state.second_close and candle.close <= state.second_close:
            return False
        return True


# ==========================================================
# PIPELINE DE ESCANEO Y PROCESAMIENTO
# ==========================================================
class ScannerPipeline:
    def process_asset(self, asset):
        start_asset_time = time.perf_counter()
        candles = market_feed.get(asset)
        if not candle_validator.validate(candles): return

        previous = candles[-3]
        current = candles[-2]
        opening = candles[-1]

        if not new_candle.detect(asset, current): return

        state = gsr_memory.get(asset)
        indicator = indicator_engine.update(asset, candles)
        if indicator is None or not indicator_validator.validate(indicator): return

        if state.phase == GSRPhase.OBSERVANDO: detect_first_candle(asset, previous, indicator)
        elif state.phase == GSRPhase.PRIMERA_VELA: detect_second_candle(asset, current, indicator)
        elif state.phase == GSRPhase.SEGUNDA_VELA: ready_engine.confirm(asset, indicator)
        elif state.phase == GSRPhase.LISTA:
            # P3: esperar una vela que CONFIRME la reversión antes de disparar.
            if current.timestamp > state.second_candle and _confirm_reversal(asset, current, state):
                fire_signal(asset, opening, indicator)
            elif state.confirm_deadline and int(broker_clock.now_ts()) > state.confirm_deadline:
                gsr_memory.reset(asset)

        proximity_engine.update(asset, state, indicator)

        asset_latency = time.perf_counter() - start_asset_time
        if scan_metrics.avg_latency_per_asset == 0.0:
            scan_metrics.avg_latency_per_asset = asset_latency
        else:
            scan_metrics.avg_latency_per_asset = (scan_metrics.avg_latency_per_asset * 0.9) + (asset_latency * 0.1)

    def process(self):
        for asset in market_feed.active_assets():
            self.process_asset(asset)

pipeline = ScannerPipeline()

# ==========================================================
# CLIENTE CONECTOR IQ OPTION CON SINCRONIZACIÓN BIPLANA
# ==========================================================
class IQClient:
    def __init__(self):
        self.api = None
        self.connected = False
        self._candle_executor = ThreadPoolExecutor(max_workers=5)

    def connect(self):
        if not IQ_AVAILABLE or not IQ_EMAIL or not IQ_PASSWORD:
            log.error("❌ Credenciales ausentes en el entorno local.")
            return False
        try:
            log.info(f"[IQ] Intentando conectar a IQ Option con: {IQ_EMAIL}...")
            self.api = IQ_Option(IQ_EMAIL, IQ_PASSWORD)
            ok, reason = self.api.connect()
            if not ok:
                log.error(f"❌ Falló el apretón de manos con el bróker: {reason}")
                return False
            self.api.change_balance(IQ_MODE)
            time.sleep(2)
            self.connected = True
            log.info(f"✅ IQ Option enlazado correctamente al entorno: {IQ_MODE}")

            self.sync_clock()
            return True
        except Exception as e:
            log.error(f"❌ Excepción crítica al conectar: {e}")
            self.connected = False
            return False

    def sync_clock(self):
        if self.is_connected():
            try:
                server_ts = self.api.get_server_timestamp()
                if server_ts and isinstance(server_ts, (int, float)):
                    broker_clock.sync_with_broker(float(server_ts))
            except Exception as e:
                log.warning(f"⚠️ Error obteniendo server timestamp de IQ Option: {e}")

    def is_connected(self):
        if self.api:
            try:
                if not self.api.check_connect():
                    self.connected = False
            except Exception:
                self.connected = False
        else:
            self.connected = False
        return self.connected

    def get_candles(self, asset, interval=60, count=100, end_time=None):
        if self.is_connected():
            try:
                end = end_time if end_time else int(broker_clock.now_ts())
                future = self._candle_executor.submit(self.api.get_candles, asset, interval, count, end)
                raw = future.result(timeout=5)
                if raw and isinstance(raw, list):
                    res = []
                    for c in raw:
                        if isinstance(c, dict) and "from" in c:
                            res.append(
                                Candle(
                                    timestamp=c["from"],
                                    open=float(c["open"]),
                                    high=float(c["max"]),
                                    low=float(c["min"]),
                                    close=float(c["close"]),
                                    volume=float(c["volume"])
                                )
                            )
                    return res
            except Exception as e:
                log.debug(f"Retraso o lectura nula de velas para {asset}: {e}")
        return None

iq = IQClient()

# ==========================================================
# CandleStreamManager — REALTIME CANDLES (no get_candles en ciclo)
# ==========================================================
class CandleStreamManager:
    SIZE = 60      # M1
    MAXDICT = 250  # histórico que carga al suscribir

    def __init__(self):
        self.lock = threading.Lock()
        self.streams = set()
        self._stop = threading.Event()

    def start(self, asset) -> bool:
        try:
            fut = iq._candle_executor.submit(iq.api.start_candles_stream, asset, self.SIZE, self.MAXDICT)
            fut.result(timeout=25)
            with self.lock:
                self.streams.add(asset)
            log.info(f"[STREAM] iniciado para {asset}")
            return True
        except Exception as e:
            log.warning(f"[STREAM] no se pudo iniciar stream {asset}: {e}")
            return False

    def stop(self, asset):
        try:
            iq.api.stop_candles_stream(asset, self.SIZE)
        except Exception:
            pass
        with self.lock:
            self.streams.discard(asset)
        # Limpiar estado del activo cerrado (GSR, proximidad, velas, mercado)
        try:
            gsr_memory.reset(asset)
        except Exception:
            pass
        try:
            candle_cache.cache.pop(asset, None)
            market_feed.market.pop(asset, None)
        except Exception:
            pass
        try:
            proximity_engine.rows.pop(asset, None)
        except Exception:
            pass

    def get_realtime(self, asset):
        try:
            data = iq.api.get_realtime_candles(asset, self.SIZE)
        except Exception:
            return None
        if not isinstance(data, dict) or not data:
            return None
        res = []
        for _ts, c in sorted(data.items()):
            if isinstance(c, dict) and "from" in c:
                try:
                    res.append(Candle(
                        timestamp=c["from"],
                        open=float(c["open"]),
                        high=float(c["max"]),
                        low=float(c["min"]),
                        close=float(c["close"]),
                        volume=float(c["volume"])
                    ))
                except Exception:
                    continue
        return res or None

    def active_count(self):
        with self.lock:
            return len(self.streams)

    def reconcile(self, desired):
        with self.lock:
            current = set(self.streams)
        for asset in current - desired:
            self.stop(asset)
        for asset in desired - current:
            self.start(asset)

    def run(self):
        while not self._stop.is_set():
            try:
                if iq.is_connected() and broker_market.valid():
                    desired = set(asset_manager.all_tradable())
                    self.reconcile(desired)
            except Exception as e:
                log.warning(f"[STREAM] error de reconciliación: {e}")
            self._stop.wait(10)


candle_streams = CandleStreamManager()

# ==========================================================
# ConnectionManager — ÚNICA instancia IQ y reconexión controlada
# ==========================================================
# Un solo candado protege las operaciones sensibles de conexión para que
# NINGÚN hilo reconecte simultáneamente ni cree varios clientes IQ.
CONN_LOCK = threading.Lock()
BROKER_STATUS = "DISCONNECTED"   # CONNECTED / DISCONNECTED


def ensure_connected() -> bool:
    """Reconexión centralizada y controlada. Devuelve True si hay sesión."""
    global BROKER_STATUS
    if iq.is_connected():
        BROKER_STATUS = "CONNECTED"
        return True
    if not CONN_LOCK.acquire(blocking=False):
        # Otro hilo ya está reconectando: esperar, no duplicar.
        return False
    try:
        BROKER_STATUS = "DISCONNECTED"
        if iq.connect():
            BROKER_STATUS = "CONNECTED"
            return True
        return False
    except Exception as e:
        log.error(f"[CONN] Error en reconexión: {e}")
        BROKER_STATUS = "DISCONNECTED"
        return False
    finally:
        CONN_LOCK.release()

# ==========================================================
# BrokerMarketState — DISPONIBILIDAD OFICIAL DESDE IQ OPTION
# ==========================================================
# Fuente: get_all_init_v2()  (binary/turbo). Cada activo trae "enabled" e
# "is_suspended"; se marca OPEN solo si enabled && !is_suspended.
# Digital se AISLA: un fallo de digital no debe romper binary/turbo.
class BrokerMarketState:
    def __init__(self):
        self.lock = threading.Lock()
        self.status = "UNKNOWN"      # UNKNOWN / LIVE / STALE
        self.assets = {}             # nombre -> {'id','type','open','suspended','source'}
        self.updated_at = 0
        self.last_error = ""
        # Tolerancia: si un refresco del catálogo falla de forma transitoria
        # (STALE), seguimos usando el último snapshot conocido durante esta
        # ventana en lugar de cerrar TODO el mercado.
        self.stale_grace = 600.0

    def refresh(self):
        try:
            import concurrent.futures as _cf
            with _cf.ThreadPoolExecutor(max_workers=1) as ex:
                data = ex.submit(iq.api.get_all_init_v2).result(timeout=45)
        except Exception as e:
            self._mark_stale(str(e))
            return
        if not isinstance(data, dict):
            self._mark_stale("get_all_init_v2 no devolvió un diccionario")
            return

        parsed = {}
        for option in ("turbo", "binary"):
            node = data.get(option)
            if not isinstance(node, dict):
                continue
            actives = node.get("actives")
            if not isinstance(actives, dict):
                continue
            for _aid, v in actives.items():
                if not isinstance(v, dict):
                    continue
                name = str(v.get("name", ""))
                if "." in name:
                    name = name.split(".")[-1]
                name = name.upper()
                if not name:
                    continue
                enabled = bool(v.get("enabled", False))
                suspended = bool(v.get("is_suspended", False))
                parsed[name] = {
                    "id": _aid,
                    "type": option.upper(),
                    "open": enabled and not suspended,
                    "suspended": suspended,
                    "source": "IQ_OPTION",
                    "desc": str(v.get("description", "")),
                    "exchange": str(v.get("exchange", ""))
                }
                # Registra el id para poder pedir velas/suscribir streams
                try:
                    _IQ_ACTIVES_DICT[name] = int(_aid)
                except Exception:
                    pass

        if not parsed:
            self._mark_stale("no se obtuvo ningún activo de binary/turbo")
            return

        with self.lock:
            self.assets = parsed
            self.status = "LIVE"
            self.updated_at = int(time.time())
            self.last_error = ""
        abiertos = sum(1 for a in parsed.values() if a.get("open"))
        log.info(f"[BROKER] Disponibilidad LIVE: {len(parsed)} instrumentos ({abiertos} abiertos)")

    def _mark_stale(self, msg):
        with self.lock:
            self.status = "STALE" if self.assets else "UNKNOWN"
            self.last_error = msg
        log.warning(f"[BROKER] Disponibilidad {self.status}: {msg}")

    def get(self, asset):
        with self.lock:
            return self.assets.get(str(asset).upper())

    def is_open(self, asset):
        info = self.get(asset)
        return bool(info and info.get("open"))

    def valid(self):
        with self.lock:
            if self.status == "LIVE":
                return True
            # Degradación suave: un fallo transitorio del refresco NO debe
            # cerrar todo el mercado. Mientras tengamos un snapshot reciente,
            # la frescura de las velas decide el estado real de cada activo.
            if (self.status == "STALE" and self.assets
                    and (int(time.time()) - self.updated_at) < self.stale_grace):
                return True
            return False

broker_market = BrokerMarketState()


def _fmt_ultima_vela(asset):
    candles = market_feed.get(asset)
    if not candles:
        return "-"
    last = candles[-1]
    ts = datetime.fromtimestamp(int(last.timestamp), tz=TZ_UTC).strftime("%H:%M:%S")
    age = int(broker_clock.now_ts()) - int(last.timestamp)
    return f"{ts} / {age}s"


def run_broker_diagnostic():
    """PRUEBA DE REALIDAD: imprime exactamente lo que entrega el broker."""
    print("=" * 44)
    print("APPALÍ SCANNER")
    print("IQ OPTION BROKER DIAGNOSTIC")
    print("=" * 44)
    print(f"CONNECTION: {'OK' if iq.is_connected() else 'FAIL'}")
    print(f"ACCOUNT: {IQ_MODE}")
    try:
        server_ts = iq.api.get_server_timestamp()
    except Exception:
        server_ts = int(broker_clock.now_ts())
    try:
        server_ts = int(server_ts)
        print(f"SERVER_TIMESTAMP: {server_ts}")
        print(f"SERVER_TIME_UTC: {datetime.fromtimestamp(server_ts, tz=TZ_UTC).strftime('%H:%M:%S')}")
        print(f"COLOMBIA_TIME: {datetime.fromtimestamp(server_ts, tz=TZ_COLOMBIA).strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"SERVER_TIMESTAMP: ERROR {e}")
    print(f"AVAILABILITY: {broker_market.status}")
    print("AVAILABLE ASSETS (del broker):")
    print("asset | instrument | open | source | last_candle | candle_age")
    with broker_market.lock:
        items = sorted(broker_market.assets.items())
    for name, info in items[:60]:
        print(f"{name} | {info['type']} | {'OPEN' if info['open'] else 'CLOSED'} | {info['source']} | {_fmt_ultima_vela(name)}")
    print("=" * 44)
    sys.stdout.flush()


def _broker_state_worker():
    # Primer refresco al conectar; luego frecuencia CONTROLADA (no por ciclo).
    time.sleep(3)
    broker_market.refresh()
    if BROKER_DIAGNOSTIC:
        run_broker_diagnostic()
    while True:
        time.sleep(120)
        broker_market.refresh()

# ==========================================================
# CONTROLADOR DEL CICLO OPERATIVO (FAST LOOP & MARKET REFRESH)
# ==========================================================
class ScanController:
    def __init__(self):
        self.running = False
        self.cycles = 0
        self.scan_time = 0
        self.last_processed_minute = -1
        self.last_market_refresh = 0
        self.last_clock_sync = 0

    def execute(self):
        now_ts = broker_clock.now_ts()

        if not iq.is_connected():
            log.warning("[RECONEXIÓN] Canal WebSocket cerrado. Re-enlazando...")
            if not ensure_connected():
                return
            market_feed.refresh_market_sessions()
            return

        # Sincronizar el reloj con moderación (el endpoint varía +/-1 s y no se
        # necesita 2 veces por segundo). Se resincroniza también al reconectar.
        if now_ts - self.last_clock_sync >= 30:
            iq.sync_clock()
            self.last_clock_sync = now_ts

        if now_ts - self.last_market_refresh >= 60:
            log.info("[MARKET] Ejecutando refresco dinámico de sesiones de mercado...")
            market_feed.refresh_market_sessions()
            self.last_market_refresh = now_ts

        current_minute = int(now_ts) // 60
        if current_minute == self.last_processed_minute:
            return

        start = time.perf_counter()

        market_feed.fast_update_active()
        pipeline.process()

        scan_metrics.cycles += 1
        self.cycles = scan_metrics.cycles
        self.last_processed_minute = current_minute
        self.scan_time = round(time.perf_counter() - start, 3)

        log.info(f"[SCANNER] Ciclo #{self.cycles} completado en {self.scan_time}s | Activos vivos: {len(market_feed.active_assets())}")

    def loop(self):
        self.running = True
        while self.running:
            try:
                self.execute()
            except Exception as e:
                log.error(f"Error en el ciclo del scanner: {e}")
            time.sleep(0.5)

scan_controller = ScanController()

# ==========================================================
# INTERFAZ WEB PREMIUM CON MONITOR EN VIVO DE RELOJ
# ==========================================================
HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AppALÍ Pro Scanner - v7.5</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;800&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg: #090a0f;
    --card-bg: #11141d;
    --header-bg: #161a26;
    --border: #262c3d;
    --text: #c5cedb;
    --text-strong: #ffffff;
    --accent: #1f6feb;
    --green: #00ff88;
    --red: #ef4444;
    --font-display: 'Orbitron', sans-serif;
    --font-ui: 'Inter', sans-serif;
    --font-mono: 'JetBrains Mono', monospace;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--font-ui); padding-bottom: 40px; }
  header { background: var(--header-bg); padding: 16px 30px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); box-shadow: 0 4px 20px rgba(0,0,0,0.4); }
  .logo-area { display: flex; align-items: center; gap: 10px; }
  .logo-mark { color: var(--accent); font-size: 22px; text-shadow: 0 0 10px var(--accent); }
  .logo-text { font-family: var(--font-display); font-weight: 800; font-size: 19px; color: var(--text-strong); letter-spacing: 1px; }
  .logo-sub { font-family: var(--font-mono); font-size: 10px; color: #8b949e; letter-spacing: 2px; background: #1f2433; padding: 2px 6px; border-radius: 4px; }
  .version-tag { font-family: var(--font-mono); font-size: 11px; background: rgba(31,111,235,0.15); color: #58a6ff; padding: 4px 12px; border-radius: 20px; border: 1px solid rgba(31,111,235,0.3); }

  .clock-banner { background: #0d1117; border-bottom: 1px solid var(--border); padding: 12px 30px; display: flex; justify-content: space-around; align-items: center; font-family: var(--font-mono); font-size: 12px; }
  .clock-box { text-align: center; }
  .clock-label { color: #8b949e; font-size: 10px; text-transform: uppercase; margin-bottom: 2px; }
  .clock-val { color: var(--green); font-size: 15px; font-weight: 600; }

  .container { max-width: 1440px; margin: 25px auto; padding: 0 20px; display: grid; grid-template-columns: 1fr 340px; gap: 25px; }
  .card { background: var(--card-bg); border-radius: 12px; border: 1px solid var(--border); padding: 24px; box-shadow: 0 8px 32px rgba(0,0,0,0.5); }
  .card h3 { font-size: 13px; font-weight: 700; color: #8b949e; text-transform: uppercase; letter-spacing: 1.5px; margin-bottom: 20px; }
  table { width: 100%; border-collapse: collapse; text-align: left; }
  th { background: #171c28; padding: 14px 16px; font-size: 11px; font-weight: 600; color: #8b949e; text-transform: uppercase; border-bottom: 2px solid var(--border); }
  td { padding: 14px 16px; border-bottom: 1px solid var(--border); font-family: var(--font-mono); font-size: 13px; }
  .asset-name { font-family: var(--font-ui); font-weight: 700; color: var(--text-strong); cursor: pointer; user-select: all; padding: 4px 8px; border-radius: 4px; transition: all 0.2s ease; }
  .asset-name:hover { background: rgba(31,111,235,0.2); color: #58a6ff; }
  .type-tag { font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 600; margin-left: 6px; }
  .type-real { background: rgba(31,111,235,0.2); color: #58a6ff; }
  .type-otc { background: rgba(168,85,247,0.2); color: #c084fc; }

  .score-val { color: var(--green); font-weight: 600; }
  .phase-tag { font-family: var(--font-ui); font-weight: 700; font-size: 11px; padding: 4px 10px; border-radius: 6px; text-transform: uppercase; }
  .progress-wrapper { display: flex; align-items: center; gap: 10px; width: 130px; }
  .progress-bg { flex: 1; height: 6px; background: #1e2330; border-radius: 3px; overflow: hidden; }
  .progress-bar { height: 100%; background: linear-gradient(90deg, #1f6feb, #00ff88); }
  .reason-tag { color: #ffbc5e; background: rgba(255,188,94,0.07); padding: 3px 8px; border-radius: 4px; font-size: 11px; }

  .metric-item { margin-bottom: 18px; border-bottom: 1px solid #1c2130; padding-bottom: 14px; }
  .metric-label { font-size: 12px; color: #8b949e; margin-bottom: 6px; text-transform: uppercase; }
  .metric-value { font-family: var(--font-mono); font-size: 20px; font-weight: 600; color: var(--text-strong); }
  .metric-value span { color: var(--accent); }
</style>
</head>
<body>
<header>
  <div class="logo-area">
    <span class="logo-mark">⚡</span>
    <span class="logo-text">AppALÍ PRO</span>
    <span class="logo-sub">GSR SCANNER</span>
  </div>
  <div class="version-tag">PROD v7.5</div>
</header>

<div class="clock-banner">
  <div class="clock-box">
    <div class="clock-label">BROKER TIME</div>
    <div class="clock-val" id="clk_broker">00:00:00</div>
  </div>
  <div class="clock-box">
    <div class="clock-label">UTC TIME</div>
    <div class="clock-val" id="clk_utc">00:00:00</div>
  </div>
  <div class="clock-box">
    <div class="clock-label">COLOMBIA (COT)</div>
    <div class="clock-val" id="clk_colombia">00:00:00</div>
  </div>
  <div class="clock-box">
    <div class="clock-label">ESTADO SYNC</div>
    <div class="clock-val" id="clk_status" style="color: #58a6ff;">SYNCED</div>
  </div>
  <div class="clock-box">
    <div class="clock-label">ÚLTIMA SYNC</div>
    <div class="clock-val" id="clk_last_sync" style="color: #8b949e;">--:--:--</div>
  </div>
</div>

<div class="container">
  <div>
    <div class="card">
      <h3>🔥 Proximidades GSR de Mercado (Haz Clic en el Activo para Copiar)</h3>
      <table>
        <thead>
          <tr>
            <th>Activo</th>
            <th>Score</th>
            <th>Etapa</th>
            <th>BB</th>
            <th>RSI</th>
            <th>DAMOA</th>
            <th>Progreso GSR</th>
            <th>Filtro Faltante</th>
          </tr>
        </thead>
        <tbody id="proximity_v2_board">
          <tr><td colspan="8" style="text-align:center; color:#8b949e; padding: 30px;">Sincronizando feed de activos reales...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div>
    <div class="card">
      <h3>📊 Estado del Mercado</h3>
      <div class="metric-item">
        <div class="metric-label">Real Abiertos</div>
        <div class="metric-value" id="val_real_open" style="color: #58a6ff;">0</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">OTC Abiertos</div>
        <div class="metric-value" id="val_otc_open" style="color: #c084fc;">0</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Cerrados / Sin Datos</div>
        <div class="metric-value" id="val_closed" style="color: #8b949e;">0</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Stale</div>
        <div class="metric-value" id="val_stale" style="color: #f59e0b;">0</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Streams Activos</div>
        <div class="metric-value" id="val_streams" style="color: #06b6d4;">0</div>
      </div>

      <h3 style="margin-top:25px;">🔌 Estados del Sistema</h3>
      <div class="metric-item">
        <div class="metric-label">Broker</div>
        <div class="metric-value" id="st_broker" style="color:#8b949e;">—</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Disponibilidad</div>
        <div class="metric-value" id="st_availability" style="color:#8b949e;">—</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Velas</div>
        <div class="metric-value" id="st_candles" style="color:#8b949e;">—</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Scanner</div>
        <div class="metric-value" id="st_scanner" style="color:#8b949e;">—</div>
      </div>

      <h3 style="margin-top: 25px;">⚡ Telemetría del Engine</h3>
      <div class="metric-item">
        <div class="metric-label">Ciclos de CPU</div>
        <div class="metric-value" id="val_cycles">0</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Latencia Promedio</div>
        <div class="metric-value" id="val_avg_latency">0.0ms</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Cierre de Vela en</div>
        <div class="metric-value" id="val_timer">00<span>s</span></div>
      </div>
    </div>
  </div>
</div>

<script>
let ws = new WebSocket("ws://" + location.host + "/ws");

/* ===== Alerta sonora / visual de señales ===== */
let audioCtx = null;
let sonidoOn = true;
let senalesVistas = {};

function asegurarAudio() {
    if (!audioCtx) {
        try { audioCtx = new (window.AudioContext || window.webkitAudioContext)(); } catch (e) {}
    }
    if (audioCtx && audioCtx.state === 'suspended') { audioCtx.resume().catch(() => {}); }
}
['pointerdown', 'keydown', 'touchstart'].forEach(function (ev) {
    document.addEventListener(ev, asegurarAudio, { passive: true });
});
asegurarAudio();

function pitido(frec, dur, espera) {
    if (!sonidoOn || !audioCtx) return;
    try {
        const t0 = audioCtx.currentTime + (espera || 0);
        const osc = audioCtx.createOscillator();
        const gan = audioCtx.createGain();
        osc.type = 'sine';
        osc.frequency.value = frec;
        osc.connect(gan);
        gan.connect(audioCtx.destination);
        gan.gain.setValueAtTime(0.0001, t0);
        gan.gain.exponentialRampToValueAtTime(0.5, t0 + 0.02);
        gan.gain.exponentialRampToValueAtTime(0.0001, t0 + dur);
        osc.start(t0);
        osc.stop(t0 + dur + 0.05);
    } catch (e) {}
}
function alarmaPatronCompleto() {
    pitido(1100, 0.22, 0);
    pitido(1470, 0.22, 0.26);
    pitido(1760, 0.35, 0.52);
    try { if (navigator.vibrate) navigator.vibrate([220, 120, 220]); } catch (e) {}
}
function alertaNuevoActivo() {
    pitido(880, 0.14, 0);
    pitido(660, 0.14, 0.18);
}
function alternarSonido() {
    sonidoOn = !sonidoOn;
    asegurarAudio();
    const btn = document.getElementById('btnSonido');
    if (btn) {
        btn.innerText = sonidoOn ? '🔊 Sonido ON' : '🔇 Sonido OFF';
        btn.style.background = sonidoOn ? '#1e9e4a' : '#8b949e';
    }
}

function copyAsset(text, el) {
    navigator.clipboard.writeText(text).then(() => {
        const originalText = el.innerText;
        el.innerText = text + " ¡Copiado!";
        el.style.color = "#00ff88";
        setTimeout(() => {
            el.innerText = originalText;
            el.style.color = "";
        }, 1000);
    }).catch(err => {
        console.error('Error al copiar: ', err);
    });
}

ws.onmessage = (event) => {
    if (window.getSelection && window.getSelection().toString().length > 0) {
        return;
    }

    const d = JSON.parse(event.data);

    /* ===== Detección de señales para alertar ===== */
    if (d.proximity_v2 && d.proximity_v2.length > 0) {
        d.proximity_v2.forEach(function (row) {
            /* Señal COMPLETA = fase GSR LISTA o ENTRADA (cuando el motor dispara) */
            const patronCompleto = row.phase === 'LISTA' || row.phase === 'ENTRADA';
            /* Solo avanza la cuenta cuando un patrón GSR real entra en 1ª/2ª vela */
            const avancePatron = row.phase === 'PRIMERA_VELA' || row.phase === 'SEGUNDA_VELA';
            /* Firma estable (fase+etapa): evita sonar por cambios cosméticos del % */
            const firma = row.asset + '|' + row.phase + '|' + row.stage;
            if (senalesVistas[firma]) return;   // no repetir la misma señal
            senalesVistas[firma] = 1;
            if (Object.keys(senalesVistas).length > 400) senalesVistas = {};
            if (patronCompleto) {
                alarmaPatronCompleto();
                if ('Notification' in window && Notification.permission === 'granted') {
                    try {
                        new Notification('⚡ SEÑAL GSR: ' + row.asset, {
                            body: 'Patrón GSR COMPLETO · ' + row.score + ' pts · ' + row.type,
                            tag: row.asset
                        });
                    } catch (e) {}
                }
            } else if (avancePatron) {
                alertaNuevoActivo();
            }
        });
    }

    if (d.clock) {
        document.getElementById('clk_broker').innerText = d.clock.broker_time;
        document.getElementById('clk_utc').innerText = d.clock.utc_time;
        document.getElementById('clk_colombia').innerText = d.clock.colombia_time;
        document.getElementById('clk_status').innerText = d.clock.status;
        document.getElementById('clk_last_sync').innerText = d.clock.last_sync;
    }

    document.getElementById('val_real_open').innerText = d.status.real_open;
    document.getElementById('val_otc_open').innerText = d.status.otc_open;
    document.getElementById('val_closed').innerText = d.status.closed + d.status.no_data;
    document.getElementById('val_stale').innerText = d.status.stale;
    document.getElementById('val_streams').innerText = d.status.streams_active;

    document.getElementById('st_broker').innerText = d.status.broker;
    document.getElementById('st_availability').innerText = d.status.availability;
    document.getElementById('st_candles').innerText = d.status.candles;
    document.getElementById('st_scanner').innerText = d.status.scanner;

    document.getElementById('val_cycles').innerText = d.status.cycles;
    document.getElementById('val_timer').innerHTML = d.status.next_candle + "<span>s</span>";
    document.getElementById('val_avg_latency').innerText = d.status.avg_latency;

    const board = document.getElementById('proximity_v2_board');
    board.innerHTML = '';

    if (d.proximity_v2 && d.proximity_v2.length > 0) {
        d.proximity_v2.forEach(row => {
            const reasonText = row.status !== 'OPEN'
                ? (row.status || 'CERRADO')
                : (row.reason && row.reason.length > 0 ? row.reason.join(" | ") : "PATRÓN GSR COMPLETO");
            const typeClass = row.type === 'REAL' ? 'type-real' : 'type-otc';

            board.innerHTML += `
            <tr>
              <td>
                <span class="asset-name" onclick="copyAsset('${row.copy_name}', this)" title="Haz clic para copiar">${row.copy_name}</span>
                <span class="type-tag ${typeClass}">${row.type}</span>
              </td>
              <td class="score-val">${row.score} pts</td>
              <td><span class="phase-tag" style="color:${row.color}; background:${row.color}14; border:1px solid ${row.color}33;">${row.stage}</span></td>
              <td>${row.bb_ok ? "🟢" : "🔴"}</td>
              <td>${row.rsi_ok ? "🟢" : "🔴"}</td>
              <td>${row.damoa_ok ? "🟢" : "🔴"}</td>
              <td>
                <div class="progress-wrapper">
                  <div class="progress-bg"><div class="progress-bar" style="width:${row.progress}%"></div></div>
                  <span class="progress-text">${row.progress}%</span>
                </div>
              </td>
              <td><span class="reason-tag">${reasonText}</span></td>
            </tr>`;
        });
    } else {
        const openTotal = ((d.status && d.status.real_open) || 0) + ((d.status && d.status.otc_open) || 0);
        let emptyMsg;
        if (openTotal > 0) {
            emptyMsg = "Vigilando " + openTotal + " activos abiertos... la tabla se llena con sus porcentajes en cuanto el motor procesa sus velas.";
        } else {
            emptyMsg = "Sin instrumentos monitoreados en este instante (reconectando al broker). Se reintenta automáticamente.";
        }
        board.innerHTML = '<tr><td colspan="8" style="text-align:center; color:#8b949e; padding: 30px;">' + emptyMsg + '</td></tr>';
    }
};
</script>
<button id="btnSonido" onclick="alternarSonido()"
        style="position:fixed;bottom:16px;right:16px;z-index:999;background:#1e9e4a;color:#fff;border:none;border-radius:24px;padding:10px 16px;font-family:Montserrat,sans-serif;font-weight:700;font-size:13px;cursor:pointer;box-shadow:0 4px 14px rgba(0,0,0,.4);">🔊 Sonido ON</button>
<p style="position:fixed;bottom:64px;right:16px;z-index:999;margin:0;font-size:11px;color:#8b949e;background:rgba(0,0,0,.55);padding:4px 10px;border-radius:8px;pointer-events:none;">Haz clic en la página para activar el audio del navegador</p>
</body>
</html>
"""

# ==========================================================
# MOTOR ASGI NATIVO HTTP / WEBSOCKETS
# ==========================================================
class Application:
    def __init__(self):
        self.started = False
        self.thread = None
        self.websockets = set()

    def start_scanner(self):
        if self.started: return
        self.started = True
        self.thread = threading.Thread(target=scan_controller.loop, daemon=True)
        self.thread.start()

    async def ws_broadcast_loop(self):
        while True:
            if self.websockets:
                now_ts = broker_clock.now_ts()
                seconds_left = 60 - (int(now_ts) % 60)

                payload = {
                    "clock": broker_clock.get_telemetry(),
                    "status": {
                        "cycles": scan_controller.cycles,
                        "cycle_time": scan_controller.scan_time,
                        "next_candle": max(0, seconds_left),
                        "candles_processed": scan_metrics.candles_processed,
                        "real_open": scan_metrics.real_open_count,
                        "otc_open": scan_metrics.otc_open_count,
                        "closed": scan_metrics.closed_count,
                        "no_data": scan_metrics.no_data_count,
                        "stale": _count_stale(),
                        "streams_active": candle_streams.active_count(),
                        "avg_latency": f"{scan_metrics.avg_latency_per_asset * 1000:.1f}ms",
                        "broker": BROKER_STATUS,
                        "availability": broker_market.status,
                        "candles": _candles_state(),
                        "scanner": "RUNNING" if scan_controller.running else "PAUSED"
                    },
                    "proximity_v2": proximity_engine.top()
                }
                raw_data = json.dumps(payload)
                dead_ws = []
                # Iterar sobre una copia: evita "Set changed size during iteration"
                # cuando un cliente se conecta/desconecta durante el envío.
                for ws in list(self.websockets):
                    try:
                        await ws({"type": "websocket.send", "text": raw_data})
                    except Exception:
                        dead_ws.append(ws)
                for ws in dead_ws:
                    self.websockets.discard(ws)
            await asyncio.sleep(0.5)

APP = Application()

def _count_stale():
    return sum(1 for st in asset_manager.status_map.values()
               if getattr(st, "status", None) == AssetStatusEnum.STALE_DATA)


def _candles_state():
    now = int(broker_clock.now_ts())
    for a in market_feed.active_assets():
        c = market_feed.get(a)
        if c and (now - int(c[-1].timestamp)) <= LIVE_WINDOW_REAL:
            return "LIVE"
    return "STALE"

def iq_login_worker():
    try:
        ensure_connected()
    except Exception as e:
        log.error(f"Error durante la inicialización de sesión con el broker: {e}")
    finally:
        market_feed.refresh_market_sessions()
        threading.Thread(target=_broker_state_worker, daemon=True).start()
        threading.Thread(target=candle_streams.run, daemon=True).start()
        APP.start_scanner()

async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            message = await receive()
            if message['type'] == 'lifespan.startup':
                threading.Thread(target=iq_login_worker, daemon=True).start()
                asyncio.create_task(APP.ws_broadcast_loop())
                await send({'type': 'lifespan.startup.complete'})
            elif message['type'] == 'lifespan.shutdown':
                await send({'type': 'lifespan.shutdown.complete'})
                return

    if scope['type'] == 'http':
        path = scope['path']
        if path in ['/', '']:
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [[b'content-type', b'text/html; charset=utf-8']],
            })
            await send({'type': 'http.response.body', 'body': HTML.encode('utf-8')})
        elif path == '/api/proximity-v2':
            data = json.dumps(proximity_engine.top())
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [[b'content-type', b'application/json']],
            })
            await send({'type': 'http.response.body', 'body': data.encode('utf-8')})
        else:
            await send({
                'type': 'http.response.start',
                'status': 204 if path == '/favicon.ico' else 404,
                'headers': [],
            })
            await send({'type': 'http.response.body', 'body': b''})
        return

    if scope['type'] == 'websocket':
        await send({'type': 'websocket.accept'})
        APP.websockets.add(send)
        try:
            while True:
                msg = await receive()
                if msg.get('type') == 'websocket.disconnect':
                    break
        except Exception:
            pass
        finally:
            APP.websockets.discard(send)

# ==========================================================
# DIAGNÓSTICO E INICIALIZACIÓN
# ==========================================================
def print_initial_diagnostics():
    print("====================================================")
    print("  AppALÍ GSR Scanner - Diagnóstico de Inicialización ")
    print("====================================================")

    conn_status = "OK" if IQ_AVAILABLE else "NO_MODULE"
    print(f"BROKER CONNECTION : {conn_status}")
    print(f"BROKER CLOCK      : {broker_clock.status.value}")
    print(f"UTC CLOCK         : {broker_clock.now_utc().strftime('%H:%M:%S')}")
    print(f"COLOMBIA CLOCK    : {broker_clock.now_colombia().strftime('%H:%M:%S')}")
    print("====================================================\n")

if __name__ == "__main__":
    print_initial_diagnostics()
    uvicorn.run("app_ali:app", host=HOST, port=PORT, log_level="info", use_colors=False)