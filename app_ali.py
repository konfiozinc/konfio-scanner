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
PORT = int(os.environ.get("PORT", "8000"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
# OJO: si el modo no es exactamente REAL/PRACTICE/TOURNAMENT la librería ejecuta
# exit(1) dentro de change_balance(), y SystemExit NO lo captura 'except
# Exception': el escáner se cerraba sin más. Se normaliza aquí.
IQ_MODE = str(os.environ.get("IQ_ACCOUNT_TYPE", "PRACTICE")).strip().upper()
if IQ_MODE not in ("REAL", "PRACTICE", "TOURNAMENT"):
    IQ_MODE = "PRACTICE"

# --- Control de acceso al panel y al WebSocket ---
# El servidor escucha en 0.0.0.0: sin token, CUALQUIERA en la misma red puede
# abrir http://<tu-ip>:8000/ y ver las señales. Con el token puesto, las
# peticiones que no lo traigan se rechazan (WebSocket 4401 / HTTP 401).
# Las peticiones desde el propio equipo (127.0.0.1 / ::1) se permiten siempre,
# para que no puedas quedarte fuera de tu propio panel por olvidar el token.
#     WS_AUTH_TOKEN=<cadena larga aleatoria>   (en .env o en el entorno)
WS_AUTH_TOKEN = os.environ.get("WS_AUTH_TOKEN", "").strip()

# Tope del historial de patrones ya disparados (evita crecimiento sin límite en
# ejecuciones largas). Al superarlo se descartan los más antiguos: un patrón sólo
# se consulta en los minutos siguientes a formarse, así que podar lo viejo no
# puede provocar señales repetidas.
PATTERN_HISTORY_MAX = int(os.environ.get("PATTERN_HISTORY_MAX", "5000"))

# ==========================================================
# PUBLICACIÓN DE SEÑALES EN ALÍ BINARY OPTIONS
# ==========================================================
# Dos vías, ambas desactivadas hasta que se configuren (ver .env.example):
#   Opción A (recomendada) - Cloud Function 'postSignal':
#       APP_SIGNAL_URL + APP_SIGNAL_SECRET. El escáner hace POST y la función
#       escribe en Firestore. La service account NO vive en el escáner.
#   Opción B - escritura directa en Firestore con service account:
#       FIREBASE_SA_PATH + FIREBASE_PROJECT_ID.
# (La publicación se hace directamente en la app Alí Binary Options.)
APP_SIGNAL_URL = os.environ.get("APP_SIGNAL_URL", "").strip()
APP_SIGNAL_SECRET = os.environ.get("APP_SIGNAL_SECRET", "")

# Opción B (sin Cloud Functions): el scanner escribe directo en Firestore con
# una service account key. FIREBASE_SA_PATH = ruta al JSON de la clave.
# Vacío = desactivado.
FIREBASE_SA_PATH = os.environ.get("FIREBASE_SA_PATH", "")
FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "ali-binary-options")

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

# --- Umbral de ruptura del cuerpo fuera de banda, SEPARADO por tipo de activo ---
# Por defecto los dos valen 40.0, es decir el valor calibrado de la estrategia:
# este cambio NO altera la operativa tal como está. Se puede endurecer el OTC
# (mercado sintético, más falsos positivos) sin tocar el código:
#     set BODY_OUTSIDE_OTC=55      -> endurecer sólo OTC
#     set BODY_OUTSIDE_REAL=45     -> endurecer sólo REAL
BODY_OUTSIDE_REAL = float(os.environ.get("BODY_OUTSIDE_REAL", str(BODY_OUTSIDE)))
BODY_OUTSIDE_OTC = float(os.environ.get("BODY_OUTSIDE_OTC", str(BODY_OUTSIDE)))

# --- Filtro de volumen (OPCIONAL, desactivado por defecto) ---
# Medido sobre 1,6 M de velas M1 reales exportadas (exportar_velas.py):
#   REAL (-OP): el volumen ES REAL (todas las velas > 0; p. ej. AUDCAD-OP media
#               ~356, rango 125-2295). Aquí el filtro SÍ es viable.
#   OTC:        el volumen es 0 en TODAS las velas (24/24 activos).
# Por eso el filtro viene desactivado y, además, se auto-desactiva cuando detecta
# que el activo no publica volumen (los OTC) en lugar de bloquear sus señales.
VOLUME_FILTER_ENABLED = os.environ.get("VOLUME_FILTER_ENABLED", "0") == "1"
VOLUME_LOOKBACK = 20
VOLUME_MIN_REL = float(os.environ.get("VOLUME_MIN_REL", "1.0"))

# Umbral de referencia "estricto" para OTC: NO filtra nada, sólo sirve para que
# cada señal OTC quede registrada diciendo si ese umbral más exigente la habría
# descartado. Así se decide con datos si conviene activar BODY_OUTSIDE_OTC=55.
OTC_STRICT_REFERENCE = float(os.environ.get("OTC_STRICT_REFERENCE", "55"))

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

AUDIT_FILE = os.path.join(BASE_DIR, "signals_audit.csv")

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

# Bitácora rotativa a archivo: la consola se pierde al cerrar la ventana (y en
# Windows se recorta al llegar al límite de líneas), así que sin esto no queda
# rastro de lo ocurrido cuando algo falla por la noche. Se engancha al logger
# RAÍZ para que también capture los mensajes de la librería y de uvicorn.
LOG_FILE = os.path.join(BASE_DIR, "scanner.log")
LOG_MAX_BYTES = int(os.environ.get("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
LOG_BACKUPS = int(os.environ.get("LOG_BACKUPS", "5"))
# APPALI_NO_FILE_LOG=1 desactiva la bitácora a archivo: lo usan los tests para no
# mezclar sus señales simuladas con las reales en scanner.log.
if os.environ.get("APPALI_NO_FILE_LOG") == "1":
    log.info("[LOG] Bitácora a archivo desactivada (APPALI_NO_FILE_LOG=1).")
else:
    try:
        from logging.handlers import RotatingFileHandler
        _file_handler = RotatingFileHandler(
            LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8")
        _file_handler.setFormatter(logging.Formatter(
            "[%(asctime)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        _file_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(_file_handler)
        log.info(f"[LOG] Bitácora rotativa activa: {LOG_FILE} "
                 f"({LOG_MAX_BYTES // (1024 * 1024)} MB x {LOG_BACKUPS} copias)")
    except Exception as _log_err:            # nunca debe impedir que arranque
        log.warning(f"[LOG] No se pudo activar la bitácora a archivo: {_log_err}")

# ==========================================================
# ESTADO DE LA PUBLICACIÓN EN ALÍ BINARY OPTIONS (aviso del panel)
# ==========================================================
# El estado se deduce de la configuración (no hay proceso externo que sondear):
# aquí el estado se deduce de la configuración: si hay URL de Cloud Function o
# service account, las señales se publican; si no, no salen del escáner.
def _estado_app() -> dict:
    if APP_SIGNAL_URL:
        return {"state": "ON", "detail": f"Cloud Function: {APP_SIGNAL_URL}"}
    if FIREBASE_SA_PATH:
        return {"state": "ON", "detail": f"Firestore directo: {FIREBASE_PROJECT_ID}"}
    return {"state": "OFF",
            "detail": "sin APP_SIGNAL_URL ni FIREBASE_SA_PATH: las señales sólo "
                      "quedan en el log y en signals_audit.csv"}

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
# BLINDAJE DE LA LIBRERÍA iqoptionapi (parches en tiempo de ejecución)
# ==========================================================
# La librería clásica (7.x) espera los mensajes del websocket con bucles SIN
# timeout y SIN sleep, por ejemplo:
#
#   get_candles()        ->  while self.check_connect and candles_data == None: pass
#   get_all_init_v2()    ->  while ... == None:  (gira 30 s al 100% de CPU)
#   connect()            ->  while global_value.balance_id == None: pass
#   get_profile_ansyc()  ->  while self.api.profile.msg == None: pass
#
# Si el socket deja de responder, esos bucles NO TERMINAN NUNCA. Cada llamada
# deja un hilo girando al 100% de CPU para siempre; al agotarse los 5 hilos del
# pool TODAS las lecturas de velas empiezan a expirar y los 40 activos pasan a
# NO_DATA de forma permanente (es exactamente el fallo de scanner_run_err.log:
# "-> NO_DATA" en todos los activos y "Ciclo completado en 0.0s" sin fin).
#
# Estos parches los sustituyen por esperas con timeout que ceden CPU. Son el
# arreglo de raíz: sin esto ningún reintento posterior recupera el escáner.
IQ_CANDLE_TIMEOUT = float(os.environ.get("IQ_CANDLE_TIMEOUT", "8"))
IQ_INIT_TIMEOUT = float(os.environ.get("IQ_INIT_TIMEOUT", "35"))
IQ_STREAM_TIMEOUT = float(os.environ.get("IQ_STREAM_TIMEOUT", "5"))
IQ_PROFILE_TIMEOUT = float(os.environ.get("IQ_PROFILE_TIMEOUT", "15"))
IQ_CONNECT_TIMEOUT = float(os.environ.get("IQ_CONNECT_TIMEOUT", "60"))

# Vigilancia del bucle del escáner. El bucle NO debe hacer nunca trabajo de red:
# si lo hace, se queda bloqueado y el panel se congela ("se detiene solo").
STALL_WARN = float(os.environ.get("STALL_WARN", "120"))      # avisar si el latido se atrasa
STALL_RECOVER = float(os.environ.get("STALL_RECOVER", "300"))  # renovar sesión si persiste
HEARTBEAT_LOG = float(os.environ.get("HEARTBEAT_LOG", "300"))  # latido informativo

# Cuenta cuántos hilos se han quedado abandonados girando dentro de la librería.
# Si se supera el tope se deja de reconectar: preferimos un escáner detenido y
# avisando, antes que una máquina al 100% de CPU sin poder recuperarse.
IQ_ABANDONED_CALLS = 0
IQ_ABANDONED_MAX = 3
IQ_ABANDONED_LOCK = threading.Lock()

# La librería guarda la respuesta de velas en UN ÚNICO campo compartido
# (api.candles.candles_data): si dos hilos piden velas a la vez, se pisan la
# respuesta. Este candado serializa las peticiones de velas (histórico y
# stream) para que cada una reciba su propia respuesta.
IQ_CANDLES_LOCK = threading.Lock()


def _harden_iqoptionapi() -> bool:
    """Sustituye los bucles infinitos de la librería por versiones con timeout."""
    if not IQ_AVAILABLE:
        return False
    try:
        import iqoptionapi.constants as _iq_const
        import iqoptionapi.global_value as _iq_global
    except Exception as exc:
        log.warning(f"No se pudo blindar iqoptionapi: {exc}")
        return False

    def _safe_get_candles(self, ACTIVES, interval, count, endtime, _timeout=None):
        timeout = IQ_CANDLE_TIMEOUT if _timeout is None else float(_timeout)
        # Serializado: api.candles.candles_data es un ÚNICO campo compartido;
        # dos peticiones simultáneas se pisarían la respuesta entre sí.
        with IQ_CANDLES_LOCK:
            try:
                self.api.candles.candles_data = None
            except Exception:
                return None
            try:
                if ACTIVES not in _iq_const.ACTIVES:
                    log.debug(f"[LIB] {ACTIVES} no está en consts.ACTIVES")
                    return None
                try:
                    endtime = int(endtime)
                except Exception:
                    endtime = int(time.time())
                self.api.getcandles(_iq_const.ACTIVES[ACTIVES], interval, count, endtime)
            except Exception as exc:
                log.debug(f"[LIB] getcandles({ACTIVES}) falló: {exc}")
                return None
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    data = self.api.candles.candles_data
                except Exception:
                    data = None
                if data is not None:
                    return data
                time.sleep(0.05)          # cede CPU (antes: 'pass' al 100% de CPU)
        log.warning(f"[LIB] timeout de {timeout:.0f}s esperando velas de {ACTIVES}")
        return None

    def _safe_get_all_init_v2(self):
        try:
            self.api.api_option_init_all_result_v2 = None
        except Exception:
            return None
        try:
            self.api.get_api_option_init_all_v2()
        except Exception as exc:
            log.debug(f"[LIB] get_api_option_init_all_v2 falló: {exc}")
            return None
        start_t = time.time()
        while time.time() - start_t < IQ_INIT_TIMEOUT:
            try:
                if self.api.api_option_init_all_result_v2 is not None:
                    return self.api.api_option_init_all_result_v2
            except Exception:
                pass
            time.sleep(0.1)           # cede CPU (antes: bucle vacío 30 s)
        log.warning(f"[LIB] get_all_init_v2 sin respuesta en {IQ_INIT_TIMEOUT:.0f}s")
        return None

    def _safe_get_profile_ansyc(self, _timeout=None):
        timeout = IQ_PROFILE_TIMEOUT if _timeout is None else float(_timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if self.api.profile.msg is not None:
                    return self.api.profile.msg
            except Exception:
                return None
            time.sleep(0.05)          # cede CPU
        log.warning(f"[LIB] get_profile_ansyc sin respuesta en {timeout:.0f}s")
        return None

    def _safe_full_realtime_get_candle(self, ACTIVE, size, maxdict):
        candles = _safe_get_candles(self, ACTIVE, size, maxdict, int(time.time()))
        if not candles:
            return
        for can in candles:
            try:
                if "from" in can:
                    self.api.real_time_candles[str(ACTIVE)][int(size)][can["from"]] = can
            except Exception:
                continue

    def _safe_start_candles_one_stream(self, ACTIVE, size):
        key = str(ACTIVE) + "," + str(size)
        if key not in self.subscribe_candle:
            self.subscribe_candle.append(key)
        try:
            self.api.candle_generated_check[str(ACTIVE)][int(size)] = {}
        except Exception:
            pass
        start = time.time()
        while time.time() - start <= IQ_STREAM_TIMEOUT:
            try:
                if self.api.candle_generated_check[str(ACTIVE)][int(size)] == True:
                    return True
            except Exception:
                pass
            try:
                self.api.subscribe(_iq_const.ACTIVES[ACTIVE], size)
            except Exception as exc:
                log.debug(f"[LIB] subscribe({ACTIVE}) falló: {exc}")
                return False
            time.sleep(1)
        log.warning(f"[LIB] stream no confirmado en {IQ_STREAM_TIMEOUT:.0f}s: {ACTIVE}")
        return False

    def _safe_start_candles_stream(self, ACTIVE, size, maxdict):
        try:
            if size == "all":
                for s in self.size:
                    _safe_full_realtime_get_candle(self, ACTIVE, s, maxdict)
                    self.api.real_time_candles_maxdict_table[ACTIVE][s] = maxdict
                return _safe_start_candles_one_stream(self, ACTIVE, 60)
            if size not in self.size:
                log.error(f"[LIB] size inválido en start_candles_stream: {size}")
                return False
            self.api.real_time_candles_maxdict_table[ACTIVE][size] = maxdict
            _safe_full_realtime_get_candle(self, ACTIVE, size, maxdict)
            return _safe_start_candles_one_stream(self, ACTIVE, size)
        except Exception as exc:
            log.debug(f"[LIB] start_candles_stream({ACTIVE}) falló: {exc}")
            return False

    def _no_resubscribe(self):
        """El escáner es el único dueño de las suscripciones (CandleStreamManager):
        la librería NO debe re-suscribir de forma síncrona dentro de connect()
        (bloqueaba hasta 20 s por activo y congelaba la reconexión)."""
        self.subscribe_candle = []

    try:
        IQ_Option.get_candles = _safe_get_candles
        IQ_Option.get_all_init_v2 = _safe_get_all_init_v2
        IQ_Option.get_profile_ansyc = _safe_get_profile_ansyc
        IQ_Option.full_realtime_get_candle = _safe_full_realtime_get_candle
        IQ_Option.start_candles_one_stream = _safe_start_candles_one_stream
        IQ_Option.start_candles_stream = _safe_start_candles_stream
        IQ_Option.re_subscribe_stream = _no_resubscribe
    except Exception as exc:
        log.error(f"No se pudieron aplicar los parches a iqoptionapi: {exc}")
        return False

    # connect() espera 'balance_id' con un bucle vacío infinito. Se deja un
    # marcador no-None para que no pueda colgarse; change_balance() lo
    # sustituye por el id real en cuanto llega el perfil.
    try:
        if getattr(_iq_global, "balance_id", None) is None:
            _iq_global.balance_id = 0
    except Exception:
        pass
    return True


IQ_LIB_HARDENED = _harden_iqoptionapi()


def _patch_ws_callbacks() -> bool:
    """Compatibilidad de iqoptionapi con websocket-client 1.x.

    iqoptionapi se escribió para websocket-client 0.56. En 0.56, si el callback
    era un MÉTODO ligado se invocaba sólo con el mensaje; en 1.x se invoca
    SIEMPRE como callback(app_ws, *args). Consecuencias reales observadas:

      - on_message(self, message) recibía (ws, data) -> TypeError en CADA
        mensaje. El socket conectaba ("Websocket connected") pero no procesaba
        ni un solo mensaje: sin velas, sin catálogo y sin balance. El panel
        mostraba todo en CLOSED y el escáner parecía 'conectado pero vacío'.
      - on_close(wss) recibía (ws, code, reason) -> TypeError al cerrar, con lo
        que check_websocket_if_connect se quedaba en 1 y el escáner creía seguir
        conectado con el socket ya muerto.

    Se envuelven los cuatro callbacks para aceptar ambas convenciones."""
    try:
        from iqoptionapi.ws.client import WebsocketClient as _WSC
        from websocket import WebSocketApp as _WSA
    except Exception as exc:
        log.warning(f"No se pudieron ajustar los callbacks del websocket: {exc}")
        return False
    if getattr(_WSC, "_appali_ws_patched", False):
        return True

    import inspect

    def _tolerant(name):
        raw = _WSC.__dict__.get(name)
        original = getattr(_WSC, name)
        # Las dos convenciones de la librería:
        #  - on_message es un MÉTODO normal: se recibe ligado, así que con
        #    websocket-client 1.x llega un argumento extra (la instancia de
        #    WebSocketApp) que hay que descartar.
        #  - on_open / on_error / on_close son @staticmethod: reciben el socket
        #    como primer argumento LEGÍTIMO, igual que en 0.56. Aquí no se
        #    descarta nada; sólo se recortan argumentos añadidos después
        #    (1.x llama on_close(ws, code, reason) y la firma espera 1).
        is_static = isinstance(raw, staticmethod)
        try:
            params = [p for p in inspect.signature(original).parameters.values()
                      if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
            max_args = len(params)
        except (TypeError, ValueError):
            max_args = None

        def wrapper(*args, **kwargs):
            if not is_static:
                for idx in (1, 0):
                    if len(args) > idx and isinstance(args[idx], _WSA):
                        args = args[:idx] + args[idx + 1:]
                        break
            if max_args is not None and len(args) > max_args:
                args = args[:max_args]
            return original(*args, **kwargs)

        wrapper.__name__ = name
        setattr(_WSC, name, staticmethod(wrapper) if is_static else wrapper)

    for _name in ("on_message", "on_close", "on_open", "on_error"):
        try:
            _tolerant(_name)
        except Exception as exc:
            log.debug(f"No se pudo envolver {_name}: {exc}")
    _WSC._appali_ws_patched = True
    return True


IQ_WS_PATCHED = _patch_ws_callbacks()

# La librería clásica deja el logger 'websocket' en DEBUG: eso inunda la consola
# con cada trama enviada y recibida. Se sube a WARNING (WEBSOCKET_DEBUG=1 para
# volver a verlo).
_ws_logger = logging.getLogger("websocket")
if os.environ.get("WEBSOCKET_DEBUG") == "1":
    _ws_logger.setLevel(logging.DEBUG)
else:
    _ws_logger.setLevel(logging.WARNING)

if IQ_AVAILABLE and IQ_LIB_HARDENED:
    log.info("[LIB] iqoptionapi blindada: esperas con timeout activadas.")
if IQ_WS_PATCHED:
    log.info("[LIB] callbacks del websocket adaptados a websocket-client 1.x.")


def call_with_timeout(fn, timeout, *args, **kwargs):
    """Ejecuta una llamada bloqueante de la librería en un hilo daemon con
    límite de tiempo. Si expira, el hilo se abandona (no se puede matar en
    Python) pero queda contabilizado: si se acumulan demasiados, el escáner
    deja de reconectar en vez de degradar la máquina entera."""
    global IQ_ABANDONED_CALLS
    box = {}

    def _runner():
        try:
            box["result"] = fn(*args, **kwargs)
        except BaseException as exc:      # incluye SystemExit de la librería
            box["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True,
                              name=f"iqcall-{getattr(fn, '__name__', 'call')}")
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        with IQ_ABANDONED_LOCK:
            IQ_ABANDONED_CALLS += 1
            abandoned = IQ_ABANDONED_CALLS
        log.error(f"[LIB] '{getattr(fn, '__name__', 'call')}' excedió {timeout}s "
                  f"y quedó colgada (llamadas colgadas: {abandoned})")
        raise TimeoutError(f"{getattr(fn, '__name__', 'call')} excedió {timeout}s")
    if "error" in box:
        raise box["error"]
    return box.get("result")

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

# Desfase máximo aceptable entre el reloj del broker y el del equipo. El reloj
# local está sincronizado por NTP y las velas de IQ Option vienen en epoch UTC,
# así que un desfase mayor que esto NO es real: significa que la librería nos
# está dando un 'timesync' viejo (su hilo lector se quedó atrás). Aceptarlo
# envenenaba todas las edades de vela y con ellas el filtro de activos vivos.
CLOCK_MAX_SKEW = float(os.environ.get("CLOCK_MAX_SKEW", "30"))
CLOCK_SANE_EPOCH_MIN = 1_600_000_000   # 2020-09-13
CLOCK_SANE_EPOCH_MAX = 2_200_000_000   # 2039-09-07
# Cada cuánto se resincroniza el reloj del broker y con cuántas muestras.
# Dos muestras separadas 0,3 s reducen el ruido de latencia de red (medido:
# +1,058 s y +0,758 s en dos lecturas seguidas del mismo instante).
CLOCK_SYNC_INTERVAL = float(os.environ.get("CLOCK_SYNC_INTERVAL", "30"))
CLOCK_SAMPLES = int(os.environ.get("CLOCK_SAMPLES", "2"))
CLOCK_SAMPLE_GAP = 0.3

class BrokerClock:
    """Fuente Única Centralizada de Tiempo para todo el Scanner."""
    def __init__(self):
        self._offset: float = 0.0  # broker_time - local_time
        self._last_sync_local: float = 0.0
        self.status: ClockStatus = ClockStatus.LOCAL_FALLBACK
        self._lock = threading.Lock()
        self._rejects = 0
        self._last_warn = 0.0

    def sync_with_broker(self, broker_timestamp) -> bool:
        """Acepta el reloj del broker SOLO si es plausible. Devuelve True si se
        aplicó. Un valor absurdo se rechaza y se sigue con el reloj local."""
        try:
            ts = float(broker_timestamp)
        except (TypeError, ValueError):
            return False

        now_local = time.time()
        # Sin mensaje 'timeSync' la librería devuelve time.time()/1000 ≈ 1.78e6
        # (año 1970). Un desfase así rompería todo el escáner.
        if not (CLOCK_SANE_EPOCH_MIN < ts < CLOCK_SANE_EPOCH_MAX):
            self._reject(f"timestamp no plausible ({ts:.0f})")
            return False

        offset = ts - now_local
        if abs(offset) > CLOCK_MAX_SKEW:
            self._reject(f"desfase de {offset:.1f}s > {CLOCK_MAX_SKEW:.0f}s")
            return False

        with self._lock:
            self._offset = offset
            self._last_sync_local = now_local
            self.status = ClockStatus.SYNCED
        log.info(f"[CLOCK] Reloj del broker sincronizado. Offset: {offset:+.3f}s")
        return True

    def _reject(self, motivo: str):
        """Descarta una sincronización inválida sin inundar el log."""
        with self._lock:
            self._rejects += 1
            rejects = self._rejects
            warn = (time.time() - self._last_warn) > 60
            if warn:
                self._last_warn = time.time()
        if warn:
            log.warning(f"[CLOCK] Sincronización del broker rechazada ({motivo}). "
                        f"Se mantiene el reloj local. Rechazos: {rejects}")

    def reset_to_local(self):
        """Vuelve al reloj local (p. ej. tras perder la conexión)."""
        with self._lock:
            self._offset = 0.0
            self.status = ClockStatus.LOCAL_FALLBACK

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
            "last_sync": self.last_sync_time_str(),
            "offset": round(self._offset, 3)
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
        # Universo: cuántos activos del universo curado tienen contrato de
        # 1 minuto y cuántos se descartaron por no tenerlo (dato del broker).
        self.assets_1min = 0
        self.assets_no_1min = 0

scan_metrics = ScanMetrics()

# ==========================================================
# AUDITORÍA LOCAL DE SEÑALES (CSV)
# ==========================================================
class SignalAuditLogger:
    """Registro CSV de señales para poder AUDITAR cada disparo a posteriori.

    Incluye el motivo exacto (BB %, umbral aplicado, RSI, DAMOA y volumen), que
    es lo que permite decidir con datos si conviene endurecer el umbral de OTC.
    Si el CSV viene de una versión anterior (menos columnas) se reescribe con la
    cabecera nueva rellenando las filas viejas con vacíos: no se pierde el
    histórico ni se desalinean las columnas.
    """
    COLUMNS = ["Timestamp_UTC", "Fecha_Hora_COT", "Activo", "Direccion", "RSI", "DAMOA",
               "Tipo", "BB_pct", "Umbral_BB", "Volumen", "Vol_medio20", "Vol_rel"]

    def __init__(self, filename=AUDIT_FILE):
        self.filename = filename
        self._initialize_csv()

    def _initialize_csv(self):
        if not os.path.exists(self.filename):
            with open(self.filename, mode='w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(self.COLUMNS)
            return
        try:
            with open(self.filename, mode='r', newline='', encoding='utf-8') as f:
                filas = list(csv.reader(f))
        except Exception:
            return
        if not filas or filas[0] == self.COLUMNS:
            return
        ancho = len(self.COLUMNS)
        migradas = [self.COLUMNS]
        for fila in filas[1:]:
            if not fila:
                continue
            if len(fila) < ancho:          # fila antigua: se rellena por la derecha
                fila = fila + [""] * (ancho - len(fila))
            migradas.append(fila)          # nunca se recorta información
        try:
            with open(self.filename, mode='w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerows(migradas)
            log.info(f"[AUDIT] {self.filename}: cabecera actualizada a "
                     f"{ancho} columnas ({len(migradas) - 1} filas conservadas).")
        except Exception as e:
            log.error(f"[AUDIT] no se pudo migrar el CSV: {e}")

    def record(self, asset, direction, timestamp, rsi, damoa, detalle=None):
        try:
            detalle = detalle or {}
            display_name = format_asset_display_name(asset)
            dt_col = datetime.fromtimestamp(timestamp, tz=TZ_COLOMBIA)
            fecha_str = dt_col.strftime('%Y-%m-%d %H:%M:%S')
            fila = [timestamp, fecha_str, display_name, direction, round(rsi, 2), round(damoa, 2),
                    detalle.get("tipo", ""), detalle.get("bb_pct", ""), detalle.get("umbral_bb", ""),
                    detalle.get("volumen", ""), detalle.get("vol_medio", ""), detalle.get("vol_rel", "")]
            with open(self.filename, mode='a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(fila)
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

# NOTA: aquí había una constante MASTER_CANDIDATE_PAIRS con 10 pares fijos que
# NO se usaba en ninguna parte (código muerto). Se eliminó porque inducía a
# error: el universo real lo define COMMON_PAIR_RANK + el catálogo del broker.
# Si algún día hiciera falta forzar una lista, se puede hacer sin tocar código:
#     set MAX_UNIVERSE=30     -> recorta el universo a los 30 pares de más rango

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
# Solo los más operados (~40 activos entre REAL -OP y OTC). Se retiraron los
# menos usados (PLN, PHP, CLP, THB, HKD, SGD y cruces menores GBP/NZD, NZD/CAD,
# NZD/CHF, GBP/CAD). Ordenados por relevancia; prioridad si hay que recortar.
#
# IMPORTANTE: esta lista NO decide qué es operable a 1 minuto. Eso lo decide el
# propio broker con 'expiration_times' (ver get_candidate_list): comprobado con
# diagnostico_broker.py, el 89% del catálogo (249 de 278) sí ofrece 60 s, y los
# que no (p. ej. USD/CHF OP: 120/180/300) se descartan automáticamente. Una
# lista fija de pares se quedaría obsoleta y añadiría/quitaría activos que el
# broker sí/no permite operar.
COMMON_PAIR_RANK = {code: i for i, code in enumerate([
    # 1) Pares mayores (los más operados)
    "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    # 2) Mercados emergentes / LatAm (relevantes para este proyecto)
    "USDCOP", "USDBRL", "USDMXN", "USDZAR", "USDTRY", "USDINR",
    # 3) Cruces principales
    "EURJPY", "EURGBP", "GBPJPY", "EURCHF", "AUDJPY", "EURAUD", "EURCAD",
    "GBPCHF", "CADJPY", "CHFJPY", "AUDCAD", "AUDNZD", "NZDJPY", "EURNZD",
    "GBPAUD",
])}
# Tope de activos monitoreados a la vez. Medido en producción: 37 activos vivos
# con ciclos de 0,15 s y 35+ streams estables, así que 40 no supone problema.
# Es configurable por si se quiere recortar o ampliar:
#     set MAX_UNIVERSE=30
# OJO: NO conviene repartir los activos en lotes alternos por ciclo. El patrón
# GSR exige velas de 1 minuto CONSECUTIVAS; si un activo se saltara ciclos, sus
# velas tendrían huecos y el patrón no podría completarse nunca.
MAX_UNIVERSE = int(os.environ.get("MAX_UNIVERSE", "40"))

# Cada cuánto se revalida el universo con el broker (segundos). Los porcentajes
# del tablero NO dependen de esto: se refrescan cada ~4 s con la vela en
# formación (ver ScanController.refresh_proximity_live).
MARKET_REFRESH_INTERVAL = float(os.environ.get("MARKET_REFRESH_INTERVAL", "60"))

# Exclusiones explícitas de la variante REAL (-OP) que en la app de IQ Option
# NO se puede operar a 1 minuto (aunque el catálogo las liste con 60 s):
# se deja la variante OTC del mismo par (que sí opera a 1 min).
FORBIDDEN_REAL_M1 = {
    "EURCHF-OP", "USDCHF-OP", "USDBRL-OP", "NZDJPY-OP",
}

class LiveAssetManager:
    """Gestor Dinámico de Activos."""
    def __init__(self):
        self.active_tradable_assets: List[str] = []
        self.status_map: Dict[str, AssetTradingStatus] = {}
        self._lock = threading.Lock()

    def get_candidate_list(self) -> List[Tuple[str, str]]:
        # UNIVERSO CURADO desde el snapshot del broker: solo los pares más
        # comunes (COMMON_PAIR_RANK), REAL (-OP) y OTC (-OTC), con tope ~40.
        # Reduce streams y carga sobre la conexión.
        #
        # Si el catálogo del broker todavía no está disponible se reconstruye la
        # lista desde las constantes de la librería: antes se devolvía [] y el
        # escáner se quedaba completamente a ciegas (tablero vacío y cero activos
        # "sin explicación") hasta que llegase el snapshot.
        ranked = []
        no_1min = 0
        if broker_market.assets:
            for name, info in broker_market.assets.items():
                if name in FORBIDDEN_REAL_M1:
                    # Variante REAL que la app de IQ Option no deja operar a 1 min
                    # aunque el catálogo la liste con 60 s: es un descarte por
                    # "no operable a 1 minuto", así que también se contabiliza.
                    no_1min += 1
                    continue
                if not FOREX_CODE_RE.match(name):
                    continue
                # SOLO activos operables a 1 MINUTO (vencimiento 60 s). Si el broker
                # reporta vencimientos y 60 s no está entre ellos, se descarta
                # (p. ej. USD/CHF (OP): solo 2m/3m/5m; USD/BRL (OP): idem).
                # Dato verificado con diagnostico_broker.py: 'expiration_times'
                # llega SIEMPRE (0 activos sin el campo), así que este filtro es
                # real y no un no-op.
                ets = info.get("exp_times") or []
                if ets and 60 not in ets:
                    no_1min += 1
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
        else:
            # Sin snapshot: derivar de las constantes de la librería. Ojo: en
            # consts.ACTIVES el mercado REAL es el código simple ('EURUSD') y el
            # OTC lleva '-OTC'; el sufijo '-OP' sólo existe en el snapshot del
            # broker, así que aquí se acepta cualquiera de las dos formas.
            forbidden_codes = {n.split("-")[0] for n in FORBIDDEN_REAL_M1}
            for code, rank in COMMON_PAIR_RANK.items():
                otc = code + "-OTC"
                if (not IQ_ACTIVES_CATALOG) or otc in IQ_ACTIVES_CATALOG:
                    ranked.append((otc, "OTC", rank))
                if code in forbidden_codes:
                    continue
                real = next((n for n in (code + "-OP", code)
                             if (not IQ_ACTIVES_CATALOG) or n in IQ_ACTIVES_CATALOG), None)
                if real:
                    ranked.append((real, "REAL", rank))

        ranked.sort(key=lambda x: (x[2], x[0]))   # prioridad y orden alfabético
        candidatos = [(n, t) for n, t, _ in ranked[:MAX_UNIVERSE]]
        # Métricas para el panel: cuántos activos del catálogo ofrecen 1 minuto
        # (se descartaron los que no) y cuántos se están vigilando.
        scan_metrics.assets_1min = len(candidatos)
        scan_metrics.assets_no_1min = no_1min
        return candidatos

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
        self.cache[asset] = deque(list(candles)[-self.MAX:], maxlen=self.MAX)

    def upsert(self, asset, candles):
        """Fusiona velas nuevas respetando el orden temporal.

        Antes sólo se añadía la vela más reciente y se IGNORABA toda vela con el
        mismo timestamp: la vela en formación quedaba congelada con un cierre
        parcial que después se usaba como si fuera una vela CERRADA, corrompiendo
        indicadores y patrón. Ahora la última vela se refresca (y queda con su
        cierre real al cerrarse) y se rellenan los huecos si el ciclo se saltó
        algún minuto."""
        if not candles:
            return
        buf = self.cache.get(asset)
        if buf is None:
            self.initialize(asset, candles)
            return
        last_ts = buf[-1].timestamp
        for c in candles:
            if c.timestamp > last_ts:
                buf.append(c)
                last_ts = c.timestamp
            elif c.timestamp == last_ts:
                buf[-1] = c
            # timestamps anteriores: ya no interesan

    def append(self, asset, candle):
        self.upsert(asset, [candle])

    def get(self, asset):
        return list(self.cache.get(asset, []))


candle_cache = CandleCache()

# Mínimo de velas para calcular indicadores y patrón con garantías.
# Antes había TRES umbrales distintos (50 en el feed, 200 en el validador y 30
# en los indicadores): un activo podía quedar marcado OPEN y luego ser ignorado
# en silencio por el pipeline. Ahora hay uno solo y coherente.
MIN_CANDLES = 60
HISTORY_BACKOFF = 120.0        # espera inicial entre reintentos de histórico
HISTORY_BACKOFF_MAX = 900.0    # tope de la espera (15 min) para activos tercos
STALE_TOLERANCE = int(os.environ.get("STALE_TOLERANCE", "3"))  # avisos seguidos antes de cerrar

candle_cache = CandleCache()

class LiveMarketFeed:
    CANDLES = 250
    PERIOD = 60

    def __init__(self):
        self.market = {}
        self._hist_last = {}       # último intento de histórico por activo
        self._hist_wait = {}       # espera actual por activo (crece si falla)
        self._last_reason = {}     # anti-inundación del log por activo
        self._stale_streak = {}    # avisos de vela vieja consecutivos por activo

    def _ausente(self, asset) -> bool:
        """Histéresis de 'activo ausente' por vela atrasada.

        Antes, un solo ciclo con la vela un poco atrasada marcaba el activo como
        STALE_DATA y lo sacaba de la lista de operables. Eso además llama a
        CandleStreamManager.stop(), que BORRA el estado GSR y la caché de velas
        de ese activo: un salto puntual del broker destruía el patrón que estaba
        formándose. Ahora se toleran STALE_TOLERANCE avisos seguidos y sólo se
        cierra de verdad si se mantiene (3 ciclos ≈ 3 minutos).
        Devuelve True cuando hay que cerrarlo ya."""
        racha = self._stale_streak.get(asset, 0) + 1
        self._stale_streak[asset] = racha
        return racha >= STALE_TOLERANCE

    def _ausente_reset(self, asset):
        self._stale_streak[asset] = 0

    def _log_once(self, asset, key, level, message):
        """Registra un aviso sólo cuando cambia el motivo (evita que el log se
        llene con las mismas 40 líneas 'NO_DATA' en cada ciclo)."""
        if self._last_reason.get(asset) == key:
            return
        self._last_reason[asset] = key
        getattr(log, level)(message)

    def _candles_for(self, asset, allow_history=True):
        """Velas más frescas disponibles, en este orden:
        1) stream en tiempo real (lo normal y barato),
        2) caché local,
        3) histórico del broker (caro: con freno por activo).

        El histórico ya NO se pide en cada ciclo: era el origen de los ~200 s
        bloqueados por refresco (40 activos x 5 s de timeout) con los que el
        bucle principal dejaba de escanear.

        Si un activo no devuelve histórico, su espera se DUPLICA hasta 15 min
        (en lugar de eliminarlo del monitoreo para siempre: los activos OTC se
        pausan y vuelven, p. ej. en el rollover, y deben recuperarse solos)."""
        candles = candle_streams.get_realtime(asset)
        if candles and len(candles) >= MIN_CANDLES:
            candle_cache.upsert(asset, candles)
            return candle_cache.get(asset), "STREAM"

        cached = candle_cache.get(asset)
        if cached and len(cached) >= MIN_CANDLES:
            return cached, "CACHE"
        if not allow_history:
            return (cached or None), "CACHE"

        # Si el activo YA tiene stream suscrito, el histórico llega por el propio
        # stream (start_candles_stream carga MAXDICT velas en real_time_candles),
        # así que NO se pide por la conexión compartida. Esto evita que un hilo
        # se quede esperando el candado de velas mientras se suscriben 40 streams
        # (era lo que bloqueaba el bucle del escáner durante minutos).
        if candle_streams.is_streaming(asset):
            return (cached or None), "STREAM_INICIANDO"

        now = time.time()
        espera = self._hist_wait.get(asset, HISTORY_BACKOFF)
        if now - self._hist_last.get(asset, 0.0) < espera:
            return (cached or None), "ESPERA"
        self._hist_last[asset] = now
        hist = iq.get_candles(asset, self.PERIOD, self.CANDLES)
        if hist and len(hist) >= MIN_CANDLES:
            candle_cache.upsert(asset, hist)
            if espera != HISTORY_BACKOFF:
                log.info(f"[MARKET] {format_asset_display_name(asset)}: histórico "
                         f"recuperado; vuelve al intervalo normal de {HISTORY_BACKOFF:.0f}s")
            self._hist_wait[asset] = HISTORY_BACKOFF
        else:
            nueva = min(espera * 2, HISTORY_BACKOFF_MAX)
            if nueva != espera:
                log.info(f"[MARKET] {format_asset_display_name(asset)}: sin histórico; "
                         f"se reintentará en {nueva:.0f}s")
            self._hist_wait[asset] = nueva
        return candle_cache.get(asset), "HISTORICO"

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
            self._log_once(asset, "no-broker", "warning",
                           f"[MARKET] {format_asset_display_name(asset)} -> CERRADO (disponibilidad del broker no válida)")
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
            self._log_once(asset, "cerrado", "warning",
                           f"[MARKET] {format_asset_display_name(asset)} -> CERRADO (broker: no disponible)")
            return status

        candles, source = self._candles_for(asset)

        if not candles or len(candles) < MIN_CANDLES:
            status = AssetTradingStatus(
                asset=asset,
                asset_type=asset_type,
                tradable=False,
                status=AssetStatusEnum.NO_DATA,
                last_candle_time=0,
                checked_at=now_broker,
                reason=f"Sin velas suficientes ({len(candles or [])}/{MIN_CANDLES})."
            )
            self.market.pop(asset, None)
            self._log_once(asset, "nodata", "warning",
                           f"[MARKET] {format_asset_display_name(asset)} -> NO_DATA "
                           f"(velas {len(candles or [])}/{MIN_CANDLES}, fuente {source})")
            return status

        last_candle_ts = candles[-1].timestamp
        age = now_broker - last_candle_ts

        # === VALIDACIÓN POR SESIÓN Y TIPO DE ACTIVO ===
        if not is_asset_available_in_session(asset, asset_type, age):
            if not self._ausente(asset):
                # Histéresis: un ciclo con la vela algo atrasada NO cierra el
                # activo. Cerrarlo llama a CandleStreamManager.stop(), que borra
                # su estado GSR y la caché: un salto puntual del broker destruía
                # el patrón que se estaba formando. Se tolera y se mantiene el
                # estado anterior (el patrón sólo avanza con velas nuevas).
                previo = asset_manager.get_status(asset)
                self._log_once(asset, f"tolerado:{age // 30}", "info",
                               f"[MARKET] {format_asset_display_name(asset)} con vela de hace "
                               f"{age}s: se tolera "
                               f"({self._stale_streak.get(asset)}/{STALE_TOLERANCE}) sin cerrarlo")
                return previo or AssetTradingStatus(
                    asset=asset, asset_type=asset_type, tradable=True,
                    status=AssetStatusEnum.OPEN, last_candle_time=last_candle_ts,
                    checked_at=now_broker, reason="Vela atrasada (tolerada)")
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
            self._log_once(asset, f"viejo:{age // 30}", "warning",
                           f"[MARKET] {format_asset_display_name(asset)} -> CERRADO "
                           f"(vela de hace {age}s en {self._stale_streak.get(asset)} avisos seguidos)")
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
        self._ausente_reset(asset)

        self.market[asset] = candles
        scan_metrics.candles_processed += len(candles)
        self._log_once(asset, f"open:{source}", "info",
                       f"[MARKET] {format_asset_display_name(asset)} -> OPEN "
                       f"(vela viva hace {age}s, fuente {source})")
        return status

    def refresh_market_sessions(self):
        if not ensure_connected():
            log.warning("[MARKET] Sin sesión con el broker: refresco de sesiones omitido.")
            return

        # Sin catálogo válido todavía no se puede decidir nada: si se continuara,
        # los 40 activos se marcarían como CERRADOS en falso durante el arranque
        # (el catálogo tarda unos segundos en llegar tras conectar).
        if not broker_market.valid():
            log.info("[MARKET] Catálogo del broker aún no disponible: refresco omitido.")
            return

        candidates = asset_manager.get_candidate_list()
        if not candidates:
            log.warning("[MARKET] Sin candidatos: el catálogo del broker aún no está listo.")
            return

        for asset, asset_type in candidates:
            try:
                status = self.validate_and_fetch_asset(asset, asset_type)
            except Exception as e:
                log.warning(f"[MARKET] Error validando {asset}: {e}")
                continue
            asset_manager.update_status(status)

        asset_manager.rebuild_active_assets()
        self._last_tradable_count = len(asset_manager.active_tradable_assets)
        log.info(f"[MARKET] Universo: {scan_metrics.assets_1min} activos con contrato de 1 min "
                 f"({scan_metrics.assets_no_1min} descartados por no ofrecer vencimiento de 60 s).")

    def fast_update_active(self):
        """Ruta rápida (cada minuto): usa el stream en tiempo real. NO pide
        histórico: si un activo se queda sin datos, el refresco de sesiones (cada
        60 s) decide si sigue vivo o se cierra."""
        active = asset_manager.all_tradable()
        now_broker = int(broker_clock.now_ts())

        for asset in active:
            st = asset_manager.get_status(asset)
            asset_type = st.asset_type if st else "REAL"

            candles = candle_streams.get_realtime(asset)
            if candles and len(candles) >= MIN_CANDLES:
                candle_cache.upsert(asset, candles)
            candles = candle_cache.get(asset)

            if not candles or len(candles) < MIN_CANDLES:
                self._log_once(asset, "fast-nodata", "debug",
                               f"[MARKET] {format_asset_display_name(asset)} sin velas en Fast Loop.")
                continue

            last_ts = candles[-1].timestamp
            age = now_broker - last_ts
            max_age = LIVE_WINDOW_OTC if asset_type == "OTC" else LIVE_WINDOW_REAL
            if age <= max_age:
                self.market[asset] = candles
                self._ausente_reset(asset)
                continue

            # La vela dejó de refrescar: datos atrasados (stale). Se toleran
            # STALE_TOLERANCE ciclos seguidos antes de cerrar: cerrar de inmediato
            # ante un salto puntual borraba el patrón GSR en curso.
            if not self._ausente(asset):
                self._log_once(asset, f"fast-tolera:{age // 30}", "info",
                               f"[MARKET] {format_asset_display_name(asset)} sin refrescar "
                               f"({age}s > {max_age}s): se tolera "
                               f"({self._stale_streak.get(asset)}/{STALE_TOLERANCE})")
                continue
            if st:
                st.tradable = False
                st.status = AssetStatusEnum.STALE_DATA
                st.reason = f"Vela sin refrescar (edad {age}s > {max_age}s)"
                asset_manager.update_status(st)
            self.market.pop(asset, None)
            self._log_once(asset, "fast-stale", "warning",
                           f"[MARKET] {format_asset_display_name(asset)} en Fast Loop sin refrescar "
                           f"(edad {age}s > {max_age}s, {self._stale_streak.get(asset)} ciclos seguidos)")

    def get(self, asset):
        return self.market.get(asset)

    def active_assets(self):
        return list(self.market.keys())

market_feed = LiveMarketFeed()

# ==========================================================
# CÁLCULOS MATEMÁTICOS E INDICADORES GSR
# ==========================================================
class CandleValidator:
    MINIMUM = MIN_CANDLES
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

def _ema_last_value(x, alpha: float) -> float:
    """Último valor de la EMA recursiva de pandas ewm(alpha=..., adjust=False):
    y[0] = x[0]  y  y[i] = (1-alpha)*y[i-1] + alpha*x[i].

    Se itera con float de Python (no con escalares numpy: cada operación con un
    np.float64 crea un objeto y cuesta ~10 µs, 60 veces más que un float)."""
    values = x.tolist() if hasattr(x, "tolist") else list(x)
    acc = values[0]
    one_minus = 1.0 - alpha
    for v in values[1:]:
        acc = one_minus * acc + alpha * v
    return acc


def _trend_ok_np(close: np.ndarray) -> bool:
    """Igual que _trend_ok() pero en numpy (mismo resultado, ~20x más rápido)."""
    fast = _ema_last_value(close, 2.0 / (TREND_FAST + 1.0))
    slow = _ema_last_value(close, 2.0 / (TREND_SLOW + 1.0))
    sep = float(fast - slow)
    if close.size >= 100:
        sd = float(close[-100:].std(ddof=1))     # rolling(100).std() usa ddof=1
    else:
        sd = float("nan")
    if not np.isfinite(sd) or sd <= 0:
        return True   # sin volatilidad medible -> no bloquear
    return abs(sep) / sd < TREND_FILTER_Z


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


# Motor de indicadores: "numpy" (por defecto) o "ta" (implementación original
# con pandas + librería ta, se conserva como referencia verificable).
# Motivo del cambio: calcular BB+RSI+DAMOA con pandas costaba 215 ms por activo,
# es decir ~8,6 s por ciclo con 40 activos: la señal podía llegar hasta 8 s
# tarde dentro de una vela de 1 minuto. Las fórmulas son exactamente las mismas
# (se comparan en el auto-test contra la versión con 'ta').
INDICATORS_ENGINE = os.environ.get("INDICATORS_ENGINE", "numpy").strip().lower()


def _indicators_ta(close: np.ndarray) -> tuple:
    """Implementación original (pandas + ta). Referencia de contraste."""
    bb = ta.volatility.BollingerBands(pd.Series(close), window=BB_PERIOD, window_dev=BB_STD)
    upper = float(bb.bollinger_hband().iloc[-1])
    middle = float(bb.bollinger_mavg().iloc[-1])
    lower = float(bb.bollinger_lband().iloc[-1])
    rsi_val = float(ta.momentum.RSIIndicator(pd.Series(close), window=RSI_PERIOD).rsi().iloc[-1])

    df_close = pd.Series(close)
    ema = df_close.ewm(span=DAMOA_PERIOD, adjust=False).mean()
    volatilidad_damoa = np.sqrt((df_close.diff() ** 2).rolling(DAMOA_PERIOD).mean())
    vol_piso = volatilidad_damoa.where(
        volatilidad_damoa > (df_close.abs() * DAMOA_VOL_FLOOR_REL),
        df_close.abs() * DAMOA_VOL_FLOOR_REL
    )
    damoa_serie = ((df_close - ema) / vol_piso) * 10
    damoa_val = float(np.clip(float(damoa_serie.fillna(0.0).iloc[-1]), -DAMOA_CLAMP, DAMOA_CLAMP))
    return upper, middle, lower, rsi_val, damoa_val, _trend_ok(close)


def _indicators_numpy(close: np.ndarray) -> tuple:
    """Mismas fórmulas que _indicators_ta() pero en numpy."""
    n = close.size

    # --- Bandas de Bollinger (ta: rolling(BB_PERIOD, min_periods=BB_PERIOD), std ddof=0)
    window = close[-BB_PERIOD:] if n >= BB_PERIOD else close
    mavg = float(window.mean())
    mstd = float(window.std(ddof=0))
    upper = mavg + BB_STD * mstd
    middle = mavg
    lower = mavg - BB_STD * mstd

    # --- RSI (ta: Wilder con ewm(alpha=1/period, adjust=False); diff NaN -> 0)
    diffs = np.diff(close)
    up = np.zeros(n, dtype=float)
    down = np.zeros(n, dtype=float)
    up[1:] = np.where(diffs > 0, diffs, 0.0)
    down[1:] = np.where(diffs < 0, -diffs, 0.0)
    alpha_rsi = 1.0 / RSI_PERIOD
    emaup = _ema_last_value(up, alpha_rsi)
    emadn = _ema_last_value(down, alpha_rsi)
    if emadn == 0:
        rsi_val = 100.0
    else:
        rsi_val = 100.0 - (100.0 / (1.0 + (emaup / emadn)))

    # --- DAMOA
    ema5 = _ema_last_value(close, 2.0 / (DAMOA_PERIOD + 1.0))
    vol = float("nan")
    # rolling(5).mean() sobre las diferencias al cuadrado: NaN en los índices
    # 0..4 (la primera diferencia es NaN) -> el último valor sólo es válido si
    # hay al menos 6 velas.
    if n >= DAMOA_PERIOD + 2:
        sq = (close[-(DAMOA_PERIOD):] - close[-(DAMOA_PERIOD + 1):-1]) ** 2
        vol = float(np.sqrt(sq.mean()))      # RMS, igual que la versión con pandas
    piso = abs(float(close[-1])) * DAMOA_VOL_FLOOR_REL
    vol_use = vol if (np.isfinite(vol) and vol > piso) else piso
    if vol_use == 0:
        damoa_val = 0.0
    else:
        damoa_val = ((float(close[-1]) - ema5) / vol_use) * 10.0
        if not np.isfinite(damoa_val):
            damoa_val = 0.0
    damoa_val = float(np.clip(damoa_val, -DAMOA_CLAMP, DAMOA_CLAMP))
    return upper, middle, lower, rsi_val, damoa_val, _trend_ok_np(close)


def calculate_indicators(candles, include_forming: bool = False) -> IndicatorSnapshot:
    # P2: por defecto se trabaja SOLO con velas cerradas (se descarta la vela en
    # formación, candles[-1]) para que BB/RSI/DAMOA y el body-outside apunten al
    # MISMO cierre (sin look-ahead en las señales).
    # include_forming=True es SOLO para el tablero en vivo: incluye la vela que
    # se está formando para que los valores se muevan en tiempo real (display).
    data = candles if include_forming else candles[:-1]
    if len(data) < 30:
        return None
    close = np.array([x.close for x in data], dtype=float)

    if INDICATORS_ENGINE == "ta":
        upper, middle, lower, rsi_val, damoa_val, trend = _indicators_ta(close)
    else:
        upper, middle, lower, rsi_val, damoa_val, trend = _indicators_numpy(close)

    indicator = IndicatorSnapshot(index=len(data)-1)
    indicator.bb_upper = upper
    indicator.bb_middle = middle
    indicator.bb_lower = lower
    indicator.rsi = rsi_val
    indicator.damoa = damoa_val
    indicator.trend_ok = True if include_forming else trend

    current_candle = data[-1]
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


def body_outside_umbral(asset: str) -> float:
    """Umbral de ruptura que se aplica a ESTE activo (REAL vs OTC).

    OJO: body_outside_percent() sólo CALCULA el porcentaje; el umbral se aplica
    en las comparaciones de detect_first_candle/detect_second_candle. Se conserva
    la firma de body_outside_percent() para no romper backtest_gsr.py.

    Por defecto REAL y OTC comparten el mismo valor calibrado (40.0). Para
    endurecer sólo el mercado sintético:  set BODY_OUTSIDE_OTC=55
    """
    return BODY_OUTSIDE_OTC if str(asset).upper().endswith("-OTC") else BODY_OUTSIDE_REAL

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
# Historial de patrones ya disparados. Es un dict (ordenado por inserción) en
# lugar de un set para poder podar los más antiguos y que no crezca sin límite
# en ejecuciones de días. El test `in` funciona igual.
pattern_history = {}


def _podar_pattern_history():
    """Descarta los patrones más antiguos cuando se supera el tope.

    Un patrón sólo se consulta en los minutos siguientes a formarse, así que
    podar lo viejo no puede provocar que se repita una señal."""
    while len(pattern_history) > PATTERN_HISTORY_MAX:
        pattern_history.pop(next(iter(pattern_history)))

def detect_first_candle(asset, candle, indicator):
    state = gsr_memory.get(asset)
    umbral = body_outside_umbral(asset)
    if candle_color(candle) == "GREEN":
        outside = body_outside_upper(candle, indicator.bb_upper)
        if outside >= umbral:
            state.phase = GSRPhase.PRIMERA_VELA
            state.direction = "PUT"
            state.progress = 25
            state.first_candle = candle.timestamp
            state.first_close = candle.close
            gsr_events.add(asset, "PRIMERA_VELA", "PUT")
            return True
    elif candle_color(candle) == "RED":
        outside = body_outside_lower(candle, indicator.bb_lower)
        if outside >= umbral:
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

    # El patrón GSR exige DOS velas de impulso CONSECUTIVAS. Si el ciclo se saltó
    # un minuto (o la vela no llegó), se descarta el patrón en lugar de emparejar
    # velas separadas como si fueran contiguas.
    if candle.timestamp != state.first_candle + 60:
        gsr_memory.reset(asset)
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

    if outside < body_outside_umbral(asset):
        gsr_memory.reset(asset)
        return False

    state.phase = GSRPhase.SEGUNDA_VELA
    state.progress = 50
    state.second_candle = candle.timestamp
    state.second_close = candle.close
    state.pattern_id = pattern_engine.assign(state)
    gsr_events.add(asset, "SEGUNDA_VELA", state.direction)
    return True

def _volumen_stats(asset) -> tuple:
    """(volumen de la última vela cerrada, media de las anteriores, relativo).

    Devuelve (None, None, None) si no hay datos; el relativo es None cuando la
    media es 0 (el broker no publica volumen), para no dividir por cero.
    """
    candles = candle_cache.get(asset)
    if not candles or len(candles) < VOLUME_LOOKBACK + 2:
        return None, None, None
    previos = [c.volume for c in candles[-(VOLUME_LOOKBACK + 1):-1]]
    ultimo = float(candles[-2].volume)
    if not previos or max(previos) <= 0:
        return ultimo, 0.0, None
    media = sum(previos) / len(previos)
    return ultimo, media, ((ultimo / media) if media > 0 else None)


def _volumen_ok(asset) -> tuple:
    """Filtro de volumen OPCIONAL. Devuelve (apto, detalle).

    Exige que la última vela cerrada tenga un volumen >= VOLUME_MIN_REL veces la
    media de las VOLUME_LOOKBACK anteriores. Viene DESACTIVADO por defecto y se
    auto-desactiva cuando el activo no publica volumen utilizable: medido sobre
    1,6 M de velas reales, los activos OTC entregan volumen 0 en TODAS las velas
    (los REAL sí traen volumen real). Sin ese control, activarlo bloquearía el
    100% de las señales OTC; con él, el filtro actúa donde sí hay dato.
    """
    if not VOLUME_FILTER_ENABLED:
        return True, "filtro de volumen desactivado"
    ultimo, media, rel = _volumen_stats(asset)
    if ultimo is None:
        return True, "sin histórico de volumen suficiente"
    if rel is None:
        return True, "el broker no publica volumen (todo 0): filtro omitido"
    return (rel >= VOLUME_MIN_REL), f"volumen relativo {rel:.2f} (mínimo {VOLUME_MIN_REL:.2f})"


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

        # Filtro de volumen (opcional; ver _volumen_ok).
        vol_ok, vol_detalle = _volumen_ok(asset)
        if not vol_ok:
            log.info(f"[GSR] {format_asset_display_name(asset)} descartado en la "
                     f"confirmación: {vol_detalle}")
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

# (Aquí vivía el envío a un grupo externo mediante un puente local, que se
#  eliminó el 21-sep-2026: la publicación migró por completo a la app.)
#  la publicación de señales se hace ahora en Alí Binary Options.)


def enviar_alerta_app(asset, direction, rsi, damoa, strength, entry_time_cot):
    """PUBLICACIÓN PRINCIPAL (Opción A): señal a Alí Binary Options por Cloud
    Function HTTP, que a su vez escribe en Firestore.

    Desactivada si APP_SIGNAL_URL está vacío (no rompe nunca el escáner).
    El asset va en formato de broker ('EUR/USD' / 'EUR/USD (OTC)') y la hora en
    zona Colombia, que es como los muestra la app."""
    if not APP_SIGNAL_URL:
        return False
    try:
        import json as _json
        import urllib.request as _urllib
        payload = _json.dumps({
            "secret": APP_SIGNAL_SECRET,
            "asset": broker_search_name(asset),      # "EUR/USD" o "EUR/USD (OTC)"
            "broker": "IQ Option",
            "direction": direction,
            "entryTime": entry_time_cot,              # "HH:MM" hora Colombia
            "expiration": 1,
            "source": "bot",
            "botName": "AppALI",
            "strategy": "GSR",
            "confidence": int(strength),
            "rsi": round(rsi, 2),
            "damoa": round(damoa, 2)
        }).encode("utf-8")
        req = _urllib.Request(APP_SIGNAL_URL, data=payload,
                              headers={"Content-Type": "application/json"})
        with _urllib.urlopen(req, timeout=5) as resp:
            resp.read()
        log.info(f"📤 [APP] Señal publicada en Ali Binary Options: "
                 f"{broker_search_name(asset)} {direction} {entry_time_cot}")
        return True
    except Exception as e:
        log.warning(f"[APP] No se pudo publicar la señal en Ali Binary Options: {e}")
        return False


def enviar_alerta_firestore(asset, direction, rsi, damoa, strength, entry_time_cot):
    """Opción B (sin Cloud Functions): escribe la señal directo en Firestore con
    una service account key. Desactivada si FIREBASE_SA_PATH está vacío."""
    if not FIREBASE_SA_PATH:
        return
    try:
        from google.cloud import firestore
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(FIREBASE_SA_PATH)
        db = firestore.Client(project=FIREBASE_PROJECT_ID, credentials=creds)
        db.collection("signals").add({
            "asset": broker_search_name(asset),      # "EUR/USD" o "EUR/USD (OTC)"
            "broker": "IQ Option",
            "direction": direction,
            "entryTime": entry_time_cot,              # "HH:MM" hora Colombia
            "expiration": 1,
            "status": "pending",
            "source": "bot",
            "botName": "AppALI",
            "strategy": "GSR",
            "confidence": int(strength),
            "rsi": round(rsi, 2),
            "damoa": round(damoa, 2),
            "createdAt": firestore.SERVER_TIMESTAMP
        })
        log.info("[APP] Señal escrita en Firestore (Alí Binary Options)")
    except Exception as e:
        log.warning(f"[APP] No se pudo escribir la señal en Firestore: {e}")


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

    # ===== AUDITORÍA DEL DISPARO: motivo exacto =====
    tipo = "OTC" if str(asset).upper().endswith("-OTC") else "REAL"
    umbral_bb = body_outside_umbral(asset)
    bb_pct = float(indicator.outside)
    vol_ultimo, vol_media, vol_rel = _volumen_stats(asset)
    detalle = {
        "tipo": tipo,
        "bb_pct": round(bb_pct, 2),
        "umbral_bb": round(umbral_bb, 1),
        "volumen": "" if vol_ultimo is None else round(vol_ultimo, 2),
        "vol_medio": "" if vol_media is None else round(vol_media, 2),
        "vol_rel": "" if vol_rel is None else round(vol_rel, 3),
    }
    log.info(
        f"[SIGNAL] {format_asset_display_name(asset)} {state.direction} | "
        f"BB {bb_pct:.1f}% (umbral {umbral_bb:.0f}%) | RSI {state.rsi:.1f} | "
        f"DAMOA {state.damoa:.1f} | vol {detalle['volumen']} "
        f"(media {detalle['vol_medio']}, rel {detalle['vol_rel']}) | {tipo}"
    )
    if tipo == "OTC":
        # Evidencia para decidir si conviene endurecer el umbral de OTC: se
        # registra qué habría pasado con el umbral estricto, sin aplicarlo.
        log.info(f"[SIGNAL] {format_asset_display_name(asset)} con umbral estricto OTC "
                 f"{OTC_STRICT_REFERENCE:.0f}%: "
                 f"{'SE HABRÍA DESCARTADO' if bb_pct < OTC_STRICT_REFERENCE else 'habría pasado'}")

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
        pattern_history[state.pattern_id] = opening.timestamp
        _podar_pattern_history()
        audit_logger.record(asset, state.direction, opening.timestamp,
                            state.rsi, state.damoa, detalle)

        # === PUBLICAR EN ALÍ BINARY OPTIONS ===
        # Vía principal (Opción A): Cloud Function 'postSignal' -> Firestore.
        # Vía alternativa (Opción B): escritura directa en Firestore con service
        # account. Cada una se activa sola si su configuración está presente; si
        # no hay ninguna, la señal sólo queda en el log y en signals_audit.csv.
        # (Canal externo eliminado el 21-sep-2026: la señal se publica en la app.)
        try:
            hora_cot = datetime.fromtimestamp(int(opening.timestamp), tz=TZ_COLOMBIA).strftime("%H:%M")
        except Exception:
            hora_cot = ""

        threading.Thread(
            target=enviar_alerta_app,
            args=(asset, state.direction, state.rsi, state.damoa, 75.0, hora_cot),
            daemon=True
        ).start()

        threading.Thread(
            target=enviar_alerta_firestore,
            args=(asset, state.direction, state.rsi, state.damoa, 75.0, hora_cot),
            daemon=True
        ).start()

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
    """Métrica de CONDICIONES (no de avance del patrón).

    Cuenta cuántas de las tres condiciones GSR (BB, RSI, DAMOA) se cumplen en
    este instante; es lo que el tablero muestra como '2/3'. Deliberadamente NO
    es el porcentaje de la barra: la barra mide el avance del patrón secuencial
    (0/25/50/100 según la fase). Mezclar ambos hacía que un activo pareciera
    "casi listo" sólo por tener las tres condiciones extremas sin patrón
    formado, y eso producía señales falsas en la versión anterior."""

    KEYS = ("bb_ok", "rsi_ok", "damoa_ok")

    def calculate(self, row) -> tuple:
        cumplidas = sum(1 for k in self.KEYS if row.get(k))
        return cumplidas, len(self.KEYS)


progress_engine = ProgressEngine()

class GSRStage(Enum):
    SEARCH = 0
    FIRST = 1
    SECOND = 2
    BB_OK = 3
    RSI_OK = 4
    DAMOA_OK = 5
    READY = 6

# NOTA: aquí vivía un StageEngine que recalculaba la ETAPA a partir de las
# condiciones (BB/RSI/DAMOA), al margen de la fase real del patrón. No se usaba
# en ninguna parte, pero era el origen de la confusión "25% con las condiciones
# en verde": la etapa la determina SIEMPRE la fase del patrón secuencial
# (ver ProximityEngine.update). Se eliminó para no dejar dos fuentes de verdad.

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
            "reason": reason_engine.explain(indicator),
            "umbral_bb": round(body_outside_umbral(asset), 1)
        }

        # Dos métricas DISTINTAS y complementarias (antes se confundían):
        #   conditions_*  -> cuántas de las 3 condiciones se cumplen ahora (2/3)
        #   progress      -> avance del patrón secuencial (0/25/50/100)
        cumplidas, total = progress_engine.calculate(self.rows[asset])
        self.rows[asset]["conditions_met"] = cumplidas
        self.rows[asset]["conditions_total"] = total
        self.rows[asset]["conditions_pct"] = int(round(100 * cumplidas / total)) if total else 0

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
                # Fila "sin datos en vivo": vale tanto para un activo cerrado como
                # para uno recién abierto del que el motor aún no tiene métricas.
                # Antes ambos se pintaban igual ("CERRADO" + "PATRÓN GSR
                # COMPLETO"), así que un activo que SÍ estaba operando aparecía
                # como cerrado. Ahora se distingue con "ABRIENDO".
                fila = {
                    "asset": format_asset_display_name(code),
                    "copy_name": broker_search_name(code),
                    "raw_asset": code,
                    "type": (st.asset_type if st else "REAL"),
                    "status": "OPEN" if abierto else (st.status.name if st else "CLOSED"),
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
                    "reason": ["SIN DATOS AÚN"] if abierto else [],
                    "progress": 0,
                    "conditions_met": 0,
                    "conditions_total": len(progress_engine.KEYS),
                    "conditions_pct": 0,
                    "umbral_bb": round(body_outside_umbral(code), 1),
                    "stage": "ABRIENDO" if abierto else "CERRADO",
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

        current = candles[-2]      # última vela CERRADA
        opening = candles[-1]      # vela en formación (la que se operaría)

        if not new_candle.detect(asset, current): return

        state = gsr_memory.get(asset)
        indicator = indicator_engine.update(asset, candles)
        if indicator is None or not indicator_validator.validate(indicator): return

        # La máquina de estados avanza UNA vela cerrada por ciclo y evalúa
        # SIEMPRE la misma vela ('current', = candles[-2]) con los indicadores
        # calculados justo en su cierre. Antes el primer paso evaluaba
        # 'candles[-3]' contra los indicadores de 'candles[-2]': un look-ahead de
        # una vela que contaminaba la detección del patrón.
        if state.phase == GSRPhase.OBSERVANDO:
            detect_first_candle(asset, current, indicator)
        elif state.phase == GSRPhase.PRIMERA_VELA:
            detect_second_candle(asset, current, indicator)
        elif state.phase == GSRPhase.SEGUNDA_VELA:
            ready_engine.confirm(asset, indicator)
        elif state.phase == GSRPhase.LISTA:
            # P3: esperar una vela CERRADA que CONFIRME la reversión antes de
            # disparar. Debe ser posterior a la 2ª vela del patrón y cercana
            # (si pasan demasiados minutos el patrón ya no es válido).
            delta = current.timestamp - state.second_candle
            if 0 < delta <= 180 and _confirm_reversal(asset, current, state):
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
    """Cliente ÚNICO hacia IQ Option.

    Se crea una sola instancia de IQ_Option durante toda la vida del proceso: la
    versión anterior creaba una instancia nueva en cada reintento (el log de
    errores acumuló 199 objetos IQ_Option y 199 websockets abandonados). La
    propia librería ya cierra el socket anterior al reconectar.
    """

    def __init__(self):
        self.api = None
        self.connected = False
        self.session_gen = 0            # sube en cada reconexión correcta
        self.last_connect_attempt = 0.0
        self.connect_failures = 0
        # Pool EXCLUSIVO para el histórico de velas: nunca se comparte con los
        # streams (compartirlo fue lo que agotó los hilos y dejó todo en NO_DATA).
        self._history_executor = ThreadPoolExecutor(max_workers=4,
                                                    thread_name_prefix="iq-hist")
        self._stream_executor = ThreadPoolExecutor(max_workers=2,
                                                   thread_name_prefix="iq-strm")

    def _ensure_client(self):
        if self.api is None:
            self.api = IQ_Option(IQ_EMAIL, IQ_PASSWORD)
        return self.api

    def connect(self):
        if not IQ_AVAILABLE or not IQ_EMAIL or not IQ_PASSWORD:
            log.error("❌ Credenciales ausentes en el entorno local (.env).")
            return False
        with IQ_ABANDONED_LOCK:
            if IQ_ABANDONED_CALLS >= IQ_ABANDONED_MAX:
                log.error("[IQ] Hay llamadas de la librería colgadas de forma "
                          "permanente; no se reconecta para no degradar el equipo.")
                return False
        self.last_connect_attempt = time.time()
        try:
            log.info(f"[IQ] Conectando a IQ Option con {IQ_EMAIL} (modo {IQ_MODE})...")
            client = self._ensure_client()
            ok, reason = call_with_timeout(client.connect, IQ_CONNECT_TIMEOUT)
            if not ok:
                self.connected = False
                self.connect_failures += 1
                log.error(f"❌ Falló el apretón de manos con el bróker: {reason}")
                return False
            # change_balance() también espera el perfil con un bucle vacío.
            try:
                call_with_timeout(client.change_balance, 30, IQ_MODE)
            except Exception as exc:
                log.warning(f"[IQ] change_balance({IQ_MODE}) no confirmó: {exc}")
            self.connected = True
            self.connect_failures = 0
            self.session_gen += 1
            log.info(f"✅ IQ Option enlazado al entorno {IQ_MODE} (sesión #{self.session_gen}).")
            self.sync_clock()
            return True
        except BaseException as e:
            # BaseException a propósito: la librería puede lanzar SystemExit
            # (exit(1)) si el tipo de cuenta no es REAL/PRACTICE/TOURNAMENT.
            self.connected = False
            self.connect_failures += 1
            log.error(f"❌ Excepción crítica al conectar: {type(e).__name__}: {e}")
            return False

    def sync_clock(self):
        """Sincroniza con el reloj del broker promediando DOS muestras.

        Una sola medición arrastra la latencia de red puntual (medido con
        diagnostico_broker.py: dos lecturas separadas 0,3 s dieron +1,058 s y
        +0,758 s). El promedio reduce ese ruido, y el valor resultante sigue
        pasando por la validación de BrokerClock: si no es creíble se descarta y
        se mantiene el reloj local, en lugar de aceptar un valor envenenado."""
        if not self.connected or self.api is None:
            return
        offsets = []
        for i in range(max(1, CLOCK_SAMPLES)):
            try:
                ts = call_with_timeout(self.api.get_server_timestamp, 5)
                offsets.append(float(ts) - time.time())
            except (TypeError, ValueError):
                pass
            except Exception as e:
                log.debug(f"[CLOCK] server timestamp no disponible: {e}")
                break
            if i < CLOCK_SAMPLES - 1:
                time.sleep(CLOCK_SAMPLE_GAP)
        if not offsets:
            return
        broker_clock.sync_with_broker(time.time() + (sum(offsets) / len(offsets)))

    def rebuild(self):
        """Renueva la sesión IQ desde cero reutilizando el mismo objeto
        IQ_Option (la librería cierra el socket anterior internamente)."""
        try:
            if self.api is not None:
                inner = getattr(self.api, "api", None)
                closer = getattr(inner, "close", None) if inner is not None else None
                if callable(closer):
                    try:
                        closer()
                    except Exception:
                        pass
        finally:
            self.connected = False
            broker_clock.reset_to_local()
        return self.connect()

    def is_connected(self):
        if self.api is None:
            self.connected = False
            return False
        try:
            alive = bool(self.api.check_connect())
        except Exception:
            alive = False
        if not alive:
            self.connected = False
            broker_clock.reset_to_local()
        return self.connected and alive

    @staticmethod
    def _to_candles(raw):
        if not raw or not isinstance(raw, list):
            return None
        res = []
        for c in raw:
            if isinstance(c, dict) and "from" in c:
                try:
                    res.append(Candle(
                        timestamp=int(c["from"]),
                        open=float(c["open"]),
                        high=float(c["max"]),
                        low=float(c["min"]),
                        close=float(c["close"]),
                        volume=float(c.get("volume") or 0.0)
                    ))
                except (KeyError, TypeError, ValueError):
                    continue
        return res or None

    def get_candles(self, asset, interval=60, count=100, end_time=None):
        """Histórico de velas con timeout DURO.

        La librería parcheada ya no puede quedarse girando sin fin, pero se
        añade una segunda red de seguridad: pool dedicado + timeout explícito.
        Antes, un timeout dejaba el hilo ocupado para siempre en el mismo pool
        que usaban los streams, y a la quinta vez TODO el escáner quedaba en
        NO_DATA de forma irreversible."""
        if self.api is None or not self.connected:
            return None
        end = int(end_time) if end_time else int(time.time())
        future = None
        try:
            future = self._history_executor.submit(
                self.api.get_candles, asset, interval, count, end)
            raw = future.result(timeout=IQ_CANDLE_TIMEOUT + 4)
        except Exception as e:
            log.debug(f"[IQ] lectura de velas fallida para {asset}: {e}")
            if future is not None:
                try:
                    future.cancel()
                except Exception:
                    pass
            return None
        return self._to_candles(raw)

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
        self._session_gen = -1

    def start(self, asset) -> bool:
        if iq.api is None:
            return False
        try:
            fut = iq._stream_executor.submit(
                iq.api.start_candles_stream, asset, self.SIZE, self.MAXDICT)
            # El parche de la librería acota la espera a IQ_STREAM_TIMEOUT,
            # así que este timeout sólo es una red de seguridad adicional.
            ok = fut.result(timeout=IQ_STREAM_TIMEOUT + 20)
        except Exception as e:
            log.warning(f"[STREAM] no se pudo iniciar stream {asset}: {e}")
            return False
        if not ok:
            log.warning(f"[STREAM] el broker no confirmó el stream de {asset}")
            return False
        with self.lock:
            self.streams.add(asset)
        log.info(f"[STREAM] stream activo para {asset}")
        return True

    def stop(self, asset):
        try:
            if iq.api is not None:
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
        if iq.api is None:
            return None
        try:
            data = iq.api.get_realtime_candles(asset, self.SIZE)
        except Exception:
            return None
        if not isinstance(data, dict) or not data:
            return None
        res = []
        for _ts, c in sorted(data.items(), key=lambda kv: str(kv[0])):
            if isinstance(c, dict) and "from" in c:
                try:
                    res.append(Candle(
                        timestamp=int(c["from"]),
                        open=float(c["open"]),
                        high=float(c["max"]),
                        low=float(c["min"]),
                        close=float(c["close"]),
                        volume=float(c.get("volume") or 0.0)
                    ))
                except (KeyError, TypeError, ValueError):
                    continue
        return res or None

    def active_count(self):
        with self.lock:
            return len(self.streams)

    def is_streaming(self, asset) -> bool:
        with self.lock:
            return asset in self.streams

    def snapshot(self) -> list:
        with self.lock:
            return sorted(self.streams)

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
                # Tras una reconexión los streams anteriores ya no existen en el
                # socket nuevo: hay que volver a suscribirlos todos.
                if iq.session_gen != self._session_gen:
                    if self._session_gen != -1:
                        log.info("[STREAM] Reconexión detectada: re-suscribiendo streams...")
                    self._session_gen = iq.session_gen
                    with self.lock:
                        self.streams.clear()
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
CONN_BACKOFF_START = 5.0
CONN_BACKOFF_MAX = 180.0
_conn_backoff = CONN_BACKOFF_START
_next_connect_at = 0.0


def ensure_connected(force: bool = False) -> bool:
    """Reconexión centralizada y controlada, con espera exponencial.

    Antes CADA pasada del bucle (cada 0,5 s) lanzaba un intento de conexión
    nuevo: el log de errores acumuló 199 "Excepción crítica al conectar" y 199
    clientes IQ abandonados (cada uno con su websocket). Ahora los reintentos se
    espacian de 5 s hasta 3 minutos."""
    global BROKER_STATUS, _conn_backoff, _next_connect_at
    if iq.is_connected():
        BROKER_STATUS = "CONNECTED"
        _conn_backoff = CONN_BACKOFF_START
        _next_connect_at = 0.0
        return True
    now = time.time()
    if not force and now < _next_connect_at:
        BROKER_STATUS = "DISCONNECTED"
        return False
    if not CONN_LOCK.acquire(blocking=False):
        # Otro hilo ya está reconectando: esperar, no duplicar.
        return False
    try:
        BROKER_STATUS = "DISCONNECTED"
        if iq.connect():
            BROKER_STATUS = "CONNECTED"
            _conn_backoff = CONN_BACKOFF_START
            _next_connect_at = 0.0
            return True
        _next_connect_at = time.time() + _conn_backoff
        log.warning(f"[CONN] Nuevo intento de conexión en {_conn_backoff:.0f}s "
                    f"(fallos acumulados: {iq.connect_failures}).")
        _conn_backoff = min(_conn_backoff * 2, CONN_BACKOFF_MAX)
        return False
    except BaseException as e:
        log.error(f"[CONN] Error en reconexión: {type(e).__name__}: {e}")
        BROKER_STATUS = "DISCONNECTED"
        _next_connect_at = time.time() + _conn_backoff
        _conn_backoff = min(_conn_backoff * 2, CONN_BACKOFF_MAX)
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
        self.fail_count = 0          # refrescos fallidos consecutivos
        # Un único hilo para el catálogo. OJO: antes se usaba
        # 'with ThreadPoolExecutor(...)' y su shutdown(wait=True) esperaba al
        # hilo colgado, anulando el timeout de 45 s: el refresco del catálogo
        # podía quedarse bloqueado para siempre.
        self._executor = ThreadPoolExecutor(max_workers=1,
                                            thread_name_prefix="iq-init")
        # Tolerancia: si un refresco del catálogo falla de forma transitoria
        # (STALE), seguimos usando el último snapshot conocido durante esta
        # ventana en lugar de cerrar TODO el mercado.
        self.stale_grace = 600.0

    def refresh(self):
        if iq.api is None:
            self._mark_stale("cliente IQ no inicializado")
            return
        future = None
        try:
            future = self._executor.submit(iq.api.get_all_init_v2)
            data = future.result(timeout=IQ_INIT_TIMEOUT + 10)
        except Exception as e:
            if future is not None:
                try:
                    future.cancel()      # nunca esperar al hilo: puede estar colgado
                except Exception:
                    pass
            self._mark_stale(f"{type(e).__name__}: {e}")
            return
        if not isinstance(data, dict):
            self._mark_stale("get_all_init_v2 no devolvió un diccionario")
            return

        parsed = {}
        for option in ("turbo", "binary", "blitz"):
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
                # Vencimientos disponibles (segundos): 60 = 1 min, 300 = 5 min...
                # El MISMO activo aparece en varias listas (turbo/binary/blitz);
                # se hace la UNIÓN para conocer si se puede operar a 1 minuto.
                opt = v.get("option") or {}
                exp = sorted({int(x) for x in (opt.get("expiration_times") or []) if str(x).isdigit() or isinstance(x, int)})
                if name not in parsed:
                    parsed[name] = {
                        "id": _aid,
                        "type": option.upper(),
                        "open": enabled and not suspended,
                        "suspended": suspended,
                        "source": "IQ_OPTION",
                        "desc": str(v.get("description", "")),
                        "exchange": str(v.get("exchange", "")),
                        "exp_times": exp,
                    }
                else:
                    # fusionar expiraciones y considerar abierto si alguna lista lo tiene
                    base = parsed[name]
                    base["exp_times"] = sorted(set(base.get("exp_times") or []) | set(exp))
                    if enabled and not suspended:
                        base["open"] = True
                        base["suspended"] = False
                    if not base["desc"]:
                        base["desc"] = str(v.get("description", ""))
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
            self.fail_count = 0
        abiertos = sum(1 for a in parsed.values() if a.get("open"))
        log.info(f"[BROKER] Disponibilidad LIVE: {len(parsed)} instrumentos ({abiertos} abiertos)")

    def _mark_stale(self, msg):
        with self.lock:
            self.status = "STALE" if self.assets else "UNKNOWN"
            self.last_error = msg
            self.fail_count = getattr(self, "fail_count", 0) + 1
        log.warning(f"[BROKER] Disponibilidad {self.status}: {msg}")
        # Si el catálogo falla muchas veces seguidas, la sesión de la librería
        # quedó degradada (get_all_init_v2 deja de responder): renovar la sesión.
        if self.fail_count >= 3:
            with self.lock:
                self.fail_count = 0
            log.warning("[BROKER] Catálogo fallando repetidamente → renovando sesión IQ...")
            try:
                threading.Thread(target=_recover_iq_session, daemon=True).start()
            except Exception:
                pass

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


def _recover_iq_session():
    """Renueva la sesión IQ cuando el catálogo (get_all_init_v2) falla varias
    veces seguidas (sesión degradada). Serializada con CONN_LOCK para no
    reconectar dos veces a la vez."""
    if not CONN_LOCK.acquire(blocking=False):
        return
    try:
        log.info("[RECOVER] Renovando sesión IQ (reconexión completa)...")
        if iq.rebuild():
            log.info("[RECOVER] Sesión renovada. Refrescando catálogo...")
            broker_market.refresh()
        else:
            log.warning("[RECOVER] No se pudo renovar la sesión (se reintentará).")
    except Exception as e:
        log.error(f"[RECOVER] error: {e}")
    finally:
        CONN_LOCK.release()


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
    if iq.is_connected():
        broker_market.refresh()
    if BROKER_DIAGNOSTIC:
        run_broker_diagnostic()
    while True:
        time.sleep(120)
        # Sin sesión el catálogo no puede responder: no gastar 35 s esperando.
        if not iq.is_connected():
            continue
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
        self.last_market_refresh = 0.0
        self.last_clock_sync = 0.0
        self._last_live_refresh = 0.0
        self._zero_tradable_streak = 0
        self._last_session_recovery = 0.0
        # Latido del bucle: lo vigila WatchdogWorker DESDE FUERA (un vigilante
        # dentro del propio bucle no sirve de nada si el bucle se bloquea).
        self.last_heartbeat = time.time()
        self.last_cycle_ts = 0.0
        self.last_warn_ts = 0.0

    def execute(self):
        """Una pasada del bucle. REGLA DE ORO: aquí NO se hace trabajo de red.

        Antes este método llamaba a refresh_market_sessions(), que pide el
        histórico de hasta 40 activos por la conexión compartida. Cuando eso
        tardaba (arranque, reconexión, rollover) el bucle quedaba bloqueado
        decenas de segundos o minutos: el panel se congelaba y los ciclos dejaban
        de contarse ("el scanner se detiene solo"). Ahora todo el trabajo de red
        vive en hilos propios (MarketRefreshWorker / CandleStreamManager) y aquí
        sólo se leen caché y streams, que son operaciones locales e inmediatas.
        """
        self.last_heartbeat = time.time()
        now_local = self.last_heartbeat

        if not iq.is_connected():
            # La reconexión puede tardar hasta 60 s (handshake + cambio de
            # balance), así que la hace ConnectionWorker en su propio hilo. Aquí
            # sólo se deja constancia y se sale sin bloquear el bucle.
            self.last_cycle_ts = now_local
            return

        # El reloj del broker se lee de un valor ya recibido por el websocket
        # (no es una petición de red), así que no puede bloquear el bucle.
        if now_local - self.last_clock_sync >= CLOCK_SYNC_INTERVAL:
            iq.sync_clock()
            self.last_clock_sync = now_local

        now_ts = broker_clock.now_ts()
        current_minute = int(now_ts) // 60
        if current_minute == self.last_processed_minute:
            # TABLERO EN VIVO: aunque no haya vela nueva, refrescar el %/valores
            # cada ~4 s con la vela que se está formando (solo visual, no afecta
            # a las señales, que solo se disparan con velas CERRADAS).
            if now_local - self._last_live_refresh >= 4:
                self._last_live_refresh = now_local
                try:
                    self.refresh_proximity_live()
                except Exception as e:
                    log.debug(f"[LIVE] error refresco en vivo: {e}")
            self.last_cycle_ts = now_local
            return

        start = time.perf_counter()

        market_feed.fast_update_active()
        pipeline.process()

        scan_metrics.cycles += 1
        self.cycles = scan_metrics.cycles
        self.last_processed_minute = current_minute
        self.scan_time = round(time.perf_counter() - start, 3)
        self.last_cycle_ts = now_local

        log.info(f"[SCANNER] Ciclo #{self.cycles} completado en {self.scan_time}s | Activos vivos: {len(market_feed.active_assets())}")

    def _check_degraded_session(self, now_local):
        """Sesión 'degradada': el broker responde (catálogo LIVE) pero NINGÚN
        activo entrega velas. Es el síntoma exacto del fallo anterior; en vez de
        esperar indefinidamente se renueva la sesión (con enfriamiento)."""
        if not broker_market.valid():
            self._zero_tradable_streak = 0
            return
        if asset_manager.all_tradable():
            self._zero_tradable_streak = 0
            return
        self._zero_tradable_streak += 1
        if self._zero_tradable_streak < 3:
            return
        if now_local - self._last_session_recovery < 300:
            return
        self._last_session_recovery = now_local
        self._zero_tradable_streak = 0
        log.warning("[MARKET] 3 refrescos seguidos sin ningún activo con velas. "
                    "Renovando la sesión con el broker...")
        threading.Thread(target=_recover_iq_session, daemon=True).start()

    def refresh_proximity_live(self):
        """Refresco SOLO visual del tablero: recalcula los indicadores con la vela
        que se está formando (realtime) cada ~4 s para que los porcentajes no se
        vean 'tarde'. NO toca la máquina de estados ni dispara señales (esas solo
        usan velas CERRADAS en el pipeline)."""
        if not candle_streams.active_count():
            return
        for asset in asset_manager.all_tradable():
            candles = candle_streams.get_realtime(asset)
            if not candles or len(candles) < 30:
                continue
            try:
                live = calculate_indicators(candles, include_forming=True)
            except Exception:
                continue
            if live is None:
                continue
            state = gsr_memory.get(asset)
            proximity_engine.update(asset, state, live)

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
# HILOS DE SERVICIO: refresco de mercado y vigilante del bucle
# ==========================================================
class MarketRefreshWorker:
    """Revalida el universo con el broker en su PROPIO hilo.

    Incluye lecturas al broker (histórico de velas) que pueden tardar decenas de
    segundos. Al sacarlas del bucle del escáner, por muy lento que esté el
    broker los ciclos siguen avanzando y el panel no se congela."""

    def __init__(self):
        self._stop = threading.Event()
        self._wake = threading.Event()
        self.last_run = 0.0
        self.last_duration = 0.0
        self.runs = 0

    def wake(self):
        """Pide un refresco inmediato (p. ej. tras reconectar)."""
        self._wake.set()

    def run(self):
        # Pequeña espera inicial: deja que el catálogo del broker llegue.
        self._stop.wait(3)
        while not self._stop.is_set():
            espera = MARKET_REFRESH_INTERVAL
            if self._wake.is_set():
                self._wake.clear()
                espera = 0.0
            try:
                if not iq.is_connected():
                    espera = 5.0
                elif not broker_market.valid():
                    # El catálogo del broker todavía no ha llegado: reintentar pronto
                    # en lugar de esperar un minuto entero con el escáner a ciegas.
                    espera = 5.0
                else:
                    inicio = time.perf_counter()
                    market_feed.refresh_market_sessions()
                    self.last_duration = time.perf_counter() - inicio
                    self.last_run = time.time()
                    self.runs += 1
                    scan_controller.last_market_refresh = self.last_run
                    scan_controller._check_degraded_session(self.last_run)
                    if self.last_duration > 30:
                        # No es un problema de escaneo (corre aparte), pero conviene
                        # saberlo: significa que el broker está lento.
                        log.warning(f"[MARKET] El refresco de sesiones tardó "
                                    f"{self.last_duration:.1f}s (no afecta al escaneo: "
                                    f"corre en su propio hilo).")
            except Exception as e:
                log.error(f"[MARKET] Error en el refresco de sesiones: {e}")
            self._stop.wait(max(5.0, espera))


market_refresh_worker = MarketRefreshWorker()


class ConnectionWorker:
    """Mantiene viva la sesión con el broker, fuera del bucle del escáner.

    `ensure_connected()` puede tardar hasta 60 s (handshake + change_balance), de
    modo que llamarlo desde el bucle lo bloqueaba. Aquí se reintenta cada 5 s y la
    propia espera exponencial de `ensure_connected()` evita martillear al broker.
    """

    def __init__(self):
        self._stop = threading.Event()
        self.reconnects = 0

    def run(self):
        while not self._stop.is_set():
            try:
                if not iq.is_connected():
                    if ensure_connected():
                        self.reconnects += 1
                        log.info("[CONN] Sesión restablecida por el hilo de conexión; "
                                 "se pide un refresco de mercado.")
                        market_refresh_worker.wake()
            except Exception as e:
                log.error(f"[CONN] Error en el hilo de conexión: {e}")
            self._stop.wait(5)


connection_worker = ConnectionWorker()


class WatchdogWorker:
    """Vigila DESDE FUERA que el bucle del escáner siga latiendo.

    Un vigilante dentro del propio bucle no sirve: si el bucle se bloquea, el
    vigilante se bloquea con él. Este hilo avisa en el log y, si el parón
    persiste, renueva la sesión con el broker."""

    def __init__(self):
        self._stop = threading.Event()
        self.started_at = time.time()
        self._last_log = 0.0
        self._last_hb_log = time.time()

    def status(self) -> dict:
        ahora = time.time()
        parado = ahora - scan_controller.last_heartbeat
        sin_ciclo = ahora - (scan_controller.last_cycle_ts or ahora)
        if parado > STALL_RECOVER:
            salud = "ATASCADO"
        elif parado > STALL_WARN or sin_ciclo > STALL_WARN:
            salud = "DEGRADADO"
        else:
            salud = "OK"
        _app = _estado_app()
        return {
            "salud": salud,
            "uptime_s": round(ahora - self.started_at, 1),
            "latido_hace_s": round(parado, 1),
            "ultimo_ciclo_hace_s": round(sin_ciclo, 1),
            "ciclos": scan_controller.cycles,
            "activos_vivos": len(market_feed.active_assets()),
            "activos_operables": len(asset_manager.all_tradable()),
            "streams": candle_streams.active_count(),
            "broker": BROKER_STATUS,
            "disponibilidad": broker_market.status,
            "llamadas_colgadas": IQ_ABANDONED_CALLS,
            "refresco_mercado_hace_s": (round(ahora - market_refresh_worker.last_run, 1)
                                        if market_refresh_worker.last_run else None),
            "refresco_mercado_s": round(market_refresh_worker.last_duration, 2),
            # Diagnóstico ampliado (auditoría): memoria del historial de patrones,
            # oscilación de activos y estado de las integraciones.
            "pattern_history_size": len(pattern_history),
            "pattern_history_max": PATTERN_HISTORY_MAX,
            "stale_streak_max": max(market_feed._stale_streak.values(), default=0),
            "stale_tolerance": STALE_TOLERANCE,
            # Publicación de señales en Alí Binary Options
            "app_status": _app["state"],
            "app_detail": _app["detail"],
            "app_signal_url_configurada": bool(APP_SIGNAL_URL),
            "firestore_directo_configurado": bool(FIREBASE_SA_PATH),
            "ws_auth": bool(WS_AUTH_TOKEN),
            "indicators_engine": INDICATORS_ENGINE,
            "log_file": LOG_FILE,
        }

    def run(self):
        while not self._stop.is_set():
            self._stop.wait(15)
            ahora = time.time()
            parado = ahora - scan_controller.last_heartbeat

            # Latido informativo: deja constancia en el log de que sigue vivo y
            # con qué salud, para poder verlo sin abrir el panel.
            if ahora - self._last_hb_log > HEARTBEAT_LOG:
                self._last_hb_log = ahora
                st = self.status()
                log.info(f"[WATCHDOG] {st['salud']} | ciclos={st['ciclos']} | "
                         f"activos={st['activos_vivos']} | streams={st['streams']} | "
                         f"broker={st['broker']} | APP={st['app_status']} | "
                         f"refresco_mercado_cada={st['refresco_mercado_s']}s")

            if parado <= STALL_WARN:
                continue
            if ahora - self._last_log > 60:
                self._last_log = ahora
                log.warning("[WATCHDOG] El bucle del escáner lleva "
                            f"{parado:.0f}s sin dar señales de vida "
                            f"(activos: {len(market_feed.active_assets())}, "
                            f"streams: {candle_streams.active_count()}, "
                            f"broker: {BROKER_STATUS}, "
                            f"llamadas colgadas: {IQ_ABANDONED_CALLS}). "
                            "El trabajo de red corre en hilos aparte, así que esto "
                            "no debería ocurrir; se está recopilando el estado.")
            if (parado > STALL_RECOVER
                    and ahora - scan_controller._last_session_recovery > 300):
                scan_controller._last_session_recovery = ahora
                log.error(f"[WATCHDOG] Parón de {parado:.0f}s: renovando la sesión "
                          "con el broker para recuperar el escaneo...")
                threading.Thread(target=_recover_iq_session, daemon=True).start()


watchdog_worker = WatchdogWorker()

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
  .cond-tag { font-weight: 700; color: #ffbc5e; }

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
  <div class="version-tag" id="app_badge" title="Estado de la publicación de señales en Ali Binary Options">APP …</div>
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
            <th title="Cuántas de las 3 condiciones GSR (BB, RSI, DAMOA) se cumplen AHORA. NO es una señal: la señal exige el patrón secuencial completo.">Cond.</th>
            <th title="Avance del PATRÓN GSR: 0% observando · 25% 1ª vela · 50% 2ª vela · 100% patrón validado (LISTA). Es distinto de las condiciones.">Avance Patrón</th>
            <th>Filtro Faltante</th>
          </tr>
        </thead>
        <tbody id="proximity_v2_board">
          <tr><td colspan="9" style="text-align:center; color:#8b949e; padding: 30px;">Sincronizando feed de activos reales...</td></tr>
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
      <div class="metric-item">
        <div class="metric-label">1 min disponibles</div>
        <div class="metric-value" id="val_1min" style="color: #00ff88;">0</div>
      </div>
      <div class="metric-item">
        <div class="metric-label">Descartados sin 1 min</div>
        <div class="metric-value" id="val_no1min" style="color: #8b949e;">0</div>
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
let WS_TOKEN = "__WS_TOKEN__";
let ws = new WebSocket("ws://" + location.host + "/ws" + (WS_TOKEN ? "?token=" + encodeURIComponent(WS_TOKEN) : ""));

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
    document.getElementById('val_1min').innerText = d.status.assets_1min;
    document.getElementById('val_no1min').innerText = d.status.assets_no_1min;

    document.getElementById('st_broker').innerText = d.status.broker;
    document.getElementById('st_availability').innerText = d.status.availability;
    document.getElementById('st_candles').innerText = d.status.candles;
    document.getElementById('st_scanner').innerText = d.status.scanner;

    document.getElementById('val_cycles').innerText = d.status.cycles;
    document.getElementById('val_timer').innerHTML = d.status.next_candle + "<span>s</span>";
    document.getElementById('val_avg_latency').innerText = d.status.avg_latency;

    /* ===== Aviso de publicación en Ali Binary Options ===== */
    const app = document.getElementById('app_badge');
    if (app) {
        const st = d.status.app_status || 'OFF';
        app.innerText = st === 'ON' ? 'APP ON' : 'APP OFF';
        app.style.background = st === 'ON' ? 'rgba(63,185,80,0.15)' : 'rgba(245,158,11,0.18)';
        app.style.color = st === 'ON' ? '#3fb950' : '#f59e0b';
        app.style.borderColor = st === 'ON' ? 'rgba(63,185,80,0.35)' : 'rgba(245,158,11,0.4)';
        app.title = (d.status.app_detail || '') +
            (st === 'ON' ? '' : ' — las señales NO se están publicando en la app');
    }

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
              <td><span class="cond-tag" title="Condiciones cumplidas ahora (BB/RSI/DAMOA)">${row.conditions_met || 0}/${row.conditions_total || 3}</span></td>
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
        board.innerHTML = '<tr><td colspan="9" style="text-align:center; color:#8b949e; padding: 30px;">' + emptyMsg + '</td></tr>';
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
                        "assets_1min": scan_metrics.assets_1min,
                        "assets_no_1min": scan_metrics.assets_no_1min,
                        "app_status": _estado_app()["state"],
                        "app_detail": _estado_app()["detail"],
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
    log.info("=" * 60)
    log.info(f"INICIO DEL ESCÁNER · {APP_NAME} {VERSION} · cuenta {IQ_MODE} · "
             f"puerto {PORT} · pid {os.getpid()}")
    log.info("=" * 60)
    try:
        ensure_connected()
    except Exception as e:
        log.error(f"Error durante la inicialización de sesión con el broker: {e}")
    finally:
        # Hilos de servicio, cada uno con una única responsabilidad. El bucle del
        # escáner NO hace trabajo de red: sólo lee caché y streams.
        threading.Thread(target=_broker_state_worker, daemon=True,
                         name="broker-state").start()
        threading.Thread(target=candle_streams.run, daemon=True,
                         name="candle-streams").start()
        threading.Thread(target=connection_worker.run, daemon=True,
                         name="connection").start()
        threading.Thread(target=market_refresh_worker.run, daemon=True,
                         name="market-refresh").start()
        threading.Thread(target=watchdog_worker.run, daemon=True,
                         name="watchdog").start()
        APP.start_scanner()

def _acceso_permitido(scope, cliente) -> bool:
    """Control de acceso del panel, del WebSocket y de la API.

    - Sin WS_AUTH_TOKEN: todo permitido (modo desarrollo, como hasta ahora).
    - Con token: se exige `?token=...` en la URL o la cabecera `X-Auth-Token`.
    - Desde el propio equipo (127.0.0.1 / ::1) se permite siempre: así un token
      olvidado nunca te deja fuera de tu panel, pero cualquier OTRO equipo de la
      red sí necesita el token (el servidor escucha en 0.0.0.0).
    """
    if not WS_AUTH_TOKEN:
        return True
    if cliente in ("127.0.0.1", "::1", "localhost"):
        return True
    try:
        from urllib.parse import unquote
        qs = scope.get("query_string") or b""
        for parte in qs.decode("utf-8", "ignore").split("&"):
            if parte.startswith("token=") and unquote(parte[6:]) == WS_AUTH_TOKEN:
                return True
    except Exception:
        pass
    for clave, valor in (scope.get("headers") or []):
        if clave.lower() == b"x-auth-token":
            try:
                if valor.decode("utf-8", "ignore").strip() == WS_AUTH_TOKEN:
                    return True
            except Exception:
                pass
    return False


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
        cliente = (scope.get('client') or [None])[0]
        if not _acceso_permitido(scope, cliente):
            await send({
                'type': 'http.response.start',
                'status': 401,
                'headers': [[b'content-type', b'application/json; charset=utf-8']],
            })
            await send({'type': 'http.response.body', 'body': json.dumps(
                {"error": "No autorizado. Añade ?token=... a la URL "
                          "(o define WS_AUTH_TOKEN vacío para desactivar el control)."}
            ).encode('utf-8')})
            return
        if path in ['/', '']:
            # El token se inyecta en el HTML para que el cliente WebSocket pueda
            # autenticarse sin que el usuario lo escriba a mano cada vez.
            html = HTML.replace("__WS_TOKEN__", WS_AUTH_TOKEN)
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [[b'content-type', b'text/html; charset=utf-8']],
            })
            await send({'type': 'http.response.body', 'body': html.encode('utf-8')})
        elif path == '/api/proximity-v2':
            data = json.dumps(proximity_engine.top(), ensure_ascii=False)
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [[b'content-type', b'application/json']],
            })
            await send({'type': 'http.response.body', 'body': data.encode('utf-8')})
        elif path in ('/api/status', '/api/health'):
            # Estado del motor en JSON: sirve para comprobar por HTTP si el
            # escáner sigue avanzando (o se ha quedado atascado) sin abrir el panel.
            estado = watchdog_worker.status()
            data = json.dumps(estado, ensure_ascii=False)
            await send({
                'type': 'http.response.start',
                'status': 200 if estado["salud"] != "ATASCADO" else 503,
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
        cliente = (scope.get('client') or [None])[0]
        if not _acceso_permitido(scope, cliente):
            log.warning(f"[SEGURIDAD] WebSocket rechazado (sin token válido) desde {cliente}")
            await send({'type': 'websocket.close', 'code': 4401})
            return
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
    lib_state = "OK (blindada)" if (IQ_AVAILABLE and IQ_LIB_HARDENED) else conn_status
    print(f"PYTHON            : {sys.version.split()[0]}  ({sys.executable})")
    print(f"LIBRERÍA BROKER   : {lib_state}")
    print(f"CREDENCIALES      : {'OK' if (IQ_EMAIL and IQ_PASSWORD) else 'FALTAN en .env (simulación)'}")
    print(f"CUENTA IQ         : {IQ_MODE}")
    print(f"BROKER CONNECTION : {'OK' if iq.is_connected() else 'SE CONECTA AL ARRANCAR'}")
    print(f"BROKER CLOCK      : {broker_clock.status.value}")
    print(f"UTC CLOCK         : {broker_clock.now_utc().strftime('%H:%M:%S')}")
    print(f"COLOMBIA CLOCK    : {broker_clock.now_colombia().strftime('%H:%M:%S')}")
    print(f"AUDITORÍA CSV     : {AUDIT_FILE}")
    print(f"PANEL WEB         : http://localhost:{PORT}/")
    print("====================================================\n")

if __name__ == "__main__":
    # La consola de Windows (cp1252) no soporta los emojis del log: sin esto
    # logging lanza UnicodeEncodeError y se pierden mensajes.
    for _stream in (sys.stdout, sys.stderr):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    print_initial_diagnostics()
    # Se pasa el objeto ASGI directamente. Con la cadena "app_ali:app" uvicorn
    # volvía a importar el módulo: todo el estado (reloj, cliente IQ, cachés,
    # hilos) quedaba DUPLICADO en dos copias del módulo en el mismo proceso.
    uvicorn.run(app, host=HOST, port=PORT, log_level="info", use_colors=False)