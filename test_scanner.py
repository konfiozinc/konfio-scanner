# -*- coding: utf-8 -*-
"""
==========================================================
AppALÍ GSR Scanner - AUTO-TEST FUNCIONAL (sin broker)
==========================================================
Comprueba, sin necesidad de conexión ni credenciales, las piezas que se
rompieron y que provocaban los fallos históricos del escáner:

  1. Parches de la librería iqoptionapi (bucles infinitos con timeout).
  2. Compatibilidad de los callbacks con websocket-client 1.x
     (el socket conectaba pero NO procesaba ningún mensaje).
  3. Validación del reloj del broker (rechaza desfases imposibles).
  4. Fusión correcta de la caché de velas (sin congelar el cierre parcial).
  5. Universo de candidatos, incluso sin catálogo del broker.
  6. Patrón GSR COMPLETO con velas sintéticas: 1ª vela, 2ª vela, LISTA y
     disparo de la señal en la vela de entrada correcta.
  7. Horarios de sesión y nombres de activo.

Uso:
    venv311\\Scripts\\python.exe test_scanner.py
    (o doble clic en ejecutar_tests.bat)

Devuelve código 0 si todo pasa y 1 si hay algún fallo.
"""
import json
import os
import sys
import tempfile
import time

# Permite ejecutarlo desde cualquier carpeta: la raíz del proyecto es la carpeta
# donde vive este archivo.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Los tests NO deben escribir en la bitácora de producción (scanner.log): si no,
# cada corrida deja sus señales simuladas mezcladas con las reales y hace
# imposible saber qué pasó de verdad. Se avisa al módulo ANTES de importarlo.
os.environ["APPALI_NO_FILE_LOG"] = "1"

import app_ali as A  # noqa: E402

FALLOS = []


def check(nombre, cond, extra=""):
    print(f"[{'OK  ' if cond else 'FALLA'}] {nombre} {extra}")
    if not cond:
        FALLOS.append(nombre)


print("=" * 72)
print("  AUTO-TEST FUNCIONAL DEL SCANNER AppALÍ (sin broker)")
print("=" * 72)

# ------------------------------------------------------------------ 1. parches
print("\n-- 1. Blindaje de la librería iqoptionapi --")
check("librería iqoptionapi disponible", A.IQ_AVAILABLE)
check("parches de librería aplicados", A.IQ_LIB_HARDENED)
check("get_candles ya acepta timeout",
      "_timeout" in A.IQ_Option.get_candles.__code__.co_varnames)
check("re_subscribe_stream neutralizado (no bloquea la reconexión)",
      A.IQ_Option.re_subscribe_stream.__code__.co_argcount == 1)
check("callbacks del websocket adaptados", A.IQ_WS_PATCHED)

# El fallo real: websocket-client 1.x llama on_message(ws, msg) y la librería
# sólo aceptaba (msg) -> TypeError en CADA mensaje (socket conectado pero mudo).
try:
    from unittest.mock import MagicMock
    from iqoptionapi.ws.client import WebsocketClient

    stub_api = MagicMock()
    stub_api.wss_url = "wss://127.0.0.1:1/"
    client = WebsocketClient(stub_api)
    msg = json.dumps({"name": "timeSync", "msg": 1780000000123})
    client.on_message(client.wss, msg)          # convención de websocket-client 1.x
    check("on_message(ws, msg) de 1.x se procesa",
          stub_api.timesync.server_timestamp == 1780000000123,
          f"(valor={stub_api.timesync.server_timestamp})")
    client.on_message(msg)                      # convención antigua (0.56)
    check("on_message(msg) antiguo sigue funcionando",
          stub_api.timesync.server_timestamp == 1780000000123)
    client.on_close(client.wss, 1000, "bye")    # 1.x envía 3 argumentos
    check("on_close(ws, code, reason) no rompe", True)
    client.on_open(client.wss)
    check("on_open(ws) no rompe", True)
except Exception as e:
    check("callbacks del websocket", False, f"{type(e).__name__}: {e}")

# ------------------------------------------------------------- 2. reloj broker
print("\n-- 2. Reloj del broker --")
A.broker_clock.reset_to_local()
check("rechaza timestamp de 1970 (timeSync sin llegar)", A.broker_clock.sync_with_broker(1_780_000.0) is False)
check("rechaza desfase de 500 s", A.broker_clock.sync_with_broker(time.time() + 500) is False)
check("acepta desfase creíble de +2 s", A.broker_clock.sync_with_broker(time.time() + 2) is True)
check("estado del reloj = SYNCED", A.broker_clock.status is A.ClockStatus.SYNCED)
A.broker_clock.reset_to_local()
check("vuelve a LOCAL_FALLBACK", A.broker_clock.status.value == "LOCAL_FALLBACK")
check("now_ts vuelve al reloj local", abs(A.broker_clock.now_ts() - time.time()) < 1.0)

# ---------------------------------------------------------- 3. caché de velas
print("\n-- 3. Caché de velas --")
A.candle_cache.cache.clear()
T0 = 1_700_000_000


def mk(ts, o, c):
    return A.Candle(ts, o, max(o, c), min(o, c), c, 1.0)


A.candle_cache.upsert("X", [mk(T0, 1.0, 1.0), mk(T0 + 60, 1.0, 1.0)])
check("inicializa la caché", len(A.candle_cache.get("X")) == 2)
A.candle_cache.upsert("X", [mk(T0 + 60, 1.0, 1.5)])
check("REFRESCA la vela con el mismo timestamp (no la congela)",
      A.candle_cache.get("X")[-1].close == 1.5,
      f"(close={A.candle_cache.get('X')[-1].close})")
A.candle_cache.upsert("X", [mk(T0 + 180, 2.0, 2.5)])
vals = [c.timestamp for c in A.candle_cache.get("X")]
check("rellena huecos sin duplicar", vals == [T0, T0 + 60, T0 + 180], f"{vals}")

# ------------------------------------------------------------ 4. universo
print("\n-- 4. Universo de candidatos --")
guardado = A.broker_market.assets
A.broker_market.assets = {}
cands = A.asset_manager.get_candidate_list()
check("universo de respaldo sin catálogo del broker", 0 < len(cands) <= A.MAX_UNIVERSE, f"({len(cands)})")
check("incluye variantes OTC", any(a.endswith("-OTC") for a, _ in cands))
check("incluye variantes REAL", any(t == "REAL" for _, t in cands))
check("no incluye pares prohibidos", not any(a in A.FORBIDDEN_REAL_M1 for a, _ in cands))
A.broker_market.assets = guardado

# -------------------------------------------------- 5. patrón GSR completo
print("\n-- 5. Patrón GSR de principio a fin --")
A.SESSION_FILTER_ENABLED = False          # el test no depende de la hora real
# Sin canal externo configurado: enviar_alerta_app/firestore salen sin tocar la red.
A.APP_SIGNAL_URL = ""
A.FIREBASE_SA_PATH = ""
A.audit_logger.filename = os.path.join(tempfile.gettempdir(), "test_scanner_audit.csv")
if os.path.exists(A.audit_logger.filename):
    os.remove(A.audit_logger.filename)

ASSET = "EURUSD-OP"
A.candle_cache.cache.pop(ASSET, None)
A.gsr_memory.reset(ASSET)
A.pattern_history.clear()

# 210 velas planas y luego el patrón: 2 verdes de impulso fuera de la banda
# superior, una continuación, la vela roja de reversión y la vela de ENTRADA.
serie = [mk(T0 - (210 - i) * 60, 1.0, 1.0) for i in range(210)]
patron = [
    mk(T0, 1.0000, 1.0050),        # impulso 1 (verde, cuerpo fuera de la banda)
    mk(T0 + 60, 1.0050, 1.0100),   # impulso 2 (verde, cuerpo fuera de la banda)
    mk(T0 + 120, 1.0100, 1.0150),  # continuación (RSI y DAMOA en extremo)
    mk(T0 + 180, 1.0150, 1.0040),  # reversión (roja y rompe el cierre de la 2ª)
    mk(T0 + 240, 1.0040, 1.0042),  # vela de ENTRADA (en formación)
]

senales = []
A.validate_signal_eligibility = lambda asset: (True, "OK")   # simula broker OK
_orig_fire = A.fire_signal


def fire_spy(asset, opening, indicator):
    _orig_fire(asset, opening, indicator)
    st = A.gsr_memory.get(asset)
    if st.signal_sent:
        senales.append((asset, st.direction, opening.timestamp))


A.fire_signal = fire_spy

fases = []
for c in serie + patron:
    A.candle_cache.upsert(ASSET, [c])
    A.market_feed.market[ASSET] = A.candle_cache.get(ASSET)
    A.pipeline.process_asset(ASSET)
    fases.append(A.gsr_memory.get(ASSET).phase.name)

print("   secuencia real:", " -> ".join(dict.fromkeys(fases)))
check("detecta la 1ª vela de impulso", "PRIMERA_VELA" in fases)
check("detecta la 2ª vela de impulso", "SEGUNDA_VELA" in fases)
check("llega a LISTA (patrón validado)", "LISTA" in fases)
check("dispara la señal tras la confirmación", len(senales) == 1, f"{senales}")
if senales:
    check("dirección PUT (2 verdes en la banda superior)", senales[0][1] == "PUT")
    check("entrada en la vela siguiente a la confirmación",
          senales[0][2] == T0 + 240, f"(ts={senales[0][2]}, esperado={T0 + 240})")

A.pipeline.process_asset(ASSET)
check("no repite la misma señal", len(senales) == 1)

# ---------------------------------------------------- 6. patrones inválidos
print("\n-- 6. Rechazo de patrones inválidos --")
A.gsr_memory.reset("TEST-GAP")
A.candle_cache.upsert("TEST-GAP", [mk(T0, 1.0000, 1.0050)])
st = A.gsr_memory.get("TEST-GAP")
st.phase = A.GSRPhase.PRIMERA_VELA
st.first_candle = T0
st.direction = "PUT"
ind = A.calculate_indicators([mk(T0 - 60 * i, 1.0, 1.005 if i == 0 else 1.0) for i in range(60, 0, -1)])
check("descarta la 2ª vela si NO es consecutiva",
      A.detect_second_candle("TEST-GAP", mk(T0 + 120, 1.0, 1.5), ind) is False)

# ------------------------------------------------------- 7. sesiones/horarios
print("\n-- 7. Sesiones y nombres de activo --")
import calendar  # noqa: E402

sabado = calendar.timegm(time.strptime("2026-01-03 12:00", "%Y-%m-%d %H:%M"))    # sábado
miercoles = calendar.timegm(time.strptime("2026-01-07 12:00", "%Y-%m-%d %H:%M"))  # miércoles
rollover = calendar.timegm(time.strptime("2026-01-07 21:30", "%Y-%m-%d %H:%M"))
check("no opera sábado (forex real)", A.is_forex_real_schedule_open(sabado) is False)
check("sí opera miércoles 12:00 UTC (forex real)", A.is_forex_real_schedule_open(miercoles) is True)
check("filtro de liquidez descarta sábado", A.is_liquid_session(sabado) is False)
check("filtro de liquidez acepta miércoles 12:00 UTC", A.is_liquid_session(miercoles) is True)
check("filtro de liquidez descarta el rollover (21:00-22:00 UTC)",
      A.is_liquid_session(rollover) is False)

check("nombre visible OTC", A.format_asset_display_name("EURUSD-OTC") == "EUR/USD (OTC)")
check("nombre visible OP", A.format_asset_display_name("EURUSD-OP") == "EUR/USD (OP)")
check("nombre simple del par real", A.format_asset_display_name("EURUSD") == "EUR/USD")
check("nombre para buscar en el broker", A.broker_search_name("EURUSD-OP") == "EUR/USD")
check("nombre para buscar OTC", A.broker_search_name("EURUSD-OTC") == "EUR/USD (OTC)")

# ------------------------------------------- 8. umbral de ruptura por tipo
print("\n-- 8. Umbral de ruptura separado por tipo de activo --")
check("por defecto REAL y OTC comparten el umbral calibrado",
      A.BODY_OUTSIDE_REAL == A.BODY_OUTSIDE_OTC == 40.0,
      f"({A.BODY_OUTSIDE_REAL}/{A.BODY_OUTSIDE_OTC})")
check("umbral aplicable a OTC", A.body_outside_umbral("EURUSD-OTC") == A.BODY_OUTSIDE_OTC)
check("umbral aplicable a REAL", A.body_outside_umbral("EURUSD-OP") == A.BODY_OUTSIDE_REAL)
check("umbral aplicable al par simple", A.body_outside_umbral("EURUSD") == A.BODY_OUTSIDE_REAL)

# Se mide el % real de una vela de impulso y se comprueba que el umbral por tipo
# decide de verdad (con un valor entre 40 y 55, el OTC endurecido filtra).
_a, _o = A.BODY_OUTSIDE_REAL, A.BODY_OUTSIDE_OTC
try:
    # Igual que en el pipeline: el indicador se calcula sobre velas[:-1], así que
    # para evaluar la vela de impulso hay que añadir después una vela en
    # formación (que es la que se descarta).
    _impulso = mk(T0 + 300, 1.0000, 1.0050)
    _velas = serie + [_impulso, mk(T0 + 360, 1.0050, 1.0052)]
    _ind = A.calculate_indicators(_velas)          # evalúa la última CERRADA
    _bb = A.body_outside_upper(_impulso, _ind.bb_upper)
    check("la vela de impulso rompe entre 40% y 55%", 40.0 < _bb < 55.0, f"(BB={_bb:.1f}%)")

    A.BODY_OUTSIDE_OTC = 40.0
    A.gsr_memory.reset("TH-OTC")
    check("con umbral 40% el OTC arranca el patrón",
          A.detect_first_candle("TH-OTC", _impulso, _ind) is True)

    A.BODY_OUTSIDE_OTC = 55.0
    A.gsr_memory.reset("TH-OTC")
    check("con umbral 55% el mismo OTC NO arranca el patrón",
          A.detect_first_candle("TH-OTC", _impulso, _ind) is False)
    A.gsr_memory.reset("TH-OP")
    check("el REAL sigue arrancando el patrón con 40%",
          A.detect_first_candle("TH-OP", _impulso, _ind) is True)

    A.BODY_OUTSIDE_REAL = 55.0
    A.gsr_memory.reset("TH-OP")
    check("endurecer AMBOS umbrales sí frena al REAL",
          A.detect_first_candle("TH-OP", _impulso, _ind) is False)
finally:
    A.BODY_OUTSIDE_REAL, A.BODY_OUTSIDE_OTC = _a, _o
    A.gsr_memory.reset("TH-OTC")
    A.gsr_memory.reset("TH-OP")

# --------------------------------------------------- 9. filtro de volumen
print("\n-- 9. Filtro de volumen (opcional y a prueba de datos inútiles) --")
_vol_act = "VOLTEST-OTC"
A.candle_cache.cache.pop(_vol_act, None)
# 25 velas con volumen 0: es lo que entrega IQ Option en forex/OTC (verificado
# con diagnostico_broker.py). El filtro DEBE auto-desactivarse, no bloquear.
A.candle_cache.upsert(_vol_act, [mk(T0 + i * 60, 1.0, 1.0) for i in range(25)])

A.VOLUME_FILTER_ENABLED = False
_ok, _det = A._volumen_ok(_vol_act)
check("desactivado por defecto: no filtra", _ok is True, f"({_det})")

A.VOLUME_FILTER_ENABLED = True
_ok, _det = A._volumen_ok(_vol_act)
check("con volumen 0 se auto-desactiva (NO bloquea señales)", _ok is True, f"({_det})")

# Con volumen real: la última vela cerrada por debajo de la media -> filtra.
A.candle_cache.cache.pop(_vol_act, None)
_velas_vol = [A.Candle(T0 + i * 60, 1.0, 1.0, 1.0, 1.0, 100.0) for i in range(24)]
_velas_vol.append(A.Candle(T0 + 24 * 60, 1.0, 1.0, 1.0, 1.0, 10.0))   # vela floja
_velas_vol.append(A.Candle(T0 + 25 * 60, 1.0, 1.0, 1.0, 1.0, 100.0))  # en formación
A.candle_cache.upsert(_vol_act, _velas_vol)
_ok, _det = A._volumen_ok(_vol_act)
check("con volumen real y vela floja sí filtra", _ok is False, f"({_det})")

A.VOLUME_FILTER_ENABLED = False
A.candle_cache.cache.pop(_vol_act, None)

# ------------------------------------------- 10. métrica de condiciones
print("\n-- 10. Progreso del patrón vs condiciones (dos métricas distintas) --")
check("0 condiciones", A.progress_engine.calculate({"bb_ok": False, "rsi_ok": False, "damoa_ok": False}) == (0, 3))
check("2 condiciones", A.progress_engine.calculate({"bb_ok": True, "rsi_ok": True, "damoa_ok": False}) == (2, 3))
check("3 condiciones", A.progress_engine.calculate({"bb_ok": True, "rsi_ok": True, "damoa_ok": True}) == (3, 3))
_fila = A.proximity_engine.rows.get(ASSET)
if _fila:
    check("la fila del tablero trae condiciones_met/total",
          "conditions_met" in _fila and "conditions_total" in _fila,
          f"({_fila.get('conditions_met')}/{_fila.get('conditions_total')})")
    check("la fila trae el umbral aplicado", "umbral_bb" in _fila, f"({_fila.get('umbral_bb')}%)")
_filas_cerradas = [f for f in A.proximity_engine.top() if f["status"] != "OPEN"]
check("las filas cerradas también traen la métrica de condiciones",
      all("conditions_met" in f and "umbral_bb" in f for f in _filas_cerradas))

# ------------------------------------------- 11. migración del CSV de auditoría
print("\n-- 11. Auditoría de señales (CSV) --")
import csv  # noqa: E402

_ruta_csv = os.path.join(tempfile.gettempdir(), "test_migracion_audit.csv")
try:
    with open(_ruta_csv, "w", newline="", encoding="utf-8") as _f:
        _w = csv.writer(_f)
        _w.writerow(["Timestamp_UTC", "Fecha_Hora", "Activo", "Direccion", "RSI", "DAMOA"])
        _w.writerow([1787000000, "2026-08-18 00:28:00", "AUD/USD", "PUT", 80.5, 12.4])
    _logger = A.SignalAuditLogger(_ruta_csv)      # debe migrar la cabecera vieja
    with open(_ruta_csv, newline="", encoding="utf-8") as _f:
        _filas = list(csv.reader(_f))
    check("cabecera migrada al esquema nuevo", _filas[0] == A.SignalAuditLogger.COLUMNS,
          f"({len(_filas[0])} columnas)")
    check("la fila antigua se conserva y se rellena",
          len(_filas) == 2 and len(_filas[1]) == len(A.SignalAuditLogger.COLUMNS)
          and _filas[1][:6] == ["1787000000", "2026-08-18 00:28:00", "AUD/USD", "PUT", "80.5", "12.4"],
          f"({_filas[1][:6] if len(_filas) > 1 else 'sin fila'})")

    _logger.record("EURUSD-OTC", "PUT", 1787000100, 84.2, 12.4,
                   {"tipo": "OTC", "bb_pct": 62.3, "umbral_bb": 40.0,
                    "volumen": 0.0, "vol_medio": 0.0, "vol_rel": ""})
    with open(_ruta_csv, newline="", encoding="utf-8") as _f:
        _filas = list(csv.reader(_f))
    check("la señal se registra con el motivo exacto",
          _filas[-1][6:] == ["OTC", "62.3", "40.0", "0.0", "0.0", ""],
          f"({_filas[-1][6:]})")
finally:
    try:
        os.remove(_ruta_csv)
    except Exception:
        pass

# ------------------------------------------- 12. espera adaptativa del histórico
print("\n-- 12. Espera adaptativa por activo terco (no se elimina para siempre) --")
_mf = A.market_feed
_get_real_original = A.iq.get_candles
_get_stream_original = A.candle_streams.get_realtime
try:
    _act = "TERCO-OTC"
    _mf._hist_last.pop(_act, None)
    _mf._hist_wait.pop(_act, None)
    A.candle_streams.get_realtime = lambda a: None
    A.iq.get_candles = lambda *a, **k: None          # el broker no da histórico

    _esperas = []
    for _i in range(6):
        _mf._hist_last[_act] = 0.0                   # simula que ya pasó la espera
        _mf._candles_for(_act)
        _esperas.append(_mf._hist_wait.get(_act))
    check("la espera se duplica en cada fallo", _esperas[:3] == [240.0, 480.0, 900.0], f"{_esperas}")
    check("la espera se topa en HISTORY_BACKOFF_MAX",
          _esperas[-1] == A.HISTORY_BACKOFF_MAX, f"(tope {A.HISTORY_BACKOFF_MAX:.0f}s)")
    check("el activo NO se elimina del monitoreo", _act in _mf._hist_wait)

    A.iq.get_candles = lambda *a, **k: [mk(T0 + i * 60, 1.0, 1.0) for i in range(A.MIN_CANDLES + 5)]
    _mf._hist_last[_act] = 0.0
    _mf._candles_for(_act)
    check("al recuperarse vuelve al intervalo normal",
          _mf._hist_wait.get(_act) == A.HISTORY_BACKOFF, f"({_mf._hist_wait.get(_act)})")
finally:
    A.iq.get_candles = _get_real_original
    A.candle_streams.get_realtime = _get_stream_original
    A.candle_cache.cache.pop("TERCO-OTC", None)
    _mf._hist_last.pop("TERCO-OTC", None)
    _mf._hist_wait.pop("TERCO-OTC", None)

# ------------------------------------------- 13. reloj: promedio de 2 muestras
print("\n-- 13. Reloj: promedio de dos muestras, siempre validado --")


class _ApiDesfase:
    """Devuelve time.time() + desfase, calculado EN EL MOMENTO de cada llamada.

    Ojo: antes esta prueba devolvía instantes fijos calculados a partir de un
    `time.time()` capturado ANTES de las llamadas, así que el resultado dependía
    de lo que tardara el proceso en llegar hasta aquí (test inestable: pasaba en
    una máquina rápida y fallaba en una lenta). Ahora el desfase es exacto."""

    def __init__(self, desfases):
        self.desfases = list(desfases)

    def get_server_timestamp(self):
        return time.time() + self.desfases.pop(0)


class _ApiFija:
    """Devuelve siempre el mismo valor (para simular respuestas absurdas)."""

    def __init__(self, valor):
        self.valor = valor

    def get_server_timestamp(self):
        return self.valor


_api_original = A.iq.api
_connected_original = A.iq.connected
_gap_original = A.CLOCK_SAMPLE_GAP
try:
    A.CLOCK_SAMPLE_GAP = 0.0
    A.iq.connected = True
    A.broker_clock.reset_to_local()
    A.iq.api = _ApiDesfase([2.0, 2.4])
    A.iq.sync_clock()
    _off = A.broker_clock._offset
    check("el offset cae dentro de las dos muestras", 2.0 <= _off <= 2.5, f"(offset={_off:+.3f}s)")
    check("el reloj queda SYNCED", A.broker_clock.status is A.ClockStatus.SYNCED)

    # Un valor absurdo (p. ej. la librería devolviendo time.time()/1000, que son
    # ~1.78e6, año 1970) y un desfase disparatado deben rechazarse: el promedio
    # NO se salta la validación.
    A.broker_clock.reset_to_local()
    A.iq.api = _ApiFija(1_780_000.0)
    A.iq.sync_clock()
    check("un timestamp de 1970 se rechaza",
          A.broker_clock.status.value == "LOCAL_FALLBACK",
          f"(estado={A.broker_clock.status.value})")

    A.iq.api = _ApiDesfase([500.0, 500.0])
    A.iq.sync_clock()
    check("un desfase de 500s se rechaza",
          A.broker_clock.status.value == "LOCAL_FALLBACK",
          f"(estado={A.broker_clock.status.value})")
finally:
    A.iq.api = _api_original
    A.iq.connected = _connected_original
    A.CLOCK_SAMPLE_GAP = _gap_original
    A.broker_clock.reset_to_local()

# ------------------------------------------- 14. ciclo completo del controlador
print("\n-- 14. Un ciclo real del ScanController (con broker simulado) --")
# Esta prueba existe porque un error de este tipo (una constante usada y no
# definida, p. ej. MARKET_REFRESH_INTERVAL) NO lo detecta ni el compilador ni
# los tests de unidad: sólo se ve ejecutando el bucle del escáner.
_guardado_api = A.iq.api
_guardado_conn = A.iq.connected
_guardado_assets = A.broker_market.assets
_guardado_status = A.broker_market.status
_guardado_get = A.iq.get_candles
_guardado_stream = A.candle_streams.get_realtime


class _BrokerFalso:
    """Cliente mínimo: sólo lo que toca el ciclo (conexión + reloj)."""

    def __init__(self):
        self.llamadas = 0

    def check_connect(self):
        return True

    def get_server_timestamp(self):
        return time.time() + 1.0


def _velas_recientes():
    """Velas con la ÚLTIMA recién cerrada: si fueran antiguas el filtro de
    antigüedad marcaría el activo como CERRADO y la prueba no mediría nada."""
    ahora = int(time.time())
    n = A.MIN_CANDLES + 20
    return [mk(ahora - (n - 1 - i) * 60, 1.0, 1.0) for i in range(n)]


_VELAS_PEDIDAS = [0]


def _contar_velas():
    _VELAS_PEDIDAS[0] += 1


try:
    A.iq.api = _BrokerFalso()
    A.iq.connected = True
    A.broker_market.status = "LIVE"
    A.broker_market.assets = {
        "EURUSD-OTC": {"id": 76, "type": "TURBO", "open": True, "suspended": False,
                       "source": "TEST", "desc": "EUR/USD (OTC)", "exchange": "",
                       "exp_times": [60, 300]},
        "USDCHF-OP": {"id": 3, "type": "TURBO", "open": True, "suspended": False,
                      "source": "TEST", "desc": "USD/CHF", "exchange": "",
                      "exp_times": [120, 300]},     # sin 1 min -> debe descartarse
    }
    A.iq.get_candles = lambda *a, **k: (_contar_velas(), _velas_recientes())[1]
    A.candle_streams.get_realtime = lambda a: None
    A.gsr_memory.reset("EURUSD-OTC")
    A.candle_cache.cache.pop("EURUSD-OTC", None)

    # 1) El refresco de mercado lo hace SU PROPIO HILO (ya no el bucle).
    _error = None
    try:
        A.market_feed.refresh_market_sessions()      # lo que ejecuta MarketRefreshWorker
    except Exception as e:
        _error = f"{type(e).__name__}: {e}"
    check("el refresco de mercado (hilo aparte) funciona", _error is None, f"({_error})")

    _cands = dict(A.asset_manager.get_candidate_list())
    check("el activo con 1 min entra en el universo", "EURUSD-OTC" in _cands)
    check("el activo sin 1 min (USD/CHF OP) se descarta", "USDCHF-OP" not in _cands)
    check("se contabiliza el descartado sin 1 min", A.scan_metrics.assets_no_1min >= 1,
          f"({A.scan_metrics.assets_no_1min})")
    check("se contabilizan los activos con 1 min", A.scan_metrics.assets_1min >= 1,
          f"({A.scan_metrics.assets_1min})")
    _st = A.asset_manager.get_status("EURUSD-OTC")
    check("el activo simulado queda OPEN",
          _st is not None and _st.status == A.AssetStatusEnum.OPEN,
          f"({_st.status.name if _st else 'sin estado'})")

    # 2) El ciclo del bucle NO debe pedir nada a la red.
    A.scan_controller.last_clock_sync = 0.0
    A.scan_controller.last_processed_minute = -1
    _antes = _VELAS_PEDIDAS[0]
    _error = None
    try:
        A.scan_controller.execute()
    except Exception as e:
        _error = f"{type(e).__name__}: {e}"
    check("el ciclo del escáner se ejecuta sin excepciones", _error is None, f"({_error})")
    check("el ciclo NO pide velas al broker",
          _VELAS_PEDIDAS[0] == _antes,
          f"(peticiones: {_VELAS_PEDIDAS[0] - _antes})")
    check("el ciclo avanza el contador de ciclos", A.scan_metrics.cycles >= 1,
          f"(ciclos={A.scan_metrics.cycles})")
    check("el ciclo deja latido para el vigilante",
          abs(A.scan_controller.last_heartbeat - time.time()) < 5)
finally:
    A.iq.api = _guardado_api
    A.iq.connected = _guardado_conn
    A.broker_market.assets = _guardado_assets
    A.broker_market.status = _guardado_status
    A.iq.get_candles = _guardado_get
    A.candle_streams.get_realtime = _guardado_stream
    A.market_feed.market.pop("EURUSD-OTC", None)
    A.candle_cache.cache.pop("EURUSD-OTC", None)
    A.asset_manager.status_map.pop("EURUSD-OTC", None)
    A.asset_manager.status_map.pop("USDCHF-OP", None)
    A.asset_manager.active_tradable_assets = []

# --------------------------------- 15. el bucle NO puede hacer trabajo de red
print("\n-- 15. Anti-bloqueo: el bucle del escáner no hace trabajo de red --")
# Este es el fallo "el scanner se detiene solo": si el bucle llama a algo que va
# a la red (histórico de velas, reconexión, refresco de mercado) y esa llamada
# tarda, el bucle se queda bloqueado, el panel se congela y los ciclos dejan de
# contarse. La comprobación se hace sobre el AST (sin comentarios ni docstring)
# para que sea fiable aunque se documente el porqué.
import ast          # noqa: E402
import inspect      # noqa: E402
import textwrap     # noqa: E402

_fuente = textwrap.dedent(inspect.getsource(A.ScanController.execute))
_arbol = ast.parse(_fuente)
_cuerpo = _arbol.body[0].body
if (_cuerpo and isinstance(_cuerpo[0], ast.Expr)
        and isinstance(getattr(_cuerpo[0], "value", None), ast.Constant)
        and isinstance(_cuerpo[0].value.value, str)):
    _cuerpo = _cuerpo[1:]          # se descarta el docstring
_codigo = "\n".join(ast.unparse(n) for n in _cuerpo)
for _prohibido in ("refresh_market_sessions", "get_candles", "ensure_connected",
                   ".connect(", "urlopen", "requests."):
    check(f"execute() no llama a {_prohibido}",
          _prohibido not in _codigo,
          "(lo hace su hilo correspondiente)")

check("existe el hilo de refresco de mercado", hasattr(A, "market_refresh_worker"))
check("existe el hilo de conexión", hasattr(A, "connection_worker"))
check("existe el vigilante (watchdog)", hasattr(A, "watchdog_worker"))

_estado = A.watchdog_worker.status()
check("el estado de salud se puede consultar",
      {"salud", "ciclos", "activos_vivos", "streams", "broker"} <= set(_estado),
      f"({_estado.get('salud')})")
# Se refresca el latido justo antes de comprobarlo: esta prueba verifica que un
# bucle VIVO da salud OK, no que la suite sea rápida. Sin esto, en una máquina
# lenta la suite tardaría más que STALL_WARN y el test fallaría por el reloj.
A.scan_controller.last_heartbeat = time.time()
A.scan_controller.last_cycle_ts = time.time()
_estado = A.watchdog_worker.status()
check("la salud es OK con el bucle latiendo",
      _estado["salud"] == "OK", f"(salud={_estado['salud']}, "
      f"latido hace {_estado['latido_hace_s']}s)")

# El latido debe atrasarse de verdad si el bucle se queda parado: se simula un
# bucle colgado y se comprueba que el vigilante lo detecta.
_hb_original = A.scan_controller.last_heartbeat
_cycle_original = A.scan_controller.last_cycle_ts
try:
    A.scan_controller.last_heartbeat = time.time() - (A.STALL_WARN + 30)
    A.scan_controller.last_cycle_ts = time.time() - (A.STALL_WARN + 30)
    check("detecta DEGRADADO cuando el latido se atrasa",
          A.watchdog_worker.status()["salud"] == "DEGRADADO")
    A.scan_controller.last_heartbeat = time.time() - (A.STALL_RECOVER + 30)
    check("detecta ATASCADO cuando el parón persiste",
          A.watchdog_worker.status()["salud"] == "ATASCADO")
finally:
    A.scan_controller.last_heartbeat = _hb_original
    A.scan_controller.last_cycle_ts = _cycle_original

# Con un stream ya suscrito NO se debe pedir histórico por la conexión
# compartida: era lo que dejaba el bucle esperando el candado de velas.
_orig_is_streaming = A.candle_streams.is_streaming
_orig_get_candles = A.iq.get_candles
_orig_get_realtime = A.candle_streams.get_realtime
try:
    A.candle_streams.get_realtime = lambda a: None
    A.candle_streams.is_streaming = lambda a: True
    A.iq.get_candles = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("no debe pedir histórico con un stream activo"))
    A.candle_cache.cache.pop("STRM-OTC", None)
    _c, _origen = A.market_feed._candles_for("STRM-OTC")
    check("con stream activo no pide histórico (evita el bloqueo)",
          _origen == "STREAM_INICIANDO", f"(origen={_origen})")
finally:
    A.candle_streams.is_streaming = _orig_is_streaming
    A.iq.get_candles = _orig_get_candles
    A.candle_streams.get_realtime = _orig_get_realtime
    A.candle_cache.cache.pop("STRM-OTC", None)

# Un activo ABIERTO sin fila de métricas debe mostrarse como ABRIENDO, no como
# CERRADO (antes se pintaba "CERRADO" + "PATRÓN GSR COMPLETO" y parecía muerto).
_ga = A.broker_market.assets
_gs = A.broker_market.status
try:
    A.broker_market.status = "LIVE"
    A.broker_market.assets = {
        "EURUSD-OTC": {"id": 76, "type": "TURBO", "open": True, "suspended": False,
                       "source": "TEST", "desc": "", "exchange": "", "exp_times": [60]},
    }
    A.asset_manager.status_map["EURUSD-OTC"] = A.AssetTradingStatus(
        asset="EURUSD-OTC", asset_type="OTC", tradable=True,
        status=A.AssetStatusEnum.OPEN)
    A.proximity_engine.rows.pop("EURUSD-OTC", None)
    _fila = next((f for f in A.proximity_engine.top()
                  if f["raw_asset"] == "EURUSD-OTC"), None)
    check("activo abierto sin métricas se muestra ABRIENDO",
          _fila is not None and _fila["stage"] == "ABRIENDO" and _fila["status"] == "OPEN",
          f"({_fila['stage'] if _fila else 'sin fila'})")
    check("y no dice que el patrón está completo",
          _fila is not None and _fila["reason"] == ["SIN DATOS AÚN"],
          f"({_fila['reason'] if _fila else ''})")
finally:
    A.broker_market.assets = _ga
    A.broker_market.status = _gs
    A.asset_manager.status_map.pop("EURUSD-OTC", None)

# --------------------------------- 16. auditoría: seguridad y robustez
print("\n-- 16. Auditoría: seguridad del panel, memoria e histéresis --")
import logging           # noqa: E402
import logging.handlers  # noqa: E402

# --- 1.1 credenciales: sin valores por defecto en el código ---
check("IQ_EMAIL/IQ_PASSWORD se leen del entorno, sin fallback hardcodeado",
      A.IQ_EMAIL == os.environ.get("IQ_EMAIL", "")
      and A.IQ_PASSWORD == os.environ.get("IQ_PASSWORD", ""))
check("la conexión exige credenciales antes de intentar conectar",
      "not IQ_EMAIL or not IQ_PASSWORD" in inspect.getsource(A.IQClient.connect))

# --- 1.3 control de acceso al panel y al WebSocket ---
_token_original = A.WS_AUTH_TOKEN
try:
    A.WS_AUTH_TOKEN = ""
    check("sin token configurado se permite todo (modo desarrollo)",
          A._acceso_permitido({"query_string": b"", "headers": []}, "192.168.1.50") is True)

    A.WS_AUTH_TOKEN = "secreto-de-prueba-123"
    check("con token: LAN sin token -> DENEGADO",
          A._acceso_permitido({"query_string": b"", "headers": []}, "192.168.1.50") is False)
    check("con token: ?token= correcto -> permitido",
          A._acceso_permitido({"query_string": b"token=secreto-de-prueba-123",
                               "headers": []}, "192.168.1.50") is True)
    check("con token: token incorrecto -> DENEGADO",
          A._acceso_permitido({"query_string": b"token=otro",
                               "headers": []}, "192.168.1.50") is False)
    check("con token: cabecera X-Auth-Token -> permitido",
          A._acceso_permitido({"query_string": b"",
                               "headers": [(b"x-auth-token", b"secreto-de-prueba-123")]},
                              "192.168.1.50") is True)
    check("desde el propio equipo siempre se permite (no te quedas fuera)",
          A._acceso_permitido({"query_string": b"", "headers": []}, "127.0.0.1") is True)
    check("el HTML lleva el marcador del token", "__WS_TOKEN__" in A.HTML)
    check("el JS usa el token al abrir el WebSocket", '"/ws" + (WS_TOKEN' in A.HTML)
finally:
    A.WS_AUTH_TOKEN = _token_original

# --- 2.1 poda del historial de patrones (fuga de memoria) ---
_max_original = A.PATTERN_HISTORY_MAX
try:
    A.PATTERN_HISTORY_MAX = 5
    A.pattern_history.clear()
    for i in range(20):
        A.pattern_history[f"patron-{i}"] = 1700000000 + i
    A._podar_pattern_history()
    check("el historial de patrones se poda al tope",
          len(A.pattern_history) == 5, f"({len(A.pattern_history)})")
    check("se conservan los MÁS RECIENTES",
          "patron-19" in A.pattern_history and "patron-0" not in A.pattern_history)
finally:
    A.PATTERN_HISTORY_MAX = _max_original
    A.pattern_history.clear()

# --- 2.3 histéresis STALE: un salto puntual no debe cerrar el activo ---
_mf = A.market_feed
_mf._stale_streak.clear()
_act = "HIST-OTC"
check("1er aviso de vela atrasada: NO cierra", _mf._ausente(_act) is False)
check("2º aviso de vela atrasada: NO cierra", _mf._ausente(_act) is False)
check(f"3er aviso: cierra (tolerancia={A.STALE_TOLERANCE})", _mf._ausente(_act) is True)
_mf._ausente_reset(_act)
check("tras recuperarse vuelve a tolerar", _mf._ausente(_act) is False)
_mf._stale_streak.clear()

# --- 4.1 bitácora rotativa a archivo ---
check("la bitácora a archivo está configurada", A.LOG_FILE.endswith("scanner.log"),
      f"({os.path.basename(A.LOG_FILE)})")
if os.environ.get("APPALI_NO_FILE_LOG") == "1":
    # Los tests desactivan la bitácora de producción a propósito: lo que hay que
    # comprobar aquí es el AISLAMIENTO (que la suite no ensucie scanner.log).
    check("los tests no enganchan la bitácora de producción (aislamiento)",
          not any(isinstance(h, logging.handlers.RotatingFileHandler)
                  for h in logging.getLogger().handlers))
else:
    check("hay un handler rotativo enganchado",
          any(isinstance(h, logging.handlers.RotatingFileHandler)
              for h in logging.getLogger().handlers))

# --- 4.3 /api/status ampliado ---
_estado = A.watchdog_worker.status()
for _clave in ("pattern_history_size", "stale_streak_max", "app_status",
               "ws_auth", "indicators_engine", "log_file"):
    check(f"/api/status expone '{_clave}'", _clave in _estado)

# --- 4.2 aviso de publicación en la app ---
check("el panel tiene el aviso de la app", 'id="app_badge"' in A.HTML)
check("el payload del panel incluye el estado de la app",
      "app_status" in inspect.getsource(A.Application.ws_broadcast_loop))

# --------------------------------- 17. integración Alí Binary Options
print("\n-- 17. Integración con Alí Binary Options --")
# El canal externo (puente local a un grupo) se eliminó el 21-sep-2026: la
# publicación de señales se hace en la app, por Cloud Function o Firestore.
check("APP_SIGNAL_URL existe como constante", hasattr(A, "APP_SIGNAL_URL"))
check("APP_SIGNAL_SECRET existe como constante", hasattr(A, "APP_SIGNAL_SECRET"))
check("FIREBASE_SA_PATH existe como constante", hasattr(A, "FIREBASE_SA_PATH"))
check("FIREBASE_PROJECT_ID existe como constante", hasattr(A, "FIREBASE_PROJECT_ID"))
check("enviar_alerta_app existe como función", callable(getattr(A, "enviar_alerta_app", None)))
check("enviar_alerta_firestore existe como función",
      callable(getattr(A, "enviar_alerta_firestore", None)))

# El canal externo retirado el 21-sep-2026 no debe existir en ninguna forma.
# Sus nombres se escriben aquí, en un ÚNICO sitio y a propósito: un test que
# comprueba una AUSENCIA tiene que nombrar lo que ya no debe estar (es la única
# referencia que queda en el proyecto; el resto se limpió).
_NO_DEBE_EXISTIR = ("WHATSAPP_BRIDGE_URL", "WHATSAPP_BRIDGE_ENABLED", "enviar_alerta_whatsapp")
for _prohibido in _NO_DEBE_EXISTIR:
    check(f"eliminado del módulo: {_prohibido}", not hasattr(A, _prohibido))

_src_fire = inspect.getsource(_orig_fire)   # la ORIGINAL: la sección 5 la sustituyó por un espía
check("fire_signal publica en la app (enviar_alerta_app)", "enviar_alerta_app" in _src_fire)

# El payload debe llevar exactamente los campos que espera la app.
_src_app = inspect.getsource(A.enviar_alerta_app)
_src_publicacion = _src_fire + _src_app
for _prohibido in _NO_DEBE_EXISTIR:
    check(f"la publicación no usa {_prohibido}", _prohibido not in _src_publicacion)
for _campo in ('"secret"', '"asset"', '"broker"', '"direction"', '"entryTime"',
               '"expiration"', '"source"', '"botName"', '"strategy"',
               '"confidence"', '"rsi"', '"damoa"'):
    check(f"el payload incluye {_campo}", _campo in _src_app)
check("el asset se formatea como en el broker (broker_search_name)",
      "broker_search_name(asset)" in _src_app)
check("la hora va en zona Colombia (TZ_COLOMBIA)",
      "TZ_COLOMBIA" in _src_fire)

# Estado ON/OFF según configuración (sin red de por medio).
_url_orig, _sa_orig = A.APP_SIGNAL_URL, A.FIREBASE_SA_PATH
try:
    A.APP_SIGNAL_URL = ""
    A.FIREBASE_SA_PATH = ""
    check("sin configurar -> APP OFF", A._estado_app()["state"] == "OFF")
    A.APP_SIGNAL_URL = "https://ejemplo/postSignal"
    check("con APP_SIGNAL_URL -> APP ON (Cloud Function)",
          A._estado_app()["state"] == "ON" and "Cloud Function" in A._estado_app()["detail"])
    A.APP_SIGNAL_URL = ""
    A.FIREBASE_SA_PATH = "C:\\ruta\\service-account.json"
    check("con FIREBASE_SA_PATH -> APP ON (Firestore directo)",
          A._estado_app()["state"] == "ON" and "Firestore" in A._estado_app()["detail"])
    A.APP_SIGNAL_URL = ""
    A.FIREBASE_SA_PATH = ""
    check("sin URL no intenta publicar (devuelve False sin tocar la red)",
          A.enviar_alerta_app("EURUSD-OP", "CALL", 12.5, -15.2, 75.0, "14:32") is False)
finally:
    A.APP_SIGNAL_URL, A.FIREBASE_SA_PATH = _url_orig, _sa_orig

# ------------------------------------------------------------------ resultado
try:
    os.remove(A.audit_logger.filename)
except Exception:
    pass

print("\n" + "=" * 72)
if FALLOS:
    print(f"RESULTADO: {len(FALLOS)} FALLO(S)")
    for f in FALLOS:
        print(f"   - {f}")
    print("=" * 72)
    sys.exit(1)
print("RESULTADO: TODAS LAS PRUEBAS PASARON")
print("=" * 72)
sys.exit(0)
