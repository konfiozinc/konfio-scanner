# -*- coding: utf-8 -*-
"""
==========================================================
AppALÍ GSR Scanner - TEST DE INDICADORES
==========================================================
Comprueba que el motor rápido de indicadores (numpy) da EXACTAMENTE los mismos
valores que la implementación original (pandas + librería ta) y mide la mejora
de velocidad.

Por qué importa: la versión original tardaba 215 ms por activo, es decir ~8,6 s
por ciclo con 40 activos, así que la señal podía llegar hasta 8 s tarde dentro
de una vela de 1 minuto. El motor numpy baja a ~1,7 ms por activo manteniendo
las MISMAS fórmulas (Bollinger 20/2 con ddof=0, RSI de Wilder 14, DAMOA 5 con
piso de volatilidad y tope ±50, y el filtro de tendencia EMA50/EMA150).

Compara 140 series sintéticas (tendencia, lateral, volatilidad casi nula,
saltos bruscos y mixtas) y exige:
  - diferencias máximas <= 1e-8 (sólo redondeo de coma flotante),
  - CERO discrepancias en los umbrales que deciden una señal
    (RSI >= 80 / <= 20 y DAMOA >= +10 / <= -10),
  - CERO discrepancias en el filtro de tendencia.

Uso:
    venv311\\Scripts\\python.exe test_indicadores.py
    (o doble clic en ejecutar_tests.bat)

Devuelve código 0 si son equivalentes y 1 si divergen.
"""
import os
import random
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Fuerza el motor rápido ANTES de importar el módulo (se lee al importar).
os.environ["INDICATORS_ENGINE"] = "numpy"
# Y no escribe en la bitácora de producción (es una prueba, no una ejecución real).
os.environ["APPALI_NO_FILE_LOG"] = "1"

import app_ali as A  # noqa: E402
import numpy as np   # noqa: E402

FALLOS = []


def check(nombre, cond, extra=""):
    print(f"[{'OK  ' if cond else 'FALLA'}] {nombre} {extra}")
    if not cond:
        FALLOS.append(nombre)


random.seed(20260916)
np.random.seed(20260916)


def serie(n, modo):
    """Genera n velas sintéticas de 1 minuto según el régimen de mercado."""
    precio = 1.1000
    out = []
    t = 1_700_000_000
    for i in range(n):
        if modo == 0:      # paseo aleatorio suave
            precio *= 1 + random.uniform(-0.0004, 0.0004)
        elif modo == 1:    # tendencia fuerte + ruido
            precio *= 1 + 0.00025 + random.uniform(-0.0002, 0.0002)
        elif modo == 2:    # plano: volatilidad casi nula (prueba el piso P1)
            precio *= 1 + random.uniform(-1e-7, 1e-7)
        elif modo == 3:    # saltos bruscos
            precio *= 1 + random.choice([-0.004, 0.0, 0.0, 0.004])
        else:              # mixto: tramo lateral y tramo explosivo
            precio *= 1 + (random.uniform(-1e-6, 1e-6) if i % 120 < 80
                           else random.uniform(-0.003, 0.003))
        o = precio * (1 + random.uniform(-0.0002, 0.0002))
        out.append(A.Candle(t + i * 60, o, max(o, precio), min(o, precio), precio, 10.0))
    return out


print("=" * 72)
print("  TEST DE INDICADORES - equivalencia numpy vs pandas+ta")
print("=" * 72)

check("motor de indicadores = numpy", A.INDICATORS_ENGINE == "numpy",
      f"(INDICATORS_ENGINE={A.INDICATORS_ENGINE})")

peor_bb = peor_rsi = peor_damoa = 0.0
disc_trend = 0
disc_umbrales = 0
casos = 0

for modo in range(5):
    for n in (30, 31, 60, 100, 150, 250, 251):
        for _rep in range(4):
            velas = serie(n, modo)
            close = np.array([c.close for c in velas[:-1]], dtype=float)
            u1, m1, l1, r1, d1, t1 = A._indicators_ta(close)
            u2, m2, l2, r2, d2, t2 = A._indicators_numpy(close)
            casos += 1
            peor_bb = max(peor_bb, abs(u1 - u2), abs(m1 - m2), abs(l1 - l2))
            if np.isfinite(r1) and np.isfinite(r2):
                peor_rsi = max(peor_rsi, abs(r1 - r2))
            peor_damoa = max(peor_damoa, abs(d1 - d2))
            disc_trend += (t1 != t2)
            # Lo que de verdad decide una señal: los umbrales del patrón GSR.
            dec1 = (r1 >= 80, r1 <= 20, d1 >= 10, d1 <= -10)
            dec2 = (r2 >= 80, r2 <= 20, d2 >= 10, d2 <= -10)
            disc_umbrales += sum(1 for a, b in zip(dec1, dec2) if a != b)

print(f"\ncasos comparados                  : {casos}")
print(f"diferencia máxima en Bandas       : {peor_bb:.3e}  (precio ~1.1 -> relativo {peor_bb/1.1:.1e})")
print(f"diferencia máxima en RSI          : {peor_rsi:.3e}")
print(f"diferencia máxima en DAMOA        : {peor_damoa:.3e}")
print(f"discrepancias en filtro tendencia : {disc_trend}")
print(f"discrepancias en umbrales de señal: {disc_umbrales}")

TOL = 1e-8
print()
check("Bandas de Bollinger equivalentes", peor_bb < TOL)
check("RSI equivalente", peor_rsi < TOL)
check("DAMOA equivalente", peor_damoa < TOL)
check("Filtro de tendencia idéntico", disc_trend == 0)
check("Ningún umbral de señal cambia", disc_umbrales == 0)

# ------------------------------------------------------------------- velocidad
print("\n-- Velocidad (40 activos = un ciclo completo) --")
velas = serie(250, 0)
N = 40

A.INDICATORS_ENGINE = "numpy"
for _ in range(3):
    A.calculate_indicators(velas)
t = time.perf_counter()
for _ in range(N):
    A.calculate_indicators(velas)
ms_np = (time.perf_counter() - t) / N * 1000

A.INDICATORS_ENGINE = "ta"
for _ in range(3):
    A.calculate_indicators(velas)
t = time.perf_counter()
for _ in range(N):
    A.calculate_indicators(velas)
ms_ta = (time.perf_counter() - t) / N * 1000
A.INDICATORS_ENGINE = "numpy"

print(f"calculate_indicators numpy : {ms_np:7.2f} ms/activo -> 40 activos = {ms_np*40/1000:5.2f} s")
print(f"calculate_indicators ta    : {ms_ta:7.2f} ms/activo -> 40 activos = {ms_ta*40/1000:5.2f} s")
print(f"aceleración                : {ms_ta/ms_np:.1f}x")
check("el motor rápido es más rápido que el original", ms_np < ms_ta)

print("\n" + "=" * 72)
if FALLOS:
    print(f"RESULTADO: {len(FALLOS)} FALLO(S)")
    for f in FALLOS:
        print(f"   - {f}")
    print("=" * 72)
    sys.exit(1)
print("RESULTADO: MOTOR NUMPY EQUIVALENTE AL ORIGINAL (estrategia intacta)")
print("=" * 72)
sys.exit(0)
