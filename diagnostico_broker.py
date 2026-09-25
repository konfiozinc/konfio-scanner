# -*- coding: utf-8 -*-
"""
==========================================================
AppALÍ GSR Scanner - DIAGNÓSTICO DEL BROKER (manual)
==========================================================
Herramienta de mantenimiento: conecta a IQ Option, vuelca el catálogo real y
comprueba de dónde salen los datos que usa el escáner. NO escanea ni emite
señales; sólo imprime lo que el broker entrega.

Responde a preguntas que no se pueden contestar leyendo el código:
  1. ¿El payload de get_all_init_v2 trae 'expiration_times'? ¿Dónde?
     (de eso depende el filtro de "sólo activos operables a 1 minuto")
  2. ¿Qué activos del universo curado están enabled / is_suspended?
  3. ¿Las velas traen VOLUMEN útil o viene a 0 en forex/OTC?
     (de eso depende que un filtro por volumen sea viable o no)
  4. ¿Cuánto se desvía el reloj del broker?

Uso:
    venv311\\Scripts\\python.exe diagnostico_broker.py
"""
import json
import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import app_ali as A  # noqa: E402

print("=" * 74)
print("  DIAGNÓSTICO DEL BROKER - AppALÍ GSR Scanner (sólo lectura)")
print("=" * 74)

if not A.IQ_AVAILABLE:
    print("ERROR: la librería iqoptionapi no está disponible en este entorno.")
    sys.exit(2)

ok = A.ensure_connected()
print(f"\nCONEXIÓN            : {'OK' if ok else 'FALLÓ'}")
if not ok:
    print("No se pudo conectar; revisa credenciales/red. Se aborta el diagnóstico.")
    sys.exit(1)

print(f"CUENTA              : {A.IQ_MODE}")

# ---------------------------------------------------------------- 1. reloj
local = time.time()
try:
    t1 = float(A.call_with_timeout(A.iq.api.get_server_timestamp, 5))
    time.sleep(0.3)
    t2 = float(A.call_with_timeout(A.iq.api.get_server_timestamp, 5))
    print(f"RELOJ LOCAL (UTC)   : {time.strftime('%H:%M:%S', time.gmtime(local))}")
    print(f"SERVER_TS 1         : {time.strftime('%H:%M:%S', time.gmtime(t1))}  (desfase {t1 - local:+.3f}s)")
    print(f"SERVER_TS 2         : {time.strftime('%H:%M:%S', time.gmtime(t2))}  (desfase {t2 - (local + 0.3):+.3f}s)")
except Exception as e:
    print(f"RELOJ               : no disponible ({type(e).__name__}: {e})")

# ---------------------------------------------------------------- 2. catálogo
print("\n" + "-" * 74)
print("CATÁLOGO (get_all_init_v2)")
print("-" * 74)
try:
    data = A.call_with_timeout(A.iq.api.get_all_init_v2, A.IQ_INIT_TIMEOUT + 10)
except Exception as e:
    data = None
    print(f"ERROR: {type(e).__name__}: {e}")

if not isinstance(data, dict):
    print("Sin catálogo: el broker no respondió el init.")
else:
    print(f"Claves de nivel superior: {sorted(data.keys())[:15]}")
    for opcion in ("turbo", "binary", "blitz", "digital"):
        nodo = data.get(opcion)
        if isinstance(nodo, dict) and isinstance(nodo.get("actives"), dict):
            print(f"  {opcion:<8} -> {len(nodo['actives'])} activos")

    # Estructura CRUDA de un activo: aquí se ve dónde vive 'expiration_times'.
    muestra = None
    for opcion in ("turbo", "binary"):
        nodo = data.get(opcion) or {}
        act = nodo.get("actives") or {}
        for _aid, v in act.items():
            if isinstance(v, dict) and str(v.get("name", "")).upper().startswith("EURUSD"):
                muestra = (opcion, _aid, v)
                break
        if muestra:
            break
    if muestra:
        opcion, _aid, v = muestra
        print(f"\nESTRUCTURA CRUDA de un activo ({opcion}, id={_aid}) — claves: {sorted(v.keys())}")
        if isinstance(v.get("option"), dict):
            print(f"  option -> claves: {sorted(v['option'].keys())}")
            print(f"  option.expiration_times = {v['option'].get('expiration_times')}")
        else:
            print(f"  (sin sub-dict 'option'; valor={v.get('option')!r})")
        print("  JSON (recortado):")
        print("   " + json.dumps(v, ensure_ascii=False)[:600])

    # ¿Cuántos activos del universo curado tienen vencimiento de 60 s?
    print("\n" + "-" * 74)
    print("FILTRO DE 1 MINUTO sobre el universo curado")
    print("-" * 74)
    parsed = {}
    for opcion in ("turbo", "binary", "blitz"):
        nodo = data.get(opcion)
        if not isinstance(nodo, dict):
            continue
        for _aid, v in (nodo.get("actives") or {}).items():
            if not isinstance(v, dict):
                continue
            nombre = str(v.get("name", "")).split(".")[-1].upper()
            if not nombre:
                continue
            opt = v.get("option") or {}
            exp = sorted({int(x) for x in (opt.get("expiration_times") or [])
                          if str(x).isdigit() or isinstance(x, int)})
            if nombre not in parsed:
                parsed[nombre] = {"open": bool(v.get("enabled")) and not bool(v.get("is_suspended")),
                                  "exp": exp}
            else:
                parsed[nombre]["exp"] = sorted(set(parsed[nombre]["exp"]) | set(exp))

    con_60 = [n for n, d in parsed.items() if 60 in d["exp"]]
    sin_datos = [n for n, d in parsed.items() if not d["exp"]]
    print(f"activos en el catálogo            : {len(parsed)}")
    print(f"con 60 s entre sus vencimientos   : {len(con_60)}")
    print(f"sin 'expiration_times' en el JSON : {len(sin_datos)}")

    print("\nUniverso curado (COMMON_PAIR_RANK) y su vencimiento real:")
    for code in list(A.COMMON_PAIR_RANK)[:16]:
        for suf, tipo in (("-OP", "REAL"), ("-OTC", "OTC")):
            nombre = code + suf
            alt = nombre if nombre in parsed else (code if suf == "-OP" and code in parsed else None)
            if not alt:
                continue
            d = parsed[alt]
            tiene60 = "SI " if 60 in d["exp"] else ("?  " if not d["exp"] else "NO ")
            print(f"  {nombre:<12} {tipo:<5} open={'SI' if d['open'] else 'NO'}  1min={tiene60} "
                  f"exp={d['exp'][:8]}")

# ---------------------------------------------------------------- 3. volumen
print("\n" + "-" * 74)
print("VOLUMEN DE LAS VELAS (¿sirve para filtrar?)")
print("-" * 74)
for activo in ("EURUSD-OP", "EURUSD-OTC", "USDBRL-OTC"):
    try:
        velas = A.iq.get_candles(activo, 60, 30)
    except Exception as e:
        print(f"  {activo:<12} error: {e}")
        continue
    if not velas:
        print(f"  {activo:<12} sin velas")
        continue
    vols = [c.volume for c in velas]
    print(f"  {activo:<12} {len(velas)} velas | volumen min={min(vols)} max={max(vols)} "
          f"media={sum(vols)/len(vols):.2f} | distintas={len(set(vols))}")
    print(f"               última vela: {velas[-1]}")

print("\n" + "=" * 74)
print("Diagnóstico terminado. Cierra esta ventana (el escáner no se ve afectado).")
print("=" * 74)
