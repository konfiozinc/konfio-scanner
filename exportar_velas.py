# -*- coding: utf-8 -*-
"""
==========================================================
AppALÍ GSR Scanner - EXPORTAR VELAS M1 REALES
==========================================================
Descarga velas de 1 minuto del broker y las guarda en CSV para poder hacer el
backtest con DATOS REALES (el backtest con datos sintéticos sólo sirve para
comprobar que el motor funciona, no para medir rentabilidad).

Genera un CSV por activo en `datos_reales/` con las columnas que espera
`backtest_gsr.py`:
    timestamp,open,high,low,close,volume

Uso:
    venv311\\Scripts\\python.exe exportar_velas.py                     (todos, 5000 velas)
    venv311\\Scripts\\python.exe exportar_velas.py --velas 10000
    venv311\\Scripts\\python.exe exportar_velas.py --pares EURUSD,GBPUSD
    venv311\\Scripts\\python.exe exportar_velas.py --solo-otc

Notas:
  - Es SÓLO LECTURA: no opera nada.
  - El histórico se pide por páginas (el broker no devuelve miles de velas en
    una sola petición) y se descartan los duplicados de solape.
  - `datos_reales/` está ignorado por git (son megas de datos regenerables).
"""
import argparse
import csv
import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import app_ali as A  # noqa: E402


def descargar_activo(asset: str, total: int, lote: int) -> list:
    """Descarga `total` velas de 60 s del activo, paginando hacia atrás."""
    todas = {}
    fin = int(time.time())
    paginas = 0
    while len(todas) < total and paginas < (total // lote) + 3:
        paginas += 1
        velas = A.iq.get_candles(asset, 60, lote, fin)
        if not velas:
            break
        nuevas = 0
        for c in velas:
            if c.timestamp not in todas:
                todas[c.timestamp] = c
                nuevas += 1
        if nuevas == 0:
            break                      # el broker ya no tiene más histórico
        fin = min(c.timestamp for c in velas) - 60
        if len(velas) < lote:
            break                      # página incompleta: se acabó el histórico
        if fin <= 0:
            break
    return [todas[t] for t in sorted(todas)]


def guardar_csv(ruta: str, velas: list) -> int:
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for c in velas:
            w.writerow([int(c.timestamp), c.open, c.high, c.low, c.close, c.volume])
    return len(velas)


def main():
    ap = argparse.ArgumentParser(description="Exportar velas M1 de IQ Option a CSV")
    ap.add_argument("--velas", type=int, default=5000, help="velas por activo (default 5000)")
    ap.add_argument("--lote", type=int, default=1000, help="velas por petición (default 1000)")
    ap.add_argument("--salida", default=os.path.join(BASE_DIR, "datos_reales"),
                    help="carpeta de salida (default: datos_reales/)")
    ap.add_argument("--pares", help="lista separada por comas, p.ej. EURUSD,GBPUSD")
    ap.add_argument("--solo-otc", action="store_true", help="sólo variantes OTC")
    ap.add_argument("--solo-real", action="store_true", help="sólo variantes REAL (-OP)")
    args = ap.parse_args()

    print("=" * 74)
    print("  EXPORTAR VELAS M1 REALES - AppALÍ GSR Scanner (sólo lectura)")
    print("=" * 74)

    if not A.ensure_connected():
        print("ERROR: no se pudo conectar al broker. Revisa .env y la red.")
        return 1

    # El catálogo es IMPRESCINDIBLE: get_candles necesita el id interno de cada
    # activo, y es broker_market.refresh() quien lo registra.
    print("Cargando catálogo del broker...")
    A.broker_market.refresh()
    if not A.broker_market.valid():
        print("ERROR: el broker no devolvió el catálogo de activos.")
        return 1
    print(f"Catálogo OK: {len(A.broker_market.assets)} instrumentos")

    candidatos = A.asset_manager.get_candidate_list()
    if args.pares:
        pedidos = {p.strip().upper() for p in args.pares.split(",") if p.strip()}
        candidatos = [(a, t) for a, t in candidatos
                      if a.replace("-OTC", "").replace("-OP", "") in pedidos]
    if args.solo_otc:
        candidatos = [(a, t) for a, t in candidatos if t == "OTC"]
    if args.solo_real:
        candidatos = [(a, t) for a, t in candidatos if t == "REAL"]

    if not candidatos:
        print("ERROR: no hay activos que cumplan el filtro. Usa --pares EURUSD,GBPUSD")
        return 1

    print(f"\nSe exportarán {len(candidatos)} activos x {args.velas} velas M1 "
          f"-> {args.salida}\n")
    ok, fallos, total_velas = 0, [], 0
    inicio = time.time()

    for i, (asset, tipo) in enumerate(candidatos, 1):
        t0 = time.time()
        velas = descargar_activo(asset, args.velas, args.lote)
        if len(velas) < 300:
            print(f"  [{i:>2}/{len(candidatos)}] {asset:<14} {tipo:<5} "
                  f"SIN DATOS suficientes ({len(velas)} velas) - se omite")
            fallos.append(asset)
            continue
        ruta = os.path.join(args.salida, f"velas_{asset}.csv")
        n = guardar_csv(ruta, velas)
        total_velas += n
        ok += 1
        desde = time.strftime("%Y-%m-%d %H:%M", time.gmtime(velas[0].timestamp))
        hasta = time.strftime("%Y-%m-%d %H:%M", time.gmtime(velas[-1].timestamp))
        print(f"  [{i:>2}/{len(candidatos)}] {asset:<14} {tipo:<5} {n:>6} velas "
              f"({desde} -> {hasta} UTC, {time.time()-t0:.1f}s)")

    print("\n" + "=" * 74)
    print(f"Exportados {ok}/{len(candidatos)} activos · {total_velas} velas · "
          f"{time.time()-inicio:.0f}s")
    if fallos:
        print(f"Sin datos suficientes: {', '.join(fallos)}")
    print(f"\nBacktest con datos reales:")
    print(f"  venv311\\Scripts\\python.exe backtest_gsr.py --carpeta \"{args.salida}\" --both")
    print(f"  (o uno concreto:  --csv \"{os.path.join(args.salida, 'velas_EURUSD-OP.csv')}\" --both)")
    print("=" * 74)
    return 0


if __name__ == "__main__":
    sys.exit(main())
