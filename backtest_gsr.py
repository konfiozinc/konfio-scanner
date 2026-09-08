#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backtest de la estrategia GSR (BB + RSI + DAMOA) sobre velas M1 CERRADAS.

Reutiliza la lógica REAL de app_ali.py (indicadores, detección de velas, umbrales)
y replica el pipeline de la estrategia, incluidos los filtros P3 (confirmación de
reversión), P4 (tendencia) y P5 (sesión/liquidez), y las correcciones P1/P2.

Permite comparar el rendimiento con/sin las mejoras (--both) y guardar las señales.

USO:
  (demo con datos sintéticos)  python backtest_gsr.py
  (con velas reales exportadas) python backtest_gsr.py --csv velas.csv
  (solo mejoras)                python backtest_gsr.py --csv velas.csv --mode full
  (comparar base vs mejoras)    python backtest_gsr.py --csv velas.csv --both
  (tuning)                      python backtest_gsr.py --bucket (barre 2 parámetros)

CSV esperado (columnas): timestamp, open, high, low, close, volume
  timestamp en epoch segundos o ISO (YYYY-MM-DD HH:MM:SS).
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE)
import numpy as np
import pandas as pd
import app_ali as m   # importa la estrategia real (no arranca el servidor)


# ============================================================
# CARGA DE VELAS
# ============================================================
def _parse_ts(v):
    try:
        return int(float(v))
    except ValueError:
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp())


def load_candles(csv_path):
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        if not rd.fieldnames:
            raise SystemExit(f"CSV vacío o sin encabezado: {csv_path}")
        for r in rd:
            try:
                rows.append(m.Candle(
                    timestamp=_parse_ts(r["timestamp"]),
                    open=float(r["open"]), high=float(r["high"]),
                    low=float(r["low"]), close=float(r["close"]),
                    volume=float(r.get("volume") or 0.0),
                ))
            except Exception as e:
                raise SystemExit(f"Línea inválida en CSV: {r} -> {e}")
    rows.sort(key=lambda c: c.timestamp)
    if len(rows) < 300:
        raise SystemExit(f"Se necesitan >=300 velas M1 (hay {len(rows)}).")
    return rows


def synthetic_candles(seed=1, n=30000, start_ts=1706550000):
    """Pulsos tipo 'quieto -> impulso -> reversión' que alternan dirección.
    Es el patrón que GSR busca (2 velas que rompen la banda y luego giro), así la
    demo ejercita el pipeline de punta a punta y muestra el efecto de los filtros.
    NOTA: es SOLO un smoke-test del harness; NO representa el mercado real. Para
    métricas reales ejecuta: python backtest_gsr.py --csv <velas_M1.csv>."""
    rng = np.random.default_rng(seed)
    p = 1.10
    closes = []
    for i in range(n):
        ph = i % 40
        if ph < 5:                                   # quieto (banda estrecha)
            step = rng.normal(0, 0.0004)
        elif ph < 10:                                # impulso (rompe banda)
            step = +0.0010 + abs(rng.normal(0, 0.0003))
        elif ph < 12:                                # reversión
            step = -0.0015 + rng.normal(0, 0.0003)
        else:                                        # vuelta a la media
            step = 0.04 * (1.10 - p) + rng.normal(0, 0.0005)
        p += step
        closes.append(p)
    closes = np.array(closes)
    opens = closes + rng.normal(0, 0.0005, size=n)   # open por encima o por debajo
    candles = []
    for i, c in enumerate(closes):
        o = opens[i]
        h = max(o, c) + abs(rng.normal(0, 0.00025))
        l = min(o, c) - abs(rng.normal(0, 0.00025))
        candles.append(m.Candle(timestamp=int(start_ts) + i * 60, open=o, high=h,
                                low=l, close=c, volume=1.0))
    return candles


# ============================================================
# BACKTEST (replica el pipeline de app_ali.py)
# ============================================================
def _precompute(candles):
    """Calcula UNA sola vez (vectorizado) los indicadores de cada vela y devuelve
    arrays alineados con el índice de la vela. Replica exactamente la fórmula de
    app_ali.calculate_indicators (incluidos P1 y P2)."""
    closes = np.array([c.close for c in candles], dtype=float)
    s = pd.Series(closes)
    bb = m.ta.volatility.BollingerBands(s, window=20, window_dev=2)
    upper = bb.bollinger_hband().values
    mid = bb.bollinger_mavg().values
    lower = bb.bollinger_lband().values
    rsi = m.ta.momentum.RSIIndicator(s, window=14).rsi().values
    ema = s.ewm(span=5, adjust=False).mean().values
    vol = np.sqrt((s.diff() ** 2).rolling(5).mean()).values
    vol_floor = np.abs(closes) * m.DAMOA_VOL_FLOOR_REL
    vol_p = np.where(np.isfinite(vol) & (vol > vol_floor), vol, vol_floor)
    damoa = np.clip(((closes - ema) / vol_p) * 10, -m.DAMOA_CLAMP, m.DAMOA_CLAMP)
    damoa = np.nan_to_num(damoa, nan=0.0)
    fast = s.ewm(span=m.TREND_FAST, adjust=False).mean().values
    slow = s.ewm(span=m.TREND_SLOW, adjust=False).mean().values
    sd = s.rolling(100).std().values
    trend = np.where(np.isfinite(sd) & (sd > 0), np.abs(fast - slow) / sd < m.TREND_FILTER_Z, True)
    trend = trend.astype(bool)
    return dict(upper=upper, mid=mid, lower=lower, rsi=rsi, damoa=damoa, trend=trend)


def _reset_global():
    m.gsr_memory.assets.clear()
    m.new_candle.timestamps.clear()
    m.pattern_history.clear()
    m.indicator_engine.timestamps.clear()
    m.indicator_cache.rows.clear()
    m.proximity_engine.rows.clear()
    m.gsr_events.events.clear()


def run_backtest(candles, payout=0.85, use_p3=True, use_p4=True, use_p5=True, warmup=300):
    """Recorre velas M1 cerradas y devuelve la lista de señales evaluadas.
    Cada señal: entrada = cierre de la vela del disparo; resultado = dirección
    del cierre siguiente (proxy de un binario M1)."""
    _reset_global()
    ind = _precompute(candles)
    n = len(candles)
    signals = []
    last_ts = None

    def snap(i):
        s = m.IndicatorSnapshot(index=i, bb_upper=ind["upper"][i], bb_middle=ind["mid"][i],
                                bb_lower=ind["lower"][i], rsi=ind["rsi"][i], damoa=ind["damoa"][i])
        s.trend_ok = bool(ind["trend"][i])
        c = candles[i]
        s.body = abs(c.close - c.open)
        s.outside = m.body_outside_percent(c, s)
        return s

    for i in range(warmup, n - 1):
        current = candles[i]
        if last_ts == current.timestamp:
            continue
        last_ts = current.timestamp
        # Reloj del broker -> tiempo de la vela (P3/P5 con tiempo histórico)
        m.broker_clock._offset = current.timestamp - time.time()

        state = m.gsr_memory.get("X")
        indicator = snap(i)
        if indicator is None or not m.indicator_validator.validate(indicator):
            continue
        previous = candles[i - 1]

        if state.phase == m.GSRPhase.OBSERVANDO:
            m.detect_first_candle("X", previous, indicator)
        elif state.phase == m.GSRPhase.PRIMERA_VELA:
            m.detect_second_candle("X", current, indicator)
        elif state.phase == m.GSRPhase.SEGUNDA_VELA:
            ok = True
            if state.direction == "PUT":
                if indicator.rsi < m.RSI_OVERBOUGHT or indicator.damoa < m.DAMOA_HIGH:
                    ok = False
            else:
                if indicator.rsi > m.RSI_OVERSOLD or indicator.damoa > m.DAMOA_LOW:
                    ok = False
            if use_p4 and not indicator.trend_ok:
                ok = False
            if use_p5 and m.SESSION_FILTER_ENABLED and not m.is_liquid_session(int(m.broker_clock.now_ts())):
                ok = False
            if not ok:
                m.gsr_memory.reset("X")
            else:
                state.phase = m.GSRPhase.LISTA
                state.ready = True
                state.rsi = indicator.rsi
                state.damoa = indicator.damoa
                state.reversal_pending = True
                state.confirm_deadline = int(m.broker_clock.now_ts()) + m.REVERSAL_CONFIRM_WINDOW * 60
        elif state.phase == m.GSRPhase.LISTA:
            if use_p3:
                if (current.timestamp > state.second_candle
                        and m._confirm_reversal("X", current, state)):
                    signals.append(_fire(candles, i, state))
                    m.gsr_memory.reset("X")
                elif state.confirm_deadline and int(m.broker_clock.now_ts()) > state.confirm_deadline:
                    m.gsr_memory.reset("X")
            else:
                # sin confirmación de reversión: disparar al confirmar
                if not state.signal_sent and current.timestamp > state.second_candle:
                    signals.append(_fire(candles, i, state))
                    state.signal_sent = True
                    m.gsr_memory.reset("X")
    return signals


def _fire(candles, i, state):
    entry = candles[i].close
    nxt = candles[i + 1].close
    if state.direction == "PUT":
        win = nxt < entry
    else:
        win = nxt > entry
    return {
        "ts": int(candles[i].timestamp),
        "asset": "X", "dir": state.direction,
        "rsi": round(float(state.rsi), 2), "damoa": round(float(state.damoa), 2),
        "entry": round(float(entry), 6), "next": round(float(nxt), 6),
        "win": bool(win),
    }


# ============================================================
# MÉTRICAS
# ============================================================
def metrics(signals, payout):
    total = len(signals)
    if total == 0:
        return dict(total=0, wins=0, losses=0, win_rate=0.0, expectancy=0.0,
                    profit_factor=0.0, max_consec_losses=0, net_pnl=0.0)
    wins = sum(1 for s in signals if s["win"])
    losses = total - wins
    win_rate = wins / total
    net_pnl = wins * payout - losses * 1.0
    gross_win = wins * payout
    gross_loss = losses * 1.0
    profit_factor = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    max_cc = cur = 0
    for s in signals:
        if s["win"]:
            cur = 0
        else:
            cur += 1
            max_cc = max(max_cc, cur)
    # expectancy por operación (apuesta = 1)
    expectancy = win_rate * payout - (1 - win_rate) * 1.0
    return dict(total=total, wins=wins, losses=losses, win_rate=win_rate,
                expectancy=expectancy, profit_factor=profit_factor,
                max_consec_losses=max_cc, net_pnl=net_pnl)


def save_signals(signals, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ts", "asset", "dir", "rsi", "damoa",
                                          "entry", "next", "win"])
        w.writeheader()
        for s in signals:
            w.writerow(s)
    return path


def by_dir(signals, payout):
    out = {}
    for d in ("PUT", "CALL"):
        sub = [s for s in signals if s["dir"] == d]
        out[d] = metrics(sub, payout)
    return out


# ============================================================
# REPORTE
# ============================================================
def fmt_metrics(label, md, breakeven):
    if md["total"] == 0:
        return (f"{label:<26} SIN SEÑALES\n")
    wr = md["win_rate"] * 100
    ok = "RENTABLE" if md["expectancy"] > 0 else "NO rentable"
    return (f"{label:<26} señales={md['total']:>4}  win-rate={wr:5.1f}%  "
            f"expectancy={md['expectancy']:+.3f}  profit-factor={md['profit_factor']:6.2f}  "
            f"net_pnl={md['net_pnl']:+.2f}  racha_perd={md['max_consec_losses']}  [{ok}]")


def report(candles, payout, label, use_p3, use_p4, use_p5, save=None):
    sig = run_backtest(candles, payout=payout, use_p3=use_p3, use_p4=use_p4, use_p5=use_p5)
    md = metrics(sig, payout)
    breakeven = 1.0 / (1.0 + payout)   # win-rate de equilibrio
    lines = [fmt_metrics(label, md, breakeven)]
    for d, dm in by_dir(sig, payout).items():
        lines.append("   " + fmt_metrics("   · " + d, dm, breakeven))
    if save:
        save_signals(sig, save)
        lines.append(f"   Señales guardadas -> {save}")
    return lines, md, sig


def main():
    ap = argparse.ArgumentParser(description="Backtest GSR M1")
    ap.add_argument("--csv", help="CSV de velas M1 (timestamp,open,high,low,close,volume)")
    ap.add_argument("--synthetic", type=int, default=30000, help="nº de velas sintéticas (demo)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--payout", type=float, default=0.80, help="payout binario (default 0.80)")
    ap.add_argument("--mode", choices=["full", "base"], default="full",
                    help="full=mejoras activas (P3+P4+P5); base=sin mejoras")
    ap.add_argument("--both", action="store_true", help="ejecuta base y full y compara")
    ap.add_argument("--save", help="si se da, guarda las señales en este CSV")
    args = ap.parse_args()

    candles = load_candles(args.csv) if args.csv else synthetic_candles(args.seed, args.synthetic)
    src = os.path.basename(args.csv) if args.csv else f"sintéticas({args.synthetic})"
    print(f"Backtest GSR M1 · {len(candles)} velas · fuente={src} · payout={args.payout}")
    if not args.csv:
        print("  [AVISO] DATOS SINTÉTICOS: solo sirve para comprobar que el harness funciona.")
        print("  Para medir el rendimiento REAL exporta velas M1 de IQ Option a un CSV y ejecuta:")
        print("  python backtest_gsr.py --csv velas_M1.csv  [--both]")
    breakeven = 100.0 / (1.0 + args.payout)
    print(f"Win-rate de equilibrio (payout {args.payout:.0%}): {breakeven:.1f}%")
    print("=" * 100)

    # ... con mejoras (full)
    if args.mode == "full" or args.both:
        fl, mf, sf = report(candles, args.payout, "GSR + MEJORAS (P3/P4/P5)",
                            True, True, True, save=args.save)
        for x in fl:
            print(x)
    # ... sin mejoras (base)
    if args.mode == "base" or args.both:
        bl, mb, sb = report(candles, args.payout, "GSR BASE (sin P3/P4/P5)",
                            False, False, False)
        for x in bl:
            print(x)
    print("=" * 100)
    if args.both:
        # comparación
        try:
            if mb["total"] > 0:
                delta = (mf["expectancy"] - mb["expectancy"]) * 100
                print(f"Delta expectancy base->mejoras: {delta:+.2f} pts por operación")
                print(f"Delta win-rate base->mejoras:     {(mf['win_rate']-mb['win_rate'])*100:+.1f} pts")
        except Exception:
            pass


if __name__ == "__main__":
    main()
