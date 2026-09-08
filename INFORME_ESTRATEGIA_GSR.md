# INFORME DE LA ESTRATEGIA GSR (BB + RSI + DAMOA)

**Archivo:** `app_ali.py` · **Ruta:** `C:\Users\usuario29\Documents\BOT\SCANNER`
**Alcance:** análisis de estado y propuesta de mejoras de efectividad.
**Nota previa:** en esta sesión, además, se **retiraron del universo los activos `-OP`**
(terminados en `(OP)`); ahora solo se escanean `EURUSD` (REAL) y `EURUSD-OTC`-style (OTC).

---

## 1. Cómo funciona GSR (máquina de estados)

El motor recorre una máquina de estados por activo (`GSRPhase`) sobre velas de **1 minuto (M1)**:

| Fase | Acción |
|---|---|
| `OBSERVANDO` | Busca la **1ª vela**: si es `GREEN` y su **cuerpo está ≥40% fuera de la banda superior** → `PRIMERA_VELA` (dirección `PUT`). Si es `RED` y su cuerpo rompe ≥40% la banda inferior → dirección `CALL`. |
| `PRIMERA_VELA` | Busca la **2ª vela consecutiva** del **mismo color** y también ≥40% fuera de banda → `SEGUNDA_VELA`. Si cambia de color o no supera el 40%, se resetea. |
| `SEGUNDA_VELA` | **Confirmación** con RSI y DAMOA: para `PUT` exige `RSI ≥ 80` **y** `DAMOA ≥ +10`; para `CALL` exige `RSI ≤ 20` **y** `DAMOA ≤ -10`. Si no se cumple → reset. Si cumple → `LISTA`. |
| `LISTA` | Dispara la señal (`fire_signal`), valida elegibilidad, la registra en `signals_audit.csv` y la publica al grupo por WhatsApp. |

**Interpretación:** es una estrategia de **reversión a la media tras una ruptura de 2 velas**.
Dos velas del mismo color rompen la banda de Bollinger (≥40% del cuerpo por fuera) y, con RSI en
extremo + DAMOA extremo, se anticipa el giro (PUT tras subida fuerte; CALL tras caída fuerte).

```
                            PUT: GREEN + cuerpo fuera banda↑  → 2ª GREEN fuera banda↑ → RSI≥80 & DAMOA≥+10 → SEÑAL PUT
                            CALL: RED  + cuerpo fuera banda↓  → 2ª RED  fuera banda↓ → RSI≤20 & DAMOA≤-10 → SEÑAL CALL
```

---

## 2. Componentes y parámetros

| Componente | Parámetros en código | Comentario |
|---|---|---|
| **Bollinger Bands (BB)** | window=20, dev=2 (`BB_PERIOD=20`, `BB_STD=2.0`) | Media móvil simple ±2 desv. estándar. Se usa banda superior/inferior. |
| **RSI** | window=14 (`RSI_PERIOD=14`), sobreventa/sobrecompra `80/20` | Clásico Wilder. |
| **DAMOA** | period=5 (`DAMOA_PERIOD=5`), umbrales `±10` (`DAMOA_HIGH/LOW`) | **Indicador propio**: `((close − EMA5) / sqrt(mean(diff²,5))) × 10`. Es un *z-score* de momentum normalizado por una RMS de los últimos cambios. |
| **BodyOutside** | `BODY_OUTSIDE=40.0` (%) | % del cuerpo real (open→close, sin mechas) que queda fuera de la banda. |

### Fórmula DAMOA
```
ema        = EMA(close, span=5)
volatilidad= sqrt( mean( (close.diff())² , 5 ) )     # RMS de los últimos 5 cambios
damoa      = (close - ema) / volatilidad * 10
```

---

## 3. Estado / anomalías encontradas

### 3.1 [CRÍTICO] DAMOA se dispara cuando la volatilidad tiende a 0
DAMOA divide por una **RMS de los cambios**. En mercados planos / rollovers / OTC lento, la RMS
es ~0 y el cociente se dispara a miles. **Evidencia real** (ya en `signals_audit.csv`):

```
DAMOA = -532.41 , 9564.47 , 3642.16 , 544.12
```

Con esos valores extremos, el umbral `±10` (que debería representar ~1 desviación) deja de ser una
puerta significativa: en calma, cualquier mini-movimiento pasa el filtro DAMOA; en tendencia fuerte,
es difícil alcanzar `≥ +10`. En la práctica **DAMOA funciona peor cuando más se necesita**.
**Simulación confirmada:** con RMS = 2,7e-5 (mercado casi plano) DAMOA = 14,9 y puede llegar a miles.

> Recomendación directa: poner un **piso de volatilidad** (`volatilidad = max(rms, eps)` con
> `eps` ~ 1e-6 del precio) y **acotar DAMOA** a un rango sano (p. ej. ±50). Idealmente reemplazar la
> RMS por **desviación estándar** (rolling std) y usar un z-score con piso.

### 3.2 [ALTO] Desfase de la vela en formación (look-ahead)
`calculate_indicators` construye el array `close` con **todas** las velas, incluyendo la **aún en
formación** (`candles[-1]`), así que BB/RSI/DAMOA se evalúan "al cierre" de la vela que **aún no
cerró**. En cambio, el `body_outside` y la vela usada para 1ª/2ª detección usan velas **ya cerradas**
(`previous=candles[-3]`, `current=candles[-2]`).

```
calculate_indicators(candles):  BB/RSI/DAMOA al índice len-1  (vela en formación)  <-- desfase
                                body/outside               al índice -2  (última cerrada)
```

→ La confirmación RSI/DAMOA no corresponde al mismo cierre que el patrón de velas. **Fix:** calcular
indicadores solo con velas cerradas `candles[:-1]`, para que BB/RSI/DAMOA y el body-outside
apuntan a la misma vela.

### 3.3 [MEDIO] La señal se emite antes de la reversión
`fire_signal` dispara en fase `LISTA`, que ocurre **justo después** de completar el patrón pero
**sin vela de confirmación del giro**. En tendencia fuerte, 2 velas de ruptura suelen **continuar**,
por lo que la señal de reversión puede ir contra la tendencia (whipsaw).

### 3.4 [BAJO] Memoria sin límite
`pattern_history` (set) crece sin podar durante toda la ejecución; `gsr_events` sí tiene tope de 200.
Añadir antigüedad a `pattern_history` (o descartar patrones viejos) para evitar crecimiento ilimitado.

### 3.5 [INFO] Ventanas de "activo vivo"
`LIVE_WINDOW_REAL=90` / `LIVE_WINDOW_OTC=180` s. Como el patrón usa velas M1, una edad > ventana
marca STALE. Está bien; solo conviene **histéresis** para no oscilar OPEN/CLOSED por un salto puntual.

### 3.6 [INFO] Umbrales sin validación de histórico
BB(20,2), RSI(80/20), DAMOA(±10) y BodyOutside(40%) se ven **calibrados a mano**, sin backtest. La
estrategia puede estar sobre-ajustada o dejar pasar/señalar en exceso sin métricas que lo sustenten.

---

## 4. Mejoras de efectividad (priorizadas)

### P1. Arreglar DAMOA (impacto directo en calidad de señal)
```python
# En calculate_indicators():
diff = df_close.diff()
vol  = np.sqrt((diff ** 2).rolling(5).mean())
#   Piso de volatilidad relativa para evitar explotar en mercados planos
vol  = vol.where(vol > (df_close * 1e-6))
damoa_serie = ((df_close - ema) / vol) * 10
#   Acolchar valores extremos (controla outliers tipo 9500)
damoa_val = float(np.clip(damoa_serie.fillna(0.0).iloc[-1], -50, 50))
```

### P2. Alinear indicadores a la vela cerrada (look-ahead)
```python
def calculate_indicators(candles):
    closed = candles[:-1]                 # descartar la vela en formación
    close  = np.array([x.close for x in closed], dtype=float)
    ...
    current_candle = closed[-1]           # última vela cerrada
    indicator.body  = abs(current_candle.close - current_candle.open)
    indicator.outside = body_outside_percent(current_candle, indicator)
```

### P3. Confirmación de la reversión (reduce falsos positivos)
En `GSRReadyEngine.confirm` o en una fase previa a `LISTA`, exigir un asomo de giro:
```python
# Para PUT: la vela siguiente debe cerrar ROJA (o cerrar por debajo de la anterior)
reversal = candle_color(next_candle) == "RED" and next_candle.close < state.last_close
# Solo entonces LISTA -> señal
```
Coste: 1 vela más de latencia. Mejora notable la tasa de acierto en tendencias.

### P4. Filtro de tendencia (evita ir a contracorriente)
Añadir un filtro de marco superior M5/M15 o un indicador de tendencia:
```python
ema_fast = ema(close, 50); ema_slow = ema(close, 200)
trend_up = ema_fast > ema_slow
# Solo tomar CALL/PUT de reversión si NO contradice la tendencia de fondo,
# o exigir ADX >= 25 para "mercar" (sin tendencia) -> mejor reversión a la media.
```

### P5. Filtro de sesión / liquidez
Operar en horas de mayor liquidez (solapamiento **Londres–Nueva York**, ~13:00–17:00 UTC) y **evitar
rollovers** (21:00–22:00 UTC) donde las velas OTC son erráticas. Evita falsas rupturas de banda.

### P6. Umbral de progreso mínimo
El motor ya computa `progress` (BB=40, RSI=30, DAMOA=30). Exigir **score ≥ 70** y RSI más extremo
(`≥85`/`≤15`) como condición opcional "conservadora" para filtrar señales mediocres.

### P7. Validación por backtest (imprescindible antes de arriesgar capital)
Construir un backtest sobre velas M1 cerradas que calcule **tasa de acierto (win-rate)**, **expectancy**
y **máximo drawdown** de la secuencia de señales. Para un binario con payout ~85–90%, el win-rate de
equilibrio es ~53–55%. Medirlo permitirá calibrar cada umbral y decidir si la estrategia es rentable.

### P8. Optimización de parámetros controlada
Con el backtest, barrer `BB_STD` (1.8–2.4), `RSI` (75–85 / 15–25), `DAMOA` (±8–±15) y
`BODY_OUTSIDE` (35–50) para elegir el mejor conjunto **con validación out-of-sample** (evitar overfitting).

---

## 5. Recomendación general

1. **Inmediato (bajo riesgo):** P1 (piso+clamp de DAMOA) y P2 (vela cerrada). Son correcciones de
   correctitud que no cambian la lógica de la estrategia.
2. **Siguiente (necesita validación):** P3 (confirmación de reversión), P4 (filtro de tendencia),
   P5 (sesión). Cambian la selección de señales → deben probarse.
3. **Antes de operar real:** P7 (backtest) y P8 (calibración). Mantener **modo PRACTICE** hasta tener
   métricas que lo respalden.

> **Actualización:** se implementaron **P1, P2, P3, P4, P5 y P7** en `app_ali.py` / `backtest_gsr.py`
> (ver §6 y §7). P6 y P8 quedan como propuesta.

---

## 6. Mejoras implementadas en esta sesión (P1–P5, P7)

| # | Cambio | Ubicación | Efecto |
|---|---|---|---|
| **P1** | Piso de volatilidad (`vol > precio*1e-6`) + acotado a `±DAMOA_CLAMP(50)` | `app_ali.py`→`calculate_indicators` | DAMOA ya no explota a miles (`9564`, `-532`); umbral `±10` vuelve a ser significativo. |
| **P2** | Indicadores calculados solo con velas cerradas (`candles[:-1]`) | `app_ali.py`→`calculate_indicators` | Elimina el *look-ahead*: BB/RSI/DAMOA y el body-outside apuntan a la misma vela cerrada. |
| **P3** | Confirmación de reversión antes de disparar | `AssetState.reversal_pending/confirm_deadline`, `_confirm_reversal`, rama `LISTA` del `ScannerPipeline` | La señal (WhatsApp) solo sale tras una vela que vaya en contra del patrón (PUT→roja, CALL→verde), con ventana de 3 velas. |
| **P4** | Filtro de tendencia (EMA50 vs EMA150 normalizada por std) | `_trend_ok`, `GSRReadyEngine.confirm`, `IndicatorSnapshot.trend_ok` | Descarta la señal si el mercado está en tendencia fuerte (donde la ruptura suele continuar, no revertir). |
| **P5** | Filtro de sesión/liquidez (08–17 UTC, sin rollover 21 UTC ni fin de semana) | `is_liquid_session`, `GSRReadyEngine.confirm` | Solo arma la reversión en horas de alta liquidez; evita velas erráticas. Se desactiva con `SESSION_FILTER_ENABLED=0`. |
| **P7** | Backtest sobre velas M1 cerradas | `backtest_gsr.py` | Mide win-rate, expectancy, profit-factor y racha de pérdidas, con comparación base vs mejoras. |

### Parámetros nuevos (tunables)
```
DAMOA_VOL_FLOOR_REL = 1e-6            # piso de volatilidad relativa (P1)
DAMOA_CLAMP         = 50.0            # límite de |DAMOA| (P1)
REVERSAL_CONFIRM_WINDOW = 3           # velas M1 para esperar la reversión (P3)
TREND_FAST = 50 / TREND_SLOW = 150 / TREND_FILTER_Z = 1.0   # filtro de tendencia (P4)
SESSION_FILTER_ENABLED = 1            # activa el filtro de sesión (P5)
LIQUID_SESSION_START = 8 / LIQUID_SESSION_END = 17          # ventana UTC (P5)
```

### Notas de comportamiento
- **P3/P4 cambian qué señales se emiten** (menos, pero de mayor calidad): por diseño, reduce señales
  falsas en tendencia fuerte. Se recomienda **validar con backtest (P7)** antes de dar peso real.
- **Cosmético en el dashboard:** al estar en `LISTA` a la espera de la vela de reversión, el tablero
  marca *patrón cercano*; la señal a WhatsApp sale solo con la confirmación. Si no llega la reversión,
  no se envía señal (el patrón se descarta al pasar la ventana).
- Se mantiene **modo PRACTICE** hasta tener métricas de win-rate/expectancy.

### Verificación realizada
- `app_ali.py` compila e importa OK en `venv311`.
- Pruebas unitarias: DAMOA acotado a `±50`, `trend_ok=False` en tendencia fuerte / `True` en rango,
  y confirmación de reversión correcta para PUT/CALL.
- Prueba de integración: la rama `LISTA` **solo** llama a `fire_signal` cuando la vela recién cerrada
  confirma la reversión (con una vela verde NO dispara).

---

## 7. Backtest (P7): cómo medir el rendimiento

Nuevo script **`backtest_gsr.py`**. Reutiliza la lógica REAL de `app_ali.py` (indicadores con P1/P2,
detección, umbrales) y replica el pipeline, incluidos los filtros P3/P4/P5, para medir **win-rate,
expectancy, profit-factor y racha de pérdidas** sobre velas M1 **cerradas**.

### Uso
```powershell
# Demo (datos sintéticos, solo para comprobar que el harness funciona) y comparar base vs mejoras:
venv311\Scripts\python.exe backtest_gsr.py --synthetic 30000 --both

# Con velas M1 reales exportadas de IQ Option (el paso serio):
venv311\Scripts\python.exe backtest_gsr.py --csv velas_M1.csv --both

# Solo una configuración, o guardar las señales:
venv311\Scripts\python.exe backtest_gsr.py --csv velas_M1.csv --mode full --save senales.csv
```

**CSV de entrada (columnas):** `timestamp, open, high, low, close, volume`
(`timestamp` en epoch segundos o ISO). Se aceptan ambos formatos.

### Qué mide
- **Win-rate** y **expectancy por operación** (apuesta = 1): `win_rate*payout − (1−win_rate)*1`.
- **Profit-factor** = ganancias brutas / pérdidas brutas.
- **Racha máxima de pérdidas**.
- Desglose por **dirección (PUT/CALL)**.
- **Comparación base vs mejoras** con `--both` (muestra el efecto de P3/P4/P5 sobre cantidad y calidad).

### Interpretación del resultado (win-rate de equilibrio)
Con un payout binario típico (**~80–85 %**), el **win-rate de equilibrio** es ~**55–56 %**:
```
win_rate_de_equilibrio = 1 / (1 + payout)     # con payout 0.80 -> 55.6%
```
Hay que superarlo para que la estrategia sea rentable.

### Hallazgos de la demo sintética (NO representativos de mercado real)
| Config | Señales | Win-rate | Expectancy |
|---|---|---|---|
| GSR base (sin P3/P4/P5) | 12 | 91.7 % | +0.650 |
| GSR + mejoras (P3/P4/P5) | 2 | 100 % | +0.800 |

Lo relevante de la demo es la **tendencia**: los filtros reducen la cantidad de señales (12 → 2) y
mejoran la calidad (win-rate y expectancy). Es un *smoke-test* del harness; las cifras absolutas
dependen de los datos sintéticos y **no deben leerse como rendimiento real**.

### Importante para la validación real
1. **Exporta velas M1 de IQ Option** a CSV (timestamp, open, high, low, close, volume) para los
   pares que el escáner escanea.
2. Ejecuta `python backtest_gsr.py --csv velas_M1.csv --both`.
3. Solo **si el win-rate real supera el equilibrio (~55–56 %)** y la expectancy es positiva, la
   estrategia (con las mejoras) es candidata a operar. Hasta entonces, **mantener modo PRACTICE**.
4. GSR es **muy selectivo**: en los datos sintéticos casi no dispara. Conviene validar sobre varias
   semanas de velas para tener una muestra con significancia estadística.
