# APPALÍ SCANNER — Entrega técnica

Scanner GSR M1 conectado directamente a **IQ Option como única fuente de verdad**.
Proyecto: `BOT\SCANNER\app_ali.py`.

## Requisitos
- Windows + Python 3.11 (entorno `venv311` ya preparado en esta carpeta).
- Dependencias ya instaladas en `venv311` (fastapi, uvicorn, numpy, pandas, ta,
  pytz, websockets, **websocket-client**, requests, python-dotenv e iqoptionapi
  clásico con `stable_api`).
- El paquete `websocket-client` es imprescindible: `websockets` es otro paquete
  distinto (el asíncrono que usa uvicorn) y **no** sirve para iqoptionapi.
  Si hace falta reinstalar todo:
  ```powershell
  venv311\Scripts\python -m pip install -r requirements.txt
  venv311\Scripts\python -m pip install https://github.com/iqoptionapi/iqoptionapi/archive/refs/heads/master.zip
  ```

## Comando exacto para ejecutar
```powershell
cd C:\Users\PC\Documents\KONFIO_ZINC\BOT\SCANNER
venv311\Scripts\python.exe app_ali.py
```
O doble clic en `run_scanner.bat` (usa `venv311` y abre http://localhost:8000).

`run_scanner.bat` **no** se fía de que exista `venv311\Scripts\python.exe`: ese
archivo es un redireccionador y, si el Python base con el que se creó el entorno
ya no existe, falla con `did not find executable at ...`. El lanzador prueba
cada candidato (`import sys` + `import uvicorn, numpy, pandas, ta, fastapi`) y
usa el primero que funcione de verdad: `venv311` → `venv` → `python` del sistema.

## Tests automáticos (sin broker, sin credenciales)
```powershell
ejecutar_tests.bat                 :: o:  venv311\Scripts\python.exe test_scanner.py
```
- `test_scanner.py` — verifica el motor con velas sintéticas: blindaje de la
  librería, callbacks del websocket, validación del reloj, caché de velas,
  universo de candidatos, horarios de sesión y el **patrón GSR completo hasta el
  disparo** (incluida la vela de entrada).
- `test_indicadores.py` — comprueba que el motor rápido (numpy) da los mismos
  valores que el original (pandas + `ta`) y mide la velocidad. Exige cero
  discrepancias en los umbrales que deciden una señal.
- Devuelven código 0 si todo pasa y 1 si hay fallos. Para uso automatizado,
  `set NO_PAUSE=1` evita el "Presione una tecla".

## Credenciales (archivo .env)
Las credenciales NO van hardcodeadas en el código. Se leen del archivo **`.env`**
(local, ignorado por git) o de variables de entorno. Crea `SCANNER\.env` a partir de
`.env.example`:

```ini
IQ_EMAIL=tu_correo@dominio.com
IQ_PASSWORD=tu_contraseña
IQ_ACCOUNT_TYPE=PRACTICE
APP_SIGNAL_URL=            # URL de la Cloud Function postSignal (opción A)
APP_SIGNAL_SECRET=         # secreto compartido con esa función
WS_AUTH_TOKEN=             # protege el panel si el equipo está en red compartida
```

## Variables de entorno opcionales
- `BROKER_DIAGNOSTIC=1` → imprime el diagnóstico real del broker al conectar.
- `APP_SIGNAL_URL` + `APP_SIGNAL_SECRET` → publicar señales vía Cloud Function.
- `FIREBASE_SA_PATH` + `FIREBASE_PROJECT_ID` → publicar escribiendo en Firestore.
- `WS_AUTH_TOKEN` → exige token a los equipos de la red (tu propio equipo entra igual).
- `IQ_ACCOUNT_TYPE` (PRACTICE/REAL). `IQ_EMAIL`/`IQ_PASSWORD` se toman de `.env`.
- `PORT` (8000) → puerto del panel.
- `INDICATORS_ENGINE=numpy|ta` → motor de indicadores (por defecto `numpy`,
  ~15x más rápido y numéricamente equivalente; `ta` es la implementación
  original con pandas, útil para contrastar).
- `BODY_OUTSIDE_REAL` y `BODY_OUTSIDE_OTC` (40.0 las dos) → umbral de ruptura del
  cuerpo fuera de banda, **separado por tipo de activo**. Por defecto NO cambia
  nada respecto a la estrategia calibrada. Para endurecer sólo el mercado
  sintético: `set BODY_OUTSIDE_OTC=55`.
- `VOLUME_FILTER_ENABLED` (0) y `VOLUME_MIN_REL` (1.0) → filtro de volumen
  opcional. **Ojo: IQ Option entrega volumen 0 en forex/OTC**, así que además de
  venir desactivado se auto-desactiva si detecta que el dato no sirve (ver más
  abajo).
- `OTC_STRICT_REFERENCE` (55) → NO filtra nada: sólo hace que cada señal OTC
  quede registrada diciendo si ese umbral más exigente la habría descartado.
- `MAX_UNIVERSE` (40) → tope de activos vigilados (ver nota más abajo).
- `MARKET_REFRESH_INTERVAL` (60 s) → cada cuánto se revalida el universo.
- `CLOCK_SYNC_INTERVAL` (30 s) y `CLOCK_SAMPLES` (2) → resincronización del reloj
  y número de muestras promediadas.
- `IQ_CANDLE_TIMEOUT` (8 s), `IQ_INIT_TIMEOUT` (35 s), `IQ_STREAM_TIMEOUT`
  (12 s), `IQ_CONNECT_TIMEOUT` (60 s) → límites de espera de la librería.
- `CLOCK_MAX_SKEW` (30 s) → desfase máximo aceptable del reloj del broker.
- `WEBSOCKET_DEBUG=1` → vuelve a mostrar el tráfico del websocket (muy verboso).

## Integración con Alí Binary Options (publicación de señales)
El escáner publica cada señal en la app **Alí Binary Options** por una de dos vías.
Ninguna está activa hasta que se configure; si no hay ninguna, la señal sólo queda
en el log y en `signals_audit.csv` (y el panel lo avisa con **APP OFF**).

| | Opción A (recomendada) | Opción B |
|---|---|---|
| Vía | Cloud Function HTTP `postSignal` | Escritura directa en Firestore |
| Requisitos | Función desplegada en Firebase | Service account de Firestore |
| Variables | `APP_SIGNAL_URL`, `APP_SIGNAL_SECRET` | `FIREBASE_SA_PATH`, `FIREBASE_PROJECT_ID` |
| Seguridad | La service account **no vive** en el escáner | La clave JSON queda en el equipo |

**Qué envía** (mismos campos en las dos vías): `secret`, `asset` (formato broker,
`EUR/USD` o `EUR/USD (OTC)`), `broker: "IQ Option"`, `direction` (`CALL`/`PUT`),
`entryTime` (HH:MM hora Colombia), `expiration: 1`, `source: "bot"`,
`botName: "AppALI"`, `strategy: "GSR"`, `confidence`, `rsi`, `damoa`. La opción B
añade `status: "pending"` y `createdAt` (marca de servidor).

**Cómo comprobar que funciona**:
```powershell
curl http://localhost:8000/api/status        # -> "app_status":"ON" y "app_detail"
venv311\Scripts\python.exe probar_senal_app.py          # envía una señal de prueba real
venv311\Scripts\python.exe probar_senal_app.py --local  # valida el payload sin la función
```
En el log, cada publicación correcta deja `📤 [APP] Señal publicada en Ali Binary
Options: EUR/USD CALL 14:32`; si falla, `[APP] No se pudo publicar la señal...`.

## Herramienta de diagnóstico manual
```powershell
venv311\Scripts\python.exe diagnostico_broker.py
```
Conecta (sólo lectura, no escanea ni emite señales) y vuelca lo que el broker
entrega de verdad: estructura del catálogo, `expiration_times` por activo,
desfase del reloj y **volumen de las velas**. Es la herramienta con la que se
verificaron las afirmaciones de este README; úsala antes de cambiar cualquier
umbral.

## Backtest con datos REALES (obligatorio antes de operar en real)
El backtest con datos sintéticos sólo comprueba que el motor funciona; **no mide
rentabilidad**. Para medir de verdad:

```powershell
venv311\Scripts\python.exe exportar_velas.py --velas 40000     :: velas M1 -> datos_reales/
venv311\Scripts\python.exe backtest_gsr.py --carpeta datos_reales --both
```
- `exportar_velas.py` descarga velas M1 reales (el broker da hasta ~40.000 por
  activo) y las guarda en `datos_reales/velas_<ACTIVO>.csv` (ignorado por git).
- `--carpeta` agrega TODAS las señales de todos los activos y añade el
  **intervalo de confianza del 95%**, que es lo que evita confundir suerte con
  ventaja. Un solo activo genera muy pocas señales para concluir nada.

### Resultado de la medición con datos reales (21-sep-2026)
Con **40 activos × 40.000 velas = 1.600.000 velas M1 reales** (REAL: 40 días,
OTC: 28 días), payout asumido 0,80 (equilibrio 55,6%):

| Variante | Señales | Win-rate | IC 95% | Equilibrio | Veredicto |
|---|---|---|---|---|---|
| GSR **base** (sin P3/P4/P5) | **856** | **48,0%** | **44,7% – 51,4%** | 55,6% | **SIN VENTAJA** (todo el IC por debajo) |
| GSR + mejoras P3/P4/P5 — **lo que opera el escáner** | 96 | 46,9% | 36,9% – 56,9% | 55,6% | no concluyente (y por debajo en el punto) |

🛑 **CONCLUSIÓN: la estrategia NO muestra ventaja estadística con estos datos.**
La variante base, con 856 operaciones, gana un 48% cuando necesitaría un 55,6%:
perdería ~13,6% del importe por operación. La variante con filtros reduce mucho
el número de señales sin mejorar la tasa de acierto.

**Caveats honestos** (léelos antes de sacar conclusiones):
- El payout real de IQ Option varía por activo y momento (aquí se asume 0,80); con
  payout 0,70 el equilibrio sube al 58,8% y el resultado es aún peor.
- El backtest entra al cierre de la vela de confirmación (= apertura de la
  siguiente) y resuelve con el cierre de esa vela, que es el modelo correcto de un
  binario M1, pero no reproduce el tick exacto ni el feed de cotización interno
  del broker.
- 40 días de mercado son un régimen, no una muestra de todos los regímenes.
- Aun así, un IC del 95% de 856 operaciones enteramente por debajo del equilibrio
  **sí permite descartar** la variante base.

### Calibrar antes de operar
```powershell
venv311\Scripts\python.exe backtest_gsr.py --carpeta datos_reales --bucket
```
Barre `BODY_OUTSIDE × RSI` y, para no autoengañarse, compara la **mitad antigua
y la mitad reciente** de los datos: sólo marca como candidata una combinación
que supere el equilibrio en AMBAS mitades (si gana sólo en una, es casualidad).

**Resultado del barrido (28 combinaciones × 1,6 M de velas reales):**

| | Resultado |
|---|---|
| 24 de 28 combinaciones | win-rate 46,4% – 49,3%, **todo el IC por debajo del equilibrio** → sin ventaja |
| 4 combinaciones (RSI=85, las más estrictas) | 50% – 56,1% con IC amplio (incluye el equilibrio) → no concluyente |
| La mejor (BODY=30, RSI=85) | 56,1% sobre 189 señales · IC 49,0% – 63,2% · 1ª mitad 55,6% / 2ª mitad 56,8% |
| Combinaciones que superan el equilibrio en LAS DOS mitades | **ninguna** |

🛑 **NO OPERAR EN REAL.** El win-rate se mantiene pegado al 48-49% para casi
cualquier umbral (lo propio de una estrategia sin ventaja: un binario M1
aleatorio está en el 50%), la tasa sólo sube al endurecer mucho el RSI a costa de
quedarse sin muestra, y hay un patrón claro de **deterioro reciente**: en 20 de
las 28 combinaciones la segunda mitad rinde peor que la primera (p. ej. 51,7% →
44,0%). El mejor caso no pasa la prueba de las dos mitades, que es la que separa
una ventaja real de una casualidad.

## Precisión operativa: qué es operable a 1 minuto y qué no
El escáner **no usa una lista fija de pares**. El catálogo del broker
(`get_all_init_v2`) trae `expiration_times` por activo, y sólo entran los que
ofrecen **60 s**. Medido con `diagnostico_broker.py`:

| Dato | Valor |
|---|---|
| Activos en el catálogo | 278 |
| Con vencimiento de 60 s | 249 |
| Sin el campo `expiration_times` | **0** (el filtro es real, no un no-op) |
| Ejemplo descartado | `USDCHF-OP` → exp `[120, 180, 300]` (no hay 1 min) |
| Ejemplo válido | `EURUSD-OP` → exp `[60, 120, 180, 300, 600, 900]` |

Además hay un recorte manual en `FORBIDDEN_REAL_M1` (`EURCHF-OP`, `USDCHF-OP`,
`USDBRL-OP`, `NZDJPY-OP`): son pares que el catálogo lista con 60 s pero que la
app de IQ Option no deja operar a 1 minuto. Se cuentan igual como descartados.

Cada refresco deja constancia en el log y en el panel:

```
[MARKET] Universo: 40 activos con contrato de 1 min (9 descartados por no ofrecer vencimiento de 60 s).
```
El panel muestra dos contadores nuevos: **"1 min disponibles"** y
**"Descartados sin 1 min"**.

Por qué NO se usa una lista fija tipo `PAIRS_1MIN_VALIDOS`: se quedaría obsoleta
(el broker cambia vencimientos), quitaría pares que sí operan a 1 min (se
verificó `EUR/AUD` y `EUR/CAD` abiertos y con stream) y añadiría otros que no.
El broker es la única fuente de verdad y ya se consulta.

### Límite de activos vigilados
Medido en producción: **40 activos vivos, 35+ streams y ciclos de 0,15–0,66 s**.
Se puede recortar con `set MAX_UNIVERSE=30`, pero **no conviene repartirlos en
lotes que alternen entre ciclos**: el patrón GSR exige velas de 1 minuto
CONSECUTIVAS, y si un activo se saltara ciclos sus velas tendrían huecos y el
patrón no podría completarse nunca.

## Falsos positivos en OTC: qué se hizo y qué no
La propuesta de endurecer el OTC tiene dos partes y sólo una es aplicable hoy:

1. **Umbral de ruptura distinto por tipo de activo** → implementado y
   configurable (`BODY_OUTSIDE_OTC`), pero **por defecto en 40.0, igual que
   REAL**: cambiar un parámetro calibrado de la estrategia es una decisión de
   trading, no una corrección técnica. Para activarlo:
   `set BODY_OUTSIDE_OTC=55`. El auto-test verifica que el umbral se aplica de
   verdad por tipo (una vela que rompe 51,4% arranca el patrón en REAL y no en
   OTC con 55).
2. **Filtro por volumen** → implementado pero **desactivado**, y ahora con el dato
   correcto medido sobre 1,6 M de velas M1 reales (`exportar_velas.py`):

   | Tipo | Volumen de las velas |
   |---|---|
   | REAL (`-OP`) | **REAL** en los 16 activos (media ~394; p. ej. AUDCAD-OP entre 125 y 2295) |
   | OTC | **0 en todas las velas** (24/24 activos) |

   Es decir: el filtro **sí es viable en REAL** y es imposible en OTC. Por eso,
   además de venir apagado, `_volumen_ok()` se auto-desactiva cuando detecta que
   el activo no publica volumen (los OTC) en vez de bloquear sus señales. Para
   activarlo: `set VOLUME_FILTER_ENABLED=1` (afectará sólo a los REAL).
   *(Corrección: una medición anterior con `diagnostico_broker.py` dio volumen 0
   y se generalizó a todo el forex; era sólo cierto para OTC.)*
3. **Auditoría del motivo exacto** → implementado. Cada señal se registra en el
   log y en el CSV con `Tipo`, `BB_pct`, `Umbral_BB`, `Volumen`, `Vol_medio20` y
   `Vol_rel`, más una línea que dice si el umbral estricto de referencia
   (`OTC_STRICT_REFERENCE`, 55) la habría descartado. Así se decide con datos en
   lugar de a ojo.

`signals_audit.csv` tiene ahora 12 columnas. Si venía de una versión anterior se
**migra automáticamente** al arrancar: se reescribe la cabecera y se rellenan las
filas viejas con vacíos (no se pierde ni se descoloca el histórico).

## Panel: dos métricas que antes se confundían
- **Cond.** (`2/3`) → cuántas de las tres condiciones GSR (BB, RSI, DAMOA) se
  cumplen AHORA. Es sólo eso: condiciones, no señal.
- **Avance Patrón** (`0/25/50/100 %`) → avance del patrón secuencial.

Ver "25 % con las condiciones en verde" NO era una incoherencia: significaba
"patrón recién empezado (1ª vela) aunque las condiciones ya estén extremas". Antes
había dos calculadoras de progreso (una de ellas sin usar) y por eso se podía
leer como un error; ahora hay una sola fuente de verdad y las dos cifras se
muestran separadas y etiquetadas.

## El escáner "se detenía solo": causa y arquitectura anti-bloqueo
**Síntoma**: el panel se quedaba congelado (los %/RSI no cambiaban), los "Ciclos
de CPU" dejaban de subir y el escáner parecía muerto aunque el reloj siguiera
corriendo y la pestaña siguiera viva.

**Causa raíz**: el bucle del escáner hacía trabajo de **red** dentro de
`ScanController.execute()`. En concreto llamaba a `refresh_market_sessions()`,
que pide el histórico de hasta 40 activos por la conexión compartida. Cuando el
broker iba lento (arranque, reconexión, rollover, o simplemente 40 activos
suscribiéndose a la vez), esa llamada tardaba **decenas de segundos o minutos** y
el bucle se quedaba dentro: ni ciclos, ni panel, ni señales. Al terminar volvía,
por eso el fallo era intermitente. La reconexión (`ensure_connected()`, hasta
60 s) tenía el mismo problema.

**Corrección**: el bucle sólo lee caché y streams (operaciones locales). Todo el
trabajo de red vive en hilos con una única responsabilidad:

| Hilo | Responsabilidad | Antes |
|---|---|---|
| `candle-streams` | Suscribir/mantener los streams de velas | igual |
| `market-refresh` | Revalidar el universo con el broker | **dentro del bucle** ❌ |
| `connection` | Reconectar (handshake + balance, hasta 60 s) | **dentro del bucle** ❌ |
| `broker-state` | Refrescar el catálogo del broker | igual |
| `watchdog` | Vigilar que el bucle siga latiendo | no existía |
| bucle `scanner` | Sólo leer caché/streams, indicadores y señales | hacía de todo |

Además, tres cambios que eliminan la contención:
- Si un activo **ya tiene stream**, no se le pide histórico por la conexión
  compartida (el propio stream lo trae). Antes los dos competían por el mismo
  candado de velas.
- `IQ_STREAM_TIMEOUT` de 12 s → 5 s (la espera de confirmación por activo; con 40
  activos eran hasta 8 minutos de hilo ocupado).
- El refresco de mercado mide su duración y avisa si supera 30 s.

**Resultado medido**: el refresco de mercado pasó de **minutos bloqueando el
bucle a 0,38–0,53 s**, y en una prueba de fondo de 8 minutos los ciclos fueron
1→10 **uno por minuto, sin un solo hueco**.

## Comprobar la salud por HTTP (sin abrir el panel)
```powershell
curl http://localhost:8000/api/status
```
```json
{"salud":"OK","uptime_s":498.9,"latido_hace_s":0.2,"ultimo_ciclo_hace_s":0.2,
 "ciclos":9,"activos_vivos":37,"activos_operables":37,"streams":37,
 "broker":"CONNECTED","disponibilidad":"LIVE","llamadas_colgadas":0,
 "refresco_mercado_hace_s":4.6,"refresco_mercado_s":0.38}
```
- `salud`: `OK` · `DEGRADADO` (el bucle se atrasó más de `STALL_WARN`) ·
  `ATASCADO` (más de `STALL_RECOVER`; devuelve HTTP 503 y el vigilante renueva la
  sesión para recuperarse solo).
- `llamadas_colgadas`: llamadas de la librería abandonadas por timeout; si crece,
  el broker está respondiendo mal.
- Cada 5 minutos el log deja un latido: `[WATCHDOG] OK | ciclos=… | activos=… |
  streams=… | broker=… | refresco_mercado_cada=…s`.

Los tests (`test_scanner.py`, sección 15) comprueban sobre el AST de `execute()`
que **no** llama a `refresh_market_sessions`, `get_candles`, `ensure_connected`,
`.connect(`, `urlopen` ni `requests.`: si alguien vuelve a meter red en el bucle,
los tests fallan.


## Blindaje de la librería iqoptionapi (crítico)
La librería clásica espera los mensajes del socket con bucles **sin timeout y
sin sleep**, y llama a `connect()` dentro de un `except` desnudo. Si el socket
deja de responder, esos hilos se quedan girando al 100% de CPU **para siempre**,
se agotan los del pool y TODOS los activos pasan a `NO_DATA` de forma
irreversible (era el fallo real de `scanner_run_err.log`).

`app_ali.py` los sustituye en tiempo de ejecución (`_harden_iqoptionapi`):
- `get_candles` → espera con timeout que cede CPU y **nunca** reconecta por su cuenta.
- `get_all_init_v2` → espera acotada (antes giraba 30 s al 100% de CPU).
- `get_profile_ansyc` → espera acotada.
- `start_candles_stream` / `start_candles_one_stream` → suscripción acotada.
- `re_subscribe_stream` → neutralizado: las suscripciones las gobierna
  `CandleStreamManager` (la librería re-suscribía de forma síncrona dentro de
  `connect()`, bloqueando hasta 20 s por activo).
- Las peticiones de velas se **serializan** con un candado: la librería guarda la
  respuesta en un único campo compartido y dos hilos se pisaban la respuesta.

También se adaptan los callbacks a **websocket-client 1.x** (`_patch_ws_callbacks`):
en 1.x los callbacks se invocan como `callback(ws, *args)`, mientras que
`on_message` fue escrito para 0.56 (que llamaba a los métodos ligados sólo con el
mensaje). Sin este parche el socket conecta pero **no procesa ni un mensaje**:
sin velas, sin catálogo y sin balance; y `on_close` fallaba, dejando
`check_websocket_if_connect` en 1 con el socket ya muerto.

## Arquitectura implementada (IQ Option = fuente de verdad)
- **BrokerMarketState**: snapshot de disponibilidad vía `get_all_init_v2()`
  (binary/turbo; digital aislado). Estados `LIVE / STALE / UNKNOWN`, refresco
  controlado (no por ciclo), defensivo ante `None`.
- **Universo curado**: se construye desde el snapshot del broker con una lista
  curada de pares (`COMMON_PAIR_RANK`, ~40 activos REAL `-OP` y OTC `-OTC`) y
  sólo los operables a 1 minuto (vencimiento 60 s). Si el catálogo aún no está
  disponible se reconstruye desde las constantes de la librería, para que el
  panel no aparezca vacío sin explicación.
- **CandleStreamManager**: realtime con `start_candles_stream` /
  `get_realtime_candles` solo para activos OPEN; detiene stream y limpia
  GSR/proximidad/velas al cerrarse. `get_candles` queda solo como histórico
  (con freno por activo: ya no se pide en cada ciclo).
- **ConnectionManager**: una sola instancia IQ, reconexión serializada con
  candado y **espera exponencial** de 5 s a 180 s (antes reintentaba cada 0,5 s
  y acumuló 199 clientes abandonados).
- **validate_signal_eligibility**: bloquea la señal si no hay conexión,
  disponibilidad, activo OPEN o velas actuales; registra `SIGNAL_BLOCKED`.
- **GSR intacto**: Bollinger(20,2) + RSI(14, 80/20) + DAMOA(5, +10/-10),
  ruptura 40% del cuerpo fuera de banda, 2 velas de impulso **consecutivas**
  validadas individualmente (cuerpo real, sin mechas) y vela de confirmación de
  reversión antes de disparar. Cada vela cerrada se evalúa con los indicadores
  calculados en su propio cierre (sin look-ahead).
- **Dashboard**: estados BROKER/AVAILABILITY/CANDLES/SCANNER + métricas
  (REAL/OTC/CLOSED/STALE/NO_DATA/STREAMS/CYCLES). Reloj con
  `get_server_timestamp`, validado contra el reloj local (se rechaza cualquier
  desfase imposible).
- **Favicon**: responde `204 No Content`; ASGI cumple HTTP/WebSocket/Lifespan.

## Pruebas reales realizadas
- Conexión: `✅ IQ Option enlazado al entorno PRACTICE (sesión #1)`.
- Disponibilidad: `[BROKER] Disponibilidad LIVE: 278 instrumentos (144 abiertos)`.
- Realtime: 35+ streams activos y activos marcados
  `-> OPEN (vela viva hace Ns, fuente STREAM)`.
- Operables: `Activos filtrados operables: 37 (REAL: 13, OTC: 24)`.
- Ciclos: `[SCANNER] Ciclo #3 completado en 0.15s | Activos vivos: 37`
  (antes: 7,8 s por ciclo con 0 activos).
- Reloj: offset estable en ±1 s (antes derivaba a −234 s / −388 s).
- Bloqueo: con `availability UNKNOWN` → 0 operables y `candles STALE` (sin señales).
- GSR: verificado de punta a punta con `test_scanner.py` (señal PUT disparada en
  la vela de entrada correcta).
- Indicadores: `test_indicadores.py` → 140 series comparadas, diferencias sólo de
  redondeo (≤4e-11) y **cero** cambios en los umbrales de señal.

## Propuestas evaluadas y DESCARTADAS (con el motivo)
Revisión técnica de una lista de "7 correcciones" propuesta externamente. Se
aplicó lo que mejora el sistema y se descartó lo que lo rompería:

| Propuesta | Veredicto | Motivo |
|---|---|---|
| Reemplazar `MASTER_CANDIDATE_PAIRS` por una lista fija `PAIRS_1MIN_VALIDOS` | **Descartada** | `MASTER_CANDIDATE_PAIRS` era **código muerto** (no se usaba). Una lista fija quitaría pares que sí operan a 1 min y se quedaría obsoleta; el filtro real (`expiration_times` del broker) ya existe y está verificado. |
| "Si no hay velas en 3 ciclos, eliminar el activo hasta reiniciar" | **Sustituida** | Eliminarlo permanentemente pierde activos que sólo estaban pausados (rollover OTC). Se implementó espera adaptativa (hasta 900 s) que se recupera sola. |
| `BODY_OUTSIDE_OTC = 55` por defecto | **Configurable, sin activar** | Cambiar un umbral calibrado es una decisión de trading. Se implementó como opción y se añadió la auditoría para decidir con datos. |
| Filtro por volumen (`volumen > media de 20`) | **Descartada tal cual** | IQ Option entrega **volumen 0.0** en forex/OTC: el filtro sería `0 > 0` y bloquearía el **100%** de las señales. Se dejó opcional y con auto-desactivación. |
| "Unificar ProgressEngine con la fase" | **Aclarada + simplificada** | El `ProgressEngine` original **no se usaba**: el progreso ya venía de la fase. No había dos sistemas en conflicto. Se eliminó el motor duplicado y se separaron en el panel "Condiciones (2/3)" y "Avance Patrón". |
| `ThreadPoolExecutor(max_workers=10)` + lanzar todos los `get_candles` en paralelo | **Descartada (peligrosa)** | La librería guarda la respuesta en **un único campo compartido**; en paralelo las respuestas se pisan entre sí. Además ya no hace falta: el ciclo tarda 0,15–0,66 s. |
| Reducir `CANDLES` de 250 a 120 y el timeout de 5 s a 3 s | **Descartada** | El filtro de tendencia usa `rolling(100)`: con 120 velas queda al borde y, si el broker devuelve menos, el filtro se desactiva en silencio. Un timeout más corto convierte enlaces lentos en falsos `NO_DATA`. El coste de CPU ya es 1,7 ms por activo. |
| Bajar el refresco de mercado de 60 s a 30 s | **Innecesaria** | Los porcentajes del panel ya se refrescan cada ~4 s con la vela en formación; el refresco de 60 s sólo revalida el universo (y es configurable). |
| Exigir `ClockStatus.SYNCED` para marcar activos OPEN | **Descartada (peligrosa)** | Si el `timeSync` del broker no llega, el respaldo es el **reloj local** (sincronizado por NTP y con las velas en epoch UTC). Exigir SYNCED dejaría el escáner con 0 activos y sin señales para siempre. |
| Repartir los activos en lotes que alternan ciclos | **Descartada** | Rompe el patrón GSR: exige velas de 1 minuto **consecutivas**. |
| `asyncio.Semaphore(12)` como semáforo de peticiones | **Descartada** | El escáner es multihilo (no asyncio) y esas peticiones **deben** ir serializadas, no limitadas a 12 simultáneas. |


- **Antes de cambiar cualquier umbral, mide**: `diagnostico_broker.py` muestra lo
  que el broker entrega de verdad (incluido el volumen, que es 0 en forex/OTC).
- Cada señal queda auditada en `signals_audit.csv` (12 columnas) con el motivo
  exacto, y una línea en el log dice si el umbral estricto de OTC la habría
  filtrado. Con eso se decide con datos si activar `BODY_OUTSIDE_OTC=55`.
- **Espera adaptativa**: si un activo no devuelve histórico, su reintento se
  duplica (120 s → 240 → 480 → tope 900 s) en lugar de eliminarlo del monitoreo
  para siempre. Los activos OTC se pausan y vuelven (p. ej. en el rollover) y
  deben recuperarse solos, sin reiniciar el escáner.
- **No paralelizar las peticiones de velas**: la librería guarda la respuesta en
  un único campo compartido (`api.candles.candles_data`); dos peticiones a la vez
  se pisan la respuesta. Están serializadas a propósito con `IQ_CANDLES_LOCK`.
  Tampoco hace falta: el ciclo completo tarda 0,15–0,66 s con 40 activos.
- `venv` (Python 3.14, sólo simulación) quedó con las DLL de numpy rotas: sus
  paquetes se instalaron bajo Python 3.14.7, que ya no existe en el equipo, y al
  ejecutarse sobre 3.14.5 no cargan. **No afecta al modo real**: el lanzador lo
  detecta, avisa y usa `venv311`. Para dejarlo fino:
  `rmdir /s /q venv` y `py -3.14 -m venv venv` + dependencias.
- **`venv` (Python 3.14) ya no existe**: se eliminó el 17-sep-2026 por estar
  roto (sus paquetes se instalaron bajo Python 3.14.7, que ya no está en el
  equipo, y las DLL de numpy no cargaban). El escáner usa `venv311` y el
  lanzador ya no lo necesita. Si algún día quieres el modo simulación:
  `py -3.14 -m venv venv` + `venv\Scripts\python -m pip install -r requirements.txt`
  (usa las versiones nuevas indicadas al final de `requirements.txt`).
- **Publicación de señales**: se hace en **Alí Binary Options** (ver su sección).
  Si no hay `APP_SIGNAL_URL` ni `FIREBASE_SA_PATH`, la señal sólo queda en el log
  y en `signals_audit.csv`, y el panel muestra **APP OFF** en ámbar.
- Las filas antiguas de `signals_audit.csv` con DAMOA ~9564 son anteriores al
  tope ±50 (P1) y ya no pueden repetirse. Al analizar el histórico conviene
  ignorarlas o marcarlas como `PRE-P1`: sus columnas `Tipo`, `BB_pct`,
  `Umbral_BB`, `Volumen` están vacías porque se añadieron después.
- **Bitácora a archivo**: además de la consola, el escáner escribe
  `scanner.log` (rotativo, 10 MB × 5 copias, ignorado por git). Útil cuando algo
  falla de madrugada y la ventana ya no está.
