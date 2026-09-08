# INFORME DEL SCANNER — AppALÍ GSR (IQ Option) + Puente WhatsApp

**Ruta:** `C:\Users\usuario29\Documents\BOT\SCANNER`
**Fecha del análisis:** sesión actual · **Estado:** se revisaron los archivos, se corrigieron errores,
se limpiaron artefactos y se documentan las soluciones de producción.

---

## 1. Resumen ejecutivo

El proyecto es un **scanner/robot de señales de trading** conectado a **IQ Option** como
fuente de verdad, con una **estrategia GSR** sobre velas de 1 minuto, y un **puente local a
WhatsApp** que publica las señales en un grupo. Consta de:

- `app_ali.py` (2.059 líneas) — engine Python: conexión IQ, reloj del broker, catálogo de
  activos, streams en tiempo real, indicadores GSR (Bollinger/RSI/DAMOA) y un dashboard web
  embebido en `http://localhost:8000/` (ASGI/uvicorn + WebSocket).
- `whatsapp_bridge/` (Node.js + **Baileys**) — recibe `POST /enviar` y manda la señal al grupo.
- `venv311` (Python 3.11, **modo REAL** con `iqoptionapi`) y `venv` (Python 3.14, simulación).

**Conclusiones clave del análisis:**

| Componente | Estado |
|---|---|
| `app_ali.py` | Funciona e importa OK. Se corrigió un **bug en el filtro del universo de activos**. |
| `whatsapp_bridge` | Correcto (Baileys). La **documentación** describía un stack antiguo (whatsapp-web.js). |
| `venv311` | Es el entorno real de producción (trae la librería IQ). |
| `requirements.txt` / READMEs | Tenían rutas y datos **desactualizados** (proyecto mudado de `KONFIO_ZINC` a `BOT`). |
| Artefactos | Había ~72 MB de logs/cachés/sesión Puppeteer obsoletos y **credenciales de WhatsApp versionadas en git** (riesgo). |

---

## 2. Inventario del proyecto

### Raíz de `SCANNER\`
| Archivo | Propósito | Acción |
|---|---|---|
| `app_ali.py` | Engine principal + dashboard web. | **Editado** (filtro de activos) |
| `README.md` | Portada del proyecto. | **Editado** (ruta/`index.html` incorrecto) |
| `README_INSTRUCCIONES.md` | Guía de ejecución. | **Editado** (rutas obsoletas) |
| `requirements.txt` | Dependencias. | **Editado** (ahora documenta los 2 entornos) |
| `run_scanner.bat` | Lanzador que elige `venv311` (REAL) o `venv` (SIM). | OK |
| `.gitignore` | Ignora secretos y artefactos. | **Editado** (fuga de credenciales) |
| `signals_audit.csv` | Auditoría de señales (4 filas de prueba). | OK (regenerado por la app) |
| `venv/`, `venv311/`, `__pycache__/` | Entornos y cachés de Python. | OK (no se tocan) |

### `whatsapp_bridge\`
| Archivo | Propósito | Acción |
|---|---|---|
| `index.js` | Puente HTTP → Baileys (envío al grupo). | OK |
| `bridge.config.json` | Grupo + puerto. | **Editado** (sin campos huérfanos) |
| `README_WHATSAPP.md` | Guía del puente. | **Editado** (stack correcto) |
| `run_bridge.bat` | Lanzador del puente. | OK |
| `package.json` / `package-lock.json` | Dependencias de Node. | OK |
| `sesion_baileys/` | Sesión activa del número bot. | **Desversionada** (ya no en git) |
| ~~`sesion/`~~, ~~`.wwebjs_cache/`~~, ~~`qr.txt`~~ | Stack Python anterior (Puppeteer/whatsapp-web.js). | **Eliminados** |

---

## 3. Errores encontrados y correcciones aplicadas

### 3.1 [Corregido] El universo de activos admitía instrumentos que NO son forex
**Dónde:** `app_ali.py` → `LiveAssetManager.get_candidate_list()`.

El filtro original usaba `re.search(r'[A-Z]{3}/[A-Z]{3}', descripcion)` **sin anclar**. Esa
expresión hace *substring match*, por lo que aceptaba como “par de divisas” cualquier
descripción que contuviera 3 mayúsculas + `/` + 3 mayúsculas en cualquier posición. En los
logs reales se ve que el escáner consultaba **acciones/CFDs y metales**:

```
GOOGLE/MSFT (OTC) -> CERRADO      TESLA/FORD (OTC) -> CERRADO
META/GOOGLE (OTC) -> CERRADO      NFLX/AMZN  (OTC) -> CERRADO
XAU/XAG  -> NO_DATA               INTEL/IBM  (OTC) -> CERRADO
```

Eso inflaba el universo, gastaba llamadas al broker y generaba ruido en el dashboard.

**Corrección:** el candidato ahora debe cumplir **3 condiciones**:
1. El **nombre/código** del activo es un par de 6 letras con sufijo opcional `-OTC`/`-OP`
   (`EURUSD`, `EURUSD-OTC`, `USDBRL-OP`) → excluye `GOOGLE/MSFT-OTC`, `TESLA/FORD-OTC`, etc.
2. La **descripción** debe contener un par `XXX/YYY` → excluye acciones sueltas (`GOOGLE`).
3. Se descartan **metales/commodities** (`XAU`, `XAG`, `XPT`, `XPD`, `XTI`, `XNG`, `OIL`).

**Verificado:** con un snapshot simulado de 10 instrumentos el resultado es solo forex:

```python
[('EURUSD','REAL'), ('EURUSD-OTC','OTC'), ('PENUSD-OTC','OTC'),
 ('USDBRL-OP','REAL'), ('USDCOP-OTC','OTC')]
```

> Nota (actualización posterior): se **retiraron del universo los activos `-OP`** (terminados
> en `(OP)`). El filtro solo admite `EURUSD`-style (REAL) y `EURUSD-OTC`-style (OTC). En el log
> los `-OP` aparecían con velas frescas (~12 s), pero son una variante que la estrategia no usa
> y añadía ruido; se eliminaron por petición del autor.

### 3.2 [Corregido] Credenciales de sesión de WhatsApp versionadas en git (riesgo de seguridad)
**Dónde:** repo git con remoto **`https://github.com/konfiozinc/konfio-scanner.git`**.

Había **146 archivos** de la sesión Baileys dentro de git: `creds.json`, `sender-key-*`,
`sender-key-memory-*`, `session-*.json`, `pre-key-*.json`. Contienen las claves privadas del
número bot. Si ese repositorio se subió a GitHub, **las credenciales quedaron expuestas**.

**Correcciones:**
- `.gitignore` ahora ignora `whatsapp_bridge/sesion_baileys/`, `whatsapp_bridge/qr.txt`,
  `whatsapp_bridge/.wwebjs_cache/`, `log_real.txt`, `signals_audit.csv`, `*_pid.txt`, etc.
- Se quitó `sesion_baileys/` del índice de git (`git rm -r --cached`) **sin borrar los
  archivos del disco**, de modo que el puente sigue funcionando.
- Los archivos siguen **en disco** (no se perdieron la sesión ni el vínculo).

### 3.3 [Corregido] Documentación y rutas obsoletas
- `README_INSTRUCCIONES.md`: apuntaba a `KONFIO_ZINC\SCANNER\app_ali.py`. El proyecto está
  en `BOT\SCANNER`. Corregido.
- `README.md`: decía “Abrir `index.html`”. No existe `index.html`; el panel se sirve desde
  `app_ali.py`. Corregido.
- `README_WHATSAPP.md`: describía `whatsapp-web.js` + Chrome y sesión en `sesion/`. El puente
  real usa **Baileys** y guarda la sesión en `sesion_baileys/`. Reescribido.
- `bridge.config.json`: tenía `telefono_bot` y `chromepath`, **no usados** por Baileys. Se
  dejaron solo `grupo` y `puerto`.
- `requirements.txt`: ahora distingue `venv311` (REAL) y `venv` (SIM) y lista las versiones
  reales (verificado).
- `README_INSTRUCCIONES.md` → `run_scanner.bat` → `app_ali.py`: coherentes.

---

## 4. Artefactos eliminados (limpieza)

| Eliminado | Motivo |
|---|---|
| `_probe_forex.py`, `_probe_open.py` | Probes de diagnóstico con **credenciales hardcodeadas**; ya no sirven. |
| `_scan_err.log` (201 KB), `_scan_out.log` | Salidas de una prueba manual en consola. |
| `_bat_test.log`, `_bat_test_err.log` | Logs del `.bat` de prueba (con la ruta vieja `KONFIO_ZINC`). |
| `log_real.txt` | Volcado de log con la ejecución real. |
| `_instancia_pid.txt`, `_launcher_pid.txt` | PIDs de procesos **ya inexistentes** (no había servidor escuchando). |
| `whatsapp_bridge/qr.txt` | Captura de un QR antiguo (ya vinculado). |
| `whatsapp_bridge/sesion/` (**~71,5 MB**) | Perfil de Chrome/Puppeteer del stack **anterior** (whatsapp-web.js). El puente actual usa Baileys (`sesion_baileys/`). |
| `whatsapp_bridge/.wwebjs_cache/` (~0,6 MB) | Caché de whatsapp-web.js, en desuso. |

**Espacio liberado: ~72 MB.** Se conservaron: `venv`, `venv311` (entornos de ejecución),
`node_modules` (dependencias del puente) y `sesion_baileys/` (sesión activa).

> Los archivos borrados que estaban versionados quedan como `deleted` en git; cuando el
> autor quiera, debe hacer **commit** de los cambios (ver §6).

---

## 5. Observaciones y riesgos que quedan abiertos

1. **Credenciales IQ Option hardcodeadas.** `app_ali.py` usa como valores por defecto
   `IQ_EMAIL=***REDACTED***` y `IQ_PASSWORD=***REDACTED***`; `run_scanner.bat`
   también las inyecta. El código ya permite sobrescribirlas por variables de entorno, pero
   el **fallback queda en el código/repo**. Recomendado moverlas a un `.env` (ver §6.2).
2. **Modo `PRACTICE` por defecto** (`IQ_ACCOUNT_TYPE=PRACTICE`). Bueno para validar; cuando se
   pase a real debe hacerse de forma controlada.
3. **Exposición de red.** El servidor escucha en `0.0.0.0:8000`. Si no se necesita acceso
   remoto, conviene limitarlo a `127.0.0.1`. El puente ya escucha solo en `127.0.0.1:8120`.
4. **Múltiples hilos sobre una única conexión IQ.** El engine usa un `ThreadPoolExecutor` para
   `get_candles`, streams en paralelo y refresco del catálogo. En los logs aparece
   `ERROR: Connection is already closed.` y reconexiones forzadas. Es el punto más frágil de
   producción (ver §6.3).
5. **Persistencia mínima.** La única salida de datos es `signals_audit.csv` (y el log). No hay
   métricas de win-rate ni historial consultable.
6. **`signals_audit.csv`** tiene 4 filas de prueba y un encabezado ligeramente distinto al que
   escribe el código (`Fecha_Hora_COT` vs `Fecha_Hora`). Se regenera solo; no afecta.

---

## 6. Soluciones efectivas para la producción

Priorizadas por impacto/riesgo.

### 6.1 Seguridad: limpiar credenciales del historial de git (ALTA)
- Verificar si `sesion_baileys/` (y sus claves) llegaron al remoto:
  `git -C . log --oneline --all -- whatsapp_bridge/sesion_baileys`.
- **Si ya se subió:** además del commit de borrado, **rotar** la sesión de WhatsApp (desvincular
  el número y volver a vincular) y considerar limpiar el historial con `git filter-repo`/`BFG`.
  GitHub suele avisar con *secret scanning*.
- Commit de limpieza: `git add . && git commit -m "chore: quitar credenciales y artefactos"`.

### 6.2 Credenciales por `.env` (ALTA, fácil)
1. Crear `SCANNER\.env` (ya ignorado por git):
   ```ini
   IQ_EMAIL=tu_correo@dominio.com
   IQ_PASSWORD=tu_contraseña
   IQ_ACCOUNT_TYPE=PRACTICE
   WHATSAPP_BRIDGE_URL=http://127.0.0.1:8120
   WHATSAPP_BRIDGE_ENABLED=1
   ```
2. Cargarlo al inicio, p. ej. al principio de `app_ali.py`:
   ```python
   import os
   try:
       from dotenv import load_dotenv
       load_dotenv()
   except Exception:
       pass   # opcional: dotenv no imprescindible
   ```
   (añadir `python-dotenv` a `requirements.txt`), o cargar el `.bat` con `for /f` desde `.env`.
3. Quitar las credenciales como *default* del código para que salgan solo del entorno.

### 6.3 Robustez de la conexión IQ (ALTA)
- **Unificar el acceso de red a un solo hilo** (un único lugar que toque `iq.api`), en lugar de
  repartir llamadas entre el threadpool de velas, el stream-reconcile y el refresco del catálogo.
- Aumentar **tolerancia a temporales**: en `CandleStreamManager.reconcile`, al fallar un `start`,
  reintentar con backoff y no borrar el activo del tablero de forma agresiva.
- Revisar las ventanas `LIVE_WINDOW_REAL=90` / `LIVE_WINDOW_OTC=180`: si el broker “salta” un
  segundo de velas, `age > max_age` marca el activo STALE y puede oscilar entre OPEN/CLOSED.
  Considerar una **histéresis** (no cerrar hasta N ciclos consecutivos stale).

### 6.4 Ejecución como servicio y supervisión (ALTA)
- **Iniciar el escáner como servicio** con NSSM, o con el Programador de Tareas (al arrancar
  Windows, con la mínima ventana, reinicio al fallar). Ej. NSSM (buscar en PATH) o
  `schtasks /create /tn "AppALI_Scanner" /tr "C:\...\run_scanner.bat" /sc onlogon`.
- Igual para el puente (`run_bridge.bat`).
- **Health-checks:** ya existe `GET /estado` en el puente y `GET /api/proximity-v2` en el
  escáner; exponer un `/health` que devuelva `broker`, `availability`, `streams_active`,
  `broker_clock.status` para que un monitor externo (o un `.bat`) compruebe que todo está vivo.

### 6.5 Logs y observabilidad (MEDIA)
- Dirigir el `logging` a un archivo rotativo (`RotatingFileHandler`, ej. `scanner.log`) además
  de consola, con nivel configurable por env.
- Registrar por ciclo un resumen compacto (activos vivos, REAL/OTC, edad máxima, latencia) para
  detectar degradación sin leer logs gigantes.

### 6.6 Reducir carga del broker (MEDIA)
- El universo ya es mucho menor con el filtro corregido. Aun así, priorizar **streams
  (`start_candles_stream`/`get_realtime_candles`)** para los activos OPEN y usar `get_candles`
  solo para el histórico inicial, tal como está planteado. Esto evita `N` llamadas por ciclo.

### 6.7 Datos y métricas (MEDIA)
- Guardar señales en una **base local (SQLite)** en lugar de solo CSV, y calcular métricas
  (win-rate por par/dirección/hora) para validar la estrategia antes de arriesgar capital real.
- (Opcional) publicar señales por otro canal (webhook/Telegram) además de WhatsApp, para no
  depender de un único mecanismo.

### 6.8 Disciplina de trading (ALTA recomendación)
- Seguir en **PRACTICE** hasta tener un histórico validado de señales y métricas.
- Mantener el **aviso de riesgo** (ya presente en el mensaje de WhatsApp) y añadir un panel de
  **confirmación manual** (filtro de riesgo) antes de considerar operaciones reales.

---

## 7. Archivos modificados / creados / eliminados en esta sesión

**Editados:** `.gitignore`, `README.md`, `README_INSTRUCCIONES.md`, `app_ali.py`,
`requirements.txt`, `whatsapp_bridge/README_WHATSAPP.md`, `whatsapp_bridge/bridge.config.json`.

**Eliminados:** `_probe_forex.py`, `_probe_open.py`, `_scan_err.log`, `_scan_out.log`,
`_bat_test.log`, `_bat_test_err.log`, `log_real.txt`, `_instancia_pid.txt`, `_launcher_pid.txt`,
`whatsapp_bridge/qr.txt`, `whatsapp_bridge/sesion/` (dir), `whatsapp_bridge/.wwebjs_cache/` (dir).

**Desversionados de git (se conservan en disco):** `whatsapp_bridge/sesion_baileys/`.

**Creado:** este informe `INFORME_SCANNER.md`.

*Cualquier cambio queda **sin commit** para revisión del autor. Validado: `app_ali.py` importa y
compila OK en `venv311`; `index.js` pasa `node --check`.*
