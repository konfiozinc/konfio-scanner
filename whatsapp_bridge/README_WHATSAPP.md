# Puente WhatsApp — Señales AppALÍ GSR al grupo

El escáner (`app_ali.py`) detecta cuando una señal llega a fase **LISTA/ENTRADA** y
la envía automáticamente al grupo de WhatsApp de la comunidad a través de este
puente local.

## Arquitectura

```
[Escáner app_ali.py]                  [Puente (Node/WhatsApp Web)]          [WhatsApp]
fase LISTA/ENTRADA ──HTTP POST──▶   whatsapp_bridge/index.js  ──sesión──▶   Grupo
  (fire_signal)      /enviar           número DEDICADO vinculado
```

## Pasos para activarlo (cuando tengas el número dedicado)

### 1. Prepara el número bot
- Usa un **número dedicado** (nunca el personal): teléfono secundario/SIM extra.
- Agrégalo como **miembro del grupo** al que llegarán las señales.
- Nota: la automatización por WhatsApp Web es oficiosa; WhatsApp puede
  suspender temporalmente números que la usen. Por eso se recomienda un número
  solo para esto.

### 2. Configura el grupo
Edita `whatsapp_bridge\bridge.config.json`:

```json
{
  "grupo": "NOMBRE EXACTO DEL GRUPO",
  "puerto": 8120,
  "chromepath": "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
}
```

### 3. Inicia el puente y vincula el número
Doble clic en **`whatsapp_bridge\run_bridge.bat`** (o `node index.js` dentro de
esa carpeta). En la consola aparecerá un **código QR**: escanéalo con
**WhatsApp del número bot → Dispositivos vinculados → Vincular dispositivo**.

Cuando se vincule verás:
```
[WA] Conectado y listo.
[WA] Grupo resuelto: <nombre> -> <id>
```
Si el grupo no coincide, el puente lista los grupos disponibles; ajusta el
nombre exacto en la config y reinicia.

La sesión queda guardada en `whatsapp_bridge\sesion\` (no se vuelve a pedir QR,
salvo que cierres la sesión desde el teléfono).

### 4. Prueba manual (opcional)
Con el puente activo, envía un mensaje de prueba:
```powershell
Invoke-RestMethod -Uri http://127.0.0.1:8120/enviar -Method Post -Body '{"mensaje":"⚡ Prueba: señal GSR EURUSD CALL"}' -ContentType 'application/json'
```

### 5. Estado
- `http://127.0.0.1:8120/estado` → conectado/grupo resuelto.
- `http://127.0.0.1:8120/listar-grupos` → nombres de grupos visibles para el bot.

## Integración en el escáner
- `app_ali.py` hace POST a `http://127.0.0.1:8120/enviar` cuando `fire_signal`
  dispara (fase LISTA/ENTRADA). Si el puente está apagado, solo registra un
  aviso en el log (no afecta al escáner).
- Variables opcionales de entorno:
  - `WHATSAPP_BRIDGE_URL` (por defecto `http://127.0.0.1:8120`)
  - `WHATSAPP_BRIDGE_ENABLED=0` para desactivar sin tocar el código.

## Requisitos instalados
- Node.js + Chrome del sistema (el puente usa `whatsapp-web.js` con tu Chrome;
  no descarga Chromium).
- Dependencias ya instaladas en `whatsapp_bridge\node_modules`
  (`npm install --ignore-scripts`).
