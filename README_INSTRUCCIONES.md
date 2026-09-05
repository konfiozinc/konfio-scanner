# APPALÍ SCANNER — Entrega técnica

Scanner GSR M1 conectado directamente a **IQ Option como única fuente de verdad**.
Proyecto: `KONFIO_ZINC\SCANNER\app_ali.py`.

## Requisitos
- Windows + Python 3.11 (entorno `venv311` ya preparado en esta carpeta).
- Dependencias ya instaladas en `venv311` (fastapi, uvicorn, numpy, pandas, ta,
  websockets, requests, iqoptionapi clásico con `stable_api`).

## Comando exacto para ejecutar
```powershell
cd C:\Users\usuario29\Documents\KONFIO_ZINC\SCANNER
venv311\Scripts\python.exe app_ali.py
```
O doble clic en `run_scanner.bat` (usa `venv311` y abre http://localhost:8000).

## Variables de entorno opcionales
- `BROKER_DIAGNOSTIC=1` → imprime el diagnóstico real del broker al conectar.
- `WHATSAPP_BRIDGE_URL=http://127.0.0.1:8120` (puente de señales al grupo).
- `WHATSAPP_BRIDGE_ENABLED=0` → desactiva el envío a WhatsApp.
- `IQ_EMAIL`, `IQ_PASSWORD`, `IQ_ACCOUNT_TYPE` (PRACTICE/REAL).

## Arquitectura implementada (IQ Option = fuente de verdad)
- **BrokerMarketState**: snapshot de disponibilidad vía `get_all_init_v2()`
  (binary/turbo; digital aislado). Estados `LIVE / STALE / UNKNOWN`, refresco
  controlado (no por ciclo), defensivo ante `None`.
- **Universo dinámico**: se construye desde el snapshot del broker filtrando
  pares de divisas por la descripción del instrumento (`XXX/YYY`). Sin lista fija.
- **CandleStreamManager**: realtime con `start_candles_stream` /
  `get_realtime_candles` solo para activos OPEN; detiene stream y limpia
  GSR/proximidad/velas al cerrarse. `get_candles` queda solo como histórico.
- **ConnectionManager**: una sola instancia IQ + reconexión con candado.
- **validate_signal_eligibility**: bloquea la señal si no hay conexión,
  disponibilidad, activo OPEN o velas actuales; registra `SIGNAL_BLOCKED`.
- **GSR intacto**: Bollinger(20,2) + RSI(14, 80/20) + DAMOA(5, +10/-10),
  ruptura 40% del cuerpo fuera de banda, 2 velas consecutivas validadas
  individualmente (cuerpo real, sin mechas).
- **Dashboard**: estados BROKER/AVAILABILITY/CANDLES/SCANNER + métricas
  (REAL/OTC/CLOSED/STALE/NO_DATA/STREAMS/CYCLES). Reloj con `get_server_timestamp`.
- **Favicon**: responde `204 No Content`; ASGI cumple HTTP/WebSocket/Lifespan.

## Pruebas reales realizadas
- Conexión: `CONNECTION OK / ACCOUNT PRACTICE` (BROKER DIAGNOSTIC).
- Disponibilidad: `[BROKER] Disponibilidad LIVE: 274 instrumentos (182 abiertos)`.
- Realtime: `streams_active > 0` con logs `[STREAM] iniciado para ...`.
- Bloqueo: con `availability UNKNOWN` → 0 operables y `candles STALE` (sin señales).
- Recuperación: al pasar a LIVE se revalida y abre (ej. 31 operables).
- GSR: lógica conservada; la señal solo se emite tras `validate_signal_eligibility`.
