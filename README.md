# SCANNER

Scanner/robot de señales para trading con IQ Option (estrategia GSR, velas M1).

- Backend/servidor: `app_ali.py` (entorno `venv311` = Python 3.11 + librería
  clásica `iqoptionapi` para el modo REAL del broker).
- Dashboard web: se sirve desde el propio `app_ali.py` en
  `http://localhost:8000/` (no hay `index.html` suelto; el panel está embebido).
- Puente de señales a WhatsApp: `whatsapp_bridge\` (Node.js + Baileys).

Para ejecutarlo: doble clic en `run_scanner.bat` (o sigue
`README_INSTRUCCIONES.md`).
