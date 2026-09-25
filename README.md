# SCANNER

Scanner/robot de señales para trading con IQ Option (estrategia GSR, velas M1).

- Backend/servidor: `app_ali.py` (entorno `venv311` = Python 3.11 + librería
  clásica `iqoptionapi` para el modo REAL del broker).
- Dashboard web: se sirve desde el propio `app_ali.py` en
  `http://localhost:8000/` (no hay `index.html` suelto; el panel está embebido).
- Estado de salud por HTTP: `http://localhost:8000/api/status`.
- Publicación de señales en **Alí Binary Options** (Firebase): por Cloud Function
  `postSignal` (`APP_SIGNAL_URL` + `APP_SIGNAL_SECRET`, opción recomendada) o
  escribiendo directo en Firestore con service account (`FIREBASE_SA_PATH`).
  Si no hay ninguna configurada, la señal sólo queda en el log y en
  `signals_audit.csv` y el panel muestra el aviso **APP OFF**.
- Backtest con velas reales: `exportar_velas.py` + `backtest_gsr.py --carpeta`.

Para ejecutarlo: doble clic en `run_scanner.bat` (o sigue
`README_INSTRUCCIONES.md`).
Para comprobar el motor sin broker: doble clic en `ejecutar_tests.bat`.
