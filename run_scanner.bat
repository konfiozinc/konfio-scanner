@echo off
chcp 65001 >nul
setlocal
title AppALI GSR Scanner - Servidor Activo
mode con: cols=95 lines=30
color 0B

:: Trabajar SIEMPRE desde la carpeta donde vive este .bat
cd /d "%~dp0"

echo ====================================================
echo   AppALI GSR Scanner - Iniciando servidor...
echo   Carpeta: %CD%
echo ====================================================
echo.

:: ------------------------------------------------------------------
:: Credenciales de entorno (tambien estan como valores por defecto en app_ali.py)
:: ------------------------------------------------------------------
set "IQ_EMAIL=***REDACTED***"
set "IQ_PASSWORD=***REDACTED***"
set "IQ_ACCOUNT_TYPE=PRACTICE"

:: ------------------------------------------------------------------
:: Elegir el Python:
::   1) venv311  = Python 3.11 + libreria REAL de IQ Option  (preferido)
::   2) venv     = Python 3.14 (solo SIMULACION)
::   3) python   = del sistema
:: ------------------------------------------------------------------
set "PYEXE="
set "MODO="
if exist "%~dp0venv311\Scripts\python.exe" set "PYEXE=%~dp0venv311\Scripts\python.exe"
if exist "%~dp0venv311\Scripts\python.exe" set "MODO=REAL (venv311 - IQ Option)"
if not defined PYEXE if exist "%~dp0venv\Scripts\python.exe" set "PYEXE=%~dp0venv\Scripts\python.exe"
if not defined MODO if exist "%~dp0venv\Scripts\python.exe" set "MODO=SIMULACION (venv - sin broker)"
if not defined PYEXE set "PYEXE=python"
if not defined MODO set "MODO=python del sistema"

if exist "%PYEXE%" goto ok_python

echo [ERROR] No se encontro Python: %PYEXE%
echo Crea el entorno con:  python -m venv venv311
echo                    y:  venv311\Scripts\pip install -r requirements.txt
echo (para modo real se necesita Python 3.11 y la libreria iqoptionapi clasica)
echo.
pause
exit /b 1

:ok_python
echo Python usado: %PYEXE%
echo Modo:         %MODO%
echo.

:: ------------------------------------------------------------------
:: Si ya hay un servidor en el puerto 8000: abrir el panel existente
:: y salir limpio (no duplicar ni quedarse esperando una tecla).
:: ------------------------------------------------------------------
netstat -ano | findstr /C:":8000" | findstr /C:"LISTENING" >nul 2>&1
if errorlevel 1 goto puerto_libre

echo [INFO] Ya hay un servidor AppALI activo en el puerto 8000.
echo        Abriendo el panel existente en el navegador...
echo        (Si quieres reiniciarlo, cierra primero la otra ventana del servidor.)
start "" http://localhost:8000/
timeout /t 2 /nobreak >nul
exit /b 0

:puerto_libre

:: ------------------------------------------------------------------
:: Abrir el navegador 3 segundos despues (cuando el servidor ya escuche)
:: (se omite si SKIP_BROWSER=1, util para pruebas)
:: ------------------------------------------------------------------
if /i "%SKIP_BROWSER%"=="1" goto iniciar
start "" /b cmd /c "timeout /t 3 /nobreak >nul & start http://localhost:8000/"

:iniciar
echo.
echo Servidor en:  http://localhost:8000/
echo Cuenta IQ:    %IQ_EMAIL%   (modo %IQ_ACCOUNT_TYPE%)
echo.
echo Iniciando... (para detener: cierra esta ventana o Ctrl+C)
echo.

"%PYEXE%" app_ali.py

echo.
echo El servidor se detuvo (codigo %errorlevel%).
pause
