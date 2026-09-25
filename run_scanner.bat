@echo off
chcp 65001 >nul 2>&1
setlocal enableextensions
title AppALI GSR Scanner - Servidor Activo
mode con: cols=100 lines=32 >nul 2>&1
color 0B >nul 2>&1

:: Trabajar SIEMPRE desde la carpeta donde vive este .bat
cd /d "%~dp0"

:: Salida UTF-8 para que los emojis del log no rompan la consola de Windows
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUNBUFFERED=1"

echo ====================================================
echo   AppALI GSR Scanner - Iniciando servidor...
echo   Carpeta: %CD%
echo ====================================================
echo.

:: ------------------------------------------------------------------
:: Credenciales: se leen del archivo .env (local, ignorado por git) o del
:: entorno. Ver .env.example. NO se hardcodean aqui.
:: ------------------------------------------------------------------
if not defined IQ_ACCOUNT_TYPE set "IQ_ACCOUNT_TYPE=PRACTICE"

rem ------------------------------------------------------------------
rem Elegir el Python.
rem   1) venv311 = Python 3.11 + libreria REAL de IQ Option  (preferido)
rem   2) venv    = Python 3.14 (solo SIMULACION, si existe)
rem   3) python  = del sistema
rem
rem IMPORTANTE: no basta con que exista Scripts\python.exe. Ese archivo es un
rem redireccionador: si el Python base con el que se creo el entorno ya no
rem existe (por ejemplo si se desinstalo C:\Python314), lanza el error
rem   did not find executable at ...
rem y el servidor no arranca. Por eso cada candidato se PRUEBA de verdad antes
rem de usarlo, y ademas se comprueba que sus dependencias se puedan importar.
rem ------------------------------------------------------------------
set "PYEXE="
set "MODO="

call :probar "%~dp0venv311\Scripts\python.exe" "REAL (venv311 - IQ Option)"
if defined PYEXE goto ok_python
call :probar "%~dp0venv\Scripts\python.exe" "SIMULACION (venv - sin broker real)"
if defined PYEXE goto ok_python
call :probar "python" "python del sistema"
if defined PYEXE goto ok_python

echo [ERROR] No hay ningun interprete de Python FUNCIONAL en este equipo.
echo.
echo   Para el MODO REAL (recomendado):
echo      py -3.11 -m venv venv311
echo      venv311\Scripts\python -m pip install -r requirements.txt
echo      venv311\Scripts\python -m pip install https://github.com/iqoptionapi/iqoptionapi/archive/refs/heads/master.zip
echo.
echo   Si un venv ya existe pero falla, es porque su Python base desaparecio:
echo   borralo con  rmdir /s /q venv311  y vuelve a crearlo como arriba.
echo.
pause
exit /b 1

:ok_python
echo Python usado: %PYEXE%
echo Modo:         %MODO%
"%PYEXE%" -c "import sys;print('Version:      ', sys.version.split()[0])"
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
echo Cuenta IQ:    configurada en .env   (modo %IQ_ACCOUNT_TYPE%)
echo.
echo Iniciando... (para detener: cierra esta ventana o Ctrl+C)
echo.

"%PYEXE%" app_ali.py

echo.
echo El servidor se detuvo (codigo %errorlevel%).
pause
exit /b %errorlevel%

:: ------------------------------------------------------------------
:: :probar  <ruta o comando python>  <etiqueta del modo>
:: Prueba que el interprete funcione Y que sus dependencias importen.
:: ------------------------------------------------------------------
:probar
if defined PYEXE exit /b 0
if /i not "%~1"=="python" if not exist "%~1" exit /b 0
"%~1" -c "import sys" >nul 2>&1
if errorlevel 1 (
    if /i not "%~1"=="python" echo [AVISO] Interprete no utilizable: %~1
    exit /b 0
)
"%~1" -c "import uvicorn, numpy, pandas, ta, fastapi" >nul 2>&1
if errorlevel 1 (
    echo [AVISO] A %~1 le faltan dependencias: uvicorn / numpy / pandas / ta / fastapi
    echo         Instalalas con:  "%~1" -m pip install -r requirements.txt
    exit /b 0
)
set "PYEXE=%~1"
set "MODO=%~2"
exit /b 0
