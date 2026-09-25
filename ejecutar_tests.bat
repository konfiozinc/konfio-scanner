@echo off
chcp 65001 >nul 2>&1
setlocal enableextensions
title AppALI GSR Scanner - Tests del motor
cd /d "%~dp0"

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUNBUFFERED=1"

echo ====================================================
echo   AppALI GSR Scanner - Tests automaticos
echo   Carpeta: %CD%
echo ====================================================
echo.

rem ------------------------------------------------------------------
rem Estos tests NO necesitan conexion ni credenciales: verifica el motor
rem (blindaje de la libreria, reloj, cache de velas, patron GSR completo y
rem equivalencia de los indicadores) con datos sinteticos.
rem ------------------------------------------------------------------
set "PYEXE="
call :probar "%~dp0venv311\Scripts\python.exe"
if defined PYEXE goto run
call :probar "%~dp0venv\Scripts\python.exe"
if defined PYEXE goto run
call :probar "python"
if defined PYEXE goto run

echo [ERROR] No hay ningun Python funcional con las dependencias necesarias.
echo.
echo   Crea el entorno con:
echo      py -3.11 -m venv venv311
echo      venv311\Scripts\python -m pip install -r requirements.txt
echo.
if /i not "%NO_PAUSE%"=="1" pause
exit /b 1

:run
echo Python: %PYEXE%
echo.

set "FALLOS=0"

"%PYEXE%" test_scanner.py
if errorlevel 1 set "FALLOS=1"

echo.
"%PYEXE%" test_indicadores.py
if errorlevel 1 set "FALLOS=1"

echo.
echo ====================================================
if "%FALLOS%"=="1" echo   RESULTADO GLOBAL: HAY FALLOS
if "%FALLOS%"=="0" echo   RESULTADO GLOBAL: TODO OK
echo ====================================================
rem Para uso automatizado (CI, redireccion de salida):  set NO_PAUSE=1
if /i not "%NO_PAUSE%"=="1" pause
exit /b %FALLOS%

rem ------------------------------------------------------------------
rem :probar ^<ruta o comando python^>
rem Solo acepta interpretes que funcionen y tengan las dependencias.
rem ------------------------------------------------------------------
:probar
if defined PYEXE exit /b 0
if /i not "%~1"=="python" if not exist "%~1" exit /b 0
"%~1" -c "import numpy, pandas, ta" >nul 2>&1
if errorlevel 1 exit /b 0
set "PYEXE=%~1"
exit /b 0
