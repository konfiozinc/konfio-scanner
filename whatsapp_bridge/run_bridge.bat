@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Puente WhatsApp AppALI GSR
echo ====================================================
echo   Puente WhatsApp AppALI - iniciando...
echo   Escanea el QR con el numero dedicado (3135235680)
echo ====================================================
echo.
node index.js
pause
