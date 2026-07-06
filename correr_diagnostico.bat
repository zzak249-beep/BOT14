@echo off
echo Instalando aiohttp si hace falta...
py -3.11 -m pip install aiohttp --quiet
echo.
echo Corriendo el diagnostico...
echo.
py -3.11 diagnostico_bingx.py
