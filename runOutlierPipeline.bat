@echo off
title Portable Crew Outlier Pipeline
color 0b

:: 1. Move to the folder where the .bat is located
cd /d "%~dp0"

:: 2. Look for the python engine ONE LEVEL UP (..)
set PY_PATH="..\.venv\Scripts\python.exe"

:: 3. Check if the Virtual Environment folder exists
if not exist %PY_PATH% (
    echo [ERROR] Virtual Environment not found in the parent folder!
    echo Looking for: %PY_PATH%
    pause
    exit
)

:: 4. Run the script using the correct pathing
echo Starting Outlier Pipeline...
echo ---------------------------------------
%PY_PATH% "outlierPipeline.py"

echo ---------------------------------------
echo Process Complete!
pause
