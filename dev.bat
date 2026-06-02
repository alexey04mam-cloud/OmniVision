@echo off
echo ========================================
echo   OmniVision - Local Dev Server
echo ========================================
echo.
echo Starting at http://localhost:8000
echo Press Ctrl+C to stop
echo.
venv\Scripts\python.exe -m uvicorn main:app --reload --port 8000
