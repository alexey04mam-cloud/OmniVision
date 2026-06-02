@echo off
echo Validating OmniVision...
echo.

echo [Python] main.py:
venv\Scripts\python.exe -c "import py_compile; py_compile.compile('main.py', doraise=True); print('  OK')"
echo.

echo [Python] telegram_bot.py:
venv\Scripts\python.exe -c "import py_compile; py_compile.compile('telegram_bot.py', doraise=True); print('  OK')"
echo.

echo [Python] scanners.py:
venv\Scripts\python.exe -c "import py_compile; py_compile.compile('scanners.py', doraise=True); print('  OK')"
echo.

echo [Python] pro_api.py:
venv\Scripts\python.exe -c "import py_compile; py_compile.compile('pro_api.py', doraise=True); print('  OK')"
echo.

echo Done!
pause
