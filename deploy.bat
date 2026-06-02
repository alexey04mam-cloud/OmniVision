@echo off
echo ========================================
echo   OmniVision - Deploy to Railway
echo ========================================
echo.

:: Remove git lock files if they exist
if exist .git\HEAD.lock (
    del .git\HEAD.lock
    echo [OK] Removed HEAD.lock
)
if exist .git\index.lock (
    del .git\index.lock
    echo [OK] Removed index.lock
)

:: Validate Python syntax
echo.
echo [1/4] Validating Python...
venv\Scripts\python.exe -c "import py_compile; py_compile.compile('main.py', doraise=True); print('  main.py OK')"
if errorlevel 1 (
    echo [FAIL] Python syntax error! Fix before deploy.
    pause
    exit /b 1
)

:: Git add
echo.
echo [2/4] Staging files...
git add -A
if errorlevel 1 (
    echo [FAIL] git add failed
    pause
    exit /b 1
)

:: Git commit
echo.
echo [3/4] Committing...
set /p MSG="Commit message (or press Enter for 'update'): "
if "%MSG%"=="" set MSG=update
git commit -m "%MSG%"
if errorlevel 1 (
    echo [INFO] Nothing to commit or commit failed
)

:: Git push
echo.
echo [4/4] Pushing to Railway...
git push origin main
if errorlevel 1 (
    echo [FAIL] Push failed. Check your connection.
    pause
    exit /b 1
)

echo.
echo ========================================
echo   Deploy complete!
echo   https://dependable-tranquility-production-d86f.up.railway.app
echo ========================================
pause
