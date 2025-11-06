@echo off
setlocal
set SCRIPT_DIR=%~dp0
set PYTHON_CMD=python

if exist "%SCRIPT_DIR%venv\Scripts\python.exe" set PYTHON_CMD=%SCRIPT_DIR%venv\Scripts\python.exe
if exist "%SCRIPT_DIR%.venv\Scripts\python.exe" set PYTHON_CMD=%SCRIPT_DIR%.venv\Scripts\python.exe
if exist "%SCRIPT_DIR%env\Scripts\python.exe" set PYTHON_CMD=%SCRIPT_DIR%env\Scripts\python.exe

pushd "%SCRIPT_DIR%"
echo Checking for RugBase updates...
"%PYTHON_CMD%" "%SCRIPT_DIR%core\updater.py" --batch-update
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% NEQ 0 (
    echo Update failed. Please review the error above.
    popd
    pause
    exit /b %EXIT_CODE%
)

echo RugBase is now up to date.
popd
pause
exit /b 0
