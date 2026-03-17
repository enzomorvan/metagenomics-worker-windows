@echo off
REM ===========================================================================
REM METAGENOMICS WORKER SETUP
REM Everything installs into the same folder as this .bat file.
REM Double-click to install, then double-click Start_Worker.bat to run.
REM ===========================================================================

REM Install into the folder where this .bat lives
set DIR=%~dp0
set DIR=%DIR:~0,-1%
set TOOLS=%DIR%\tools
set BIN=%DIR%\tools\bin
set SRADIR=%DIR%\tools\sratoolkit

REM Check for spaces in path (Conda can't handle them)
echo "%DIR%" | findstr " " >nul 2>&1
if %errorlevel% equ 0 goto :bad_path
goto :path_ok

:bad_path
echo.
echo   ERROR: This folder has spaces in its path:
echo     %DIR%
echo.
echo   Move the metagenomics_worker folder to a path
echo   without spaces, for example:
echo     D:\metagenomics_worker
echo     C:\metagenomics_worker
echo.
pause
exit /b 1

:path_ok
echo.
echo ============================================
echo   Metagenomics Worker Setup
echo   Installing to: %DIR%
echo ============================================
echo.

if not exist "%TOOLS%" mkdir "%TOOLS%"
if not exist "%BIN%" mkdir "%BIN%"

REM ---- Python (Miniforge) ----------------------------------------------------
echo [1/2] Python + cutadapt...
if exist "%TOOLS%\miniforge3\python.exe" goto :py_ok

echo   Downloading Miniforge...
powershell -Command "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Windows-x86_64.exe' -OutFile '%TEMP%\mf.exe'"
echo   Installing...
start /wait "" "%TEMP%\mf.exe" /S /D=%TOOLS%\miniforge3
del "%TEMP%\mf.exe" >nul 2>&1
"%TOOLS%\miniforge3\Scripts\conda.exe" install -y -q requests >nul 2>&1

"%TOOLS%\miniforge3\python.exe" -c "import cutadapt" >nul 2>&1
if %errorlevel% equ 0 goto :py_ok
echo   Installing cutadapt...
"%TOOLS%\miniforge3\Scripts\pip.exe" install cutadapt >nul 2>&1

:py_ok
echo   OK

REM ---- DIAMOND ---------------------------------------------------------------
echo [2/2] DIAMOND...
if exist "%BIN%\diamond.exe" goto :dia_ok

echo   Downloading...
powershell -Command "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://github.com/bbuchfink/diamond/releases/download/v2.1.10/diamond-windows.zip' -OutFile '%TEMP%\diamond.zip'"
powershell -Command "Expand-Archive '%TEMP%\diamond.zip' '%TEMP%\diamond_tmp' -Force"
copy "%TEMP%\diamond_tmp\diamond.exe" "%BIN%\" >nul
rd /s /q "%TEMP%\diamond_tmp" >nul 2>&1
del "%TEMP%\diamond.zip" >nul 2>&1

:dia_ok
echo   OK

REM ---- SRA Toolkit + worker files (included in zip) --------------------------
echo SRA Toolkit: OK

REM ---- Worker files (already included in zip) --------------------------------
set COORDINATOR_URL=http://194.164.206.175/compute
set API_KEY=jhyPOTYST8E_xyjEAyRJ1LWrMRoZeE33kV6fW9pgIQA
echo Worker files: OK

REM ---- Create Start_Worker.bat -----------------------------------------------
echo.
echo Creating Start_Worker.bat...

set WORKER=%DIR%\Start_Worker.bat
echo @echo off> "%WORKER%"
echo cd /d "%DIR%">> "%WORKER%"
echo set PATH=%BIN%;%SRADIR%\bin;%%PATH%%>> "%WORKER%"
echo set COORDINATOR_URL=%COORDINATOR_URL%>> "%WORKER%"
echo set API_KEY=%API_KEY%>> "%WORKER%"
echo set WORKER_NAME=%%COMPUTERNAME%%>> "%WORKER%"
echo set WORK_DIR=%DIR%>> "%WORKER%"
echo "%TOOLS%\miniforge3\pythonw.exe" -m worker.gui>> "%WORKER%"

echo   OK

REM ---- Done ------------------------------------------------------------------
echo.
echo ============================================
echo   Setup complete!
echo ============================================
echo.
echo   To start processing, double-click:
echo     %WORKER%
echo.
echo   Starting worker now...
echo.

cd /d "%DIR%"
set PATH=%BIN%;%SRADIR%\bin;%PATH%
set WORKER_NAME=%COMPUTERNAME%
set WORK_DIR=%DIR%
"%TOOLS%\miniforge3\pythonw.exe" -m worker.gui
