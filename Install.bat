@echo off
setlocal enabledelayedexpansion

set APP_NAME=VA HCPCS Fee Schedule Manager
set EXE_NAME=HCPCSFeeApp.exe
set ICO_NAME=wsnc_map.ico
set SHORTCUT_NAME=VA HCPCS Fee Schedule Manager.lnk
set INSTALL_DIR=%USERPROFILE%\Documents\HCPCSFeeApp
set DATA_DIR=%INSTALL_DIR%\data

echo.
echo ========================================
echo   %APP_NAME%
echo   Per-User Installer
echo ========================================
echo.

:: -- Locate files relative to this script ------------------------------------
set SCRIPT_DIR=%~dp0
set SRC_EXE=%SCRIPT_DIR%%EXE_NAME%
set SRC_ICO=%SCRIPT_DIR%%ICO_NAME%

if not exist "%SRC_EXE%" (
    echo ERROR: %EXE_NAME% not found next to Install.bat.
    echo        Make sure you extracted the full ZIP before running this installer.
    echo.
    pause
    exit /b 1
)

:: -- Create install directory ------------------------------------------------
echo Creating install directory...
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"
if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"

:: -- Copy application files --------------------------------------------------
echo Copying %EXE_NAME%...
copy /y "%SRC_EXE%" "%INSTALL_DIR%\%EXE_NAME%" >nul

if exist "%SRC_ICO%" (
    echo Copying %ICO_NAME%...
    copy /y "%SRC_ICO%" "%INSTALL_DIR%\%ICO_NAME%" >nul
)

:: -- Create desktop shortcut via temporary VBScript --------------------------
echo Creating desktop shortcut...

set DESKTOP=%USERPROFILE%\Desktop
set SHORTCUT_PATH=%DESKTOP%\%SHORTCUT_NAME%
set TARGET_EXE=%INSTALL_DIR%\%EXE_NAME%

if exist "%INSTALL_DIR%\%ICO_NAME%" (
    set ICON_PATH=%INSTALL_DIR%\%ICO_NAME%
) else (
    set ICON_PATH=%TARGET_EXE%
)

set VBS_FILE=%TEMP%\create_shortcut_%RANDOM%.vbs

(
    echo Set ws = CreateObject("WScript.Shell"^)
    echo Set sc = ws.CreateShortcut("%SHORTCUT_PATH%"^)
    echo sc.TargetPath = "%TARGET_EXE%"
    echo sc.WorkingDirectory = "%INSTALL_DIR%"
    echo sc.Description = "%APP_NAME%"
    echo sc.IconLocation = "%ICON_PATH%,0"
    echo sc.Save
) > "%VBS_FILE%"

cscript //nologo "%VBS_FILE%"
del "%VBS_FILE%"

echo.
echo ========================================
echo   Installation complete!
echo ========================================
echo.
echo   Installed to : %INSTALL_DIR%
echo   Shortcut     : %SHORTCUT_PATH%
echo.
echo Double-click the desktop shortcut to launch the app.
echo.
pause
