@echo off
echo Installing dependencies...
pip install -r requirements.txt --quiet
pip install pyinstaller --quiet

echo Cleaning previous build...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

echo Building VA HCPCS Fee Schedule Manager...
pyinstaller ^
  --onefile ^
  --windowed ^
  --name HCPCSFeeApp ^
  --hidden-import PyQt6.QtPrintSupport ^
  --hidden-import reportlab.graphics ^
  main.py

echo.
echo Build complete: dist\HCPCSFeeApp.exe
pause
