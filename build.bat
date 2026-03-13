@echo off
echo Building VA HCPCS Fee Schedule Manager...
pip install pyinstaller --quiet
pyinstaller --onefile --windowed --name HCPCSFeeApp --icon NONE main.py
echo.
echo Build complete: dist\HCPCSFeeApp.exe
pause
