@echo off
echo Installing dependencies...
pip install -r requirements.txt --quiet
pip install pyinstaller --quiet

echo Cleaning previous build...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

echo Converting app icon PNG to ICO...
python -c "from PIL import Image; img = Image.open('assets/app_icon.png').convert('RGBA'); img.save('assets/app_icon.ico', format='ICO', sizes=[(16,16),(32,32),(48,48),(64,64),(128,128),(256,256)])"

echo Building VA HCPCS Fee Schedule Manager...
pyinstaller ^
  --onefile ^
  --windowed ^
  --name HCPCSFeeApp ^
  --icon=assets/app_icon.ico ^
  --add-data "assets;assets" ^
  --version-file version_info.txt ^
  --hidden-import PyQt6.QtPrintSupport ^
  --hidden-import reportlab.graphics ^
  --hidden-import pyodbc ^
  --hidden-import databricks.sql ^
  --hidden-import databricks.sql.client ^
  main.py

echo Packaging ZIP...
copy assets\app_icon.ico dist\app_icon.ico
copy Install.ps1 dist\Install.ps1
copy INSTALL_README.txt dist\INSTALL_README.txt
powershell -NoProfile -Command "Compress-Archive -Force -Path dist\HCPCSFeeApp.exe,dist\Install.ps1,dist\app_icon.ico,dist\INSTALL_README.txt -DestinationPath dist\HCPCSFeeApp-Setup.zip"

echo.
echo Build complete!
echo   EXE : dist\HCPCSFeeApp.exe
echo   ZIP : dist\HCPCSFeeApp-Setup.zip
pause
