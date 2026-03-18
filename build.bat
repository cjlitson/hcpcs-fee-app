@echo off
echo Installing dependencies...
pip install -r requirements.txt --quiet
pip install pyinstaller --quiet

echo Cleaning previous build...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist

echo Converting wsnc_map.png to wsnc_map.ico...
python -c "from PIL import Image; img = Image.open('assets/wsnc_map.png').convert('RGBA'); img.save('assets/wsnc_map.ico', format='ICO', sizes=[(16,16),(32,32),(48,48),(64,64),(128,128),(256,256)])"

echo Building VA HCPCS Fee Schedule Manager...
pyinstaller ^
  --onefile ^
  --windowed ^
  --name HCPCSFeeApp ^
  --icon=assets/wsnc_map.ico ^
  --add-data "assets;assets" ^
  --version-file version_info.txt ^
  --hidden-import PyQt6.QtPrintSupport ^
  --hidden-import reportlab.graphics ^
  --hidden-import pyodbc ^
  --hidden-import databricks.sql ^
  --hidden-import databricks.sql.client ^
  main.py

echo Packaging ZIP...
copy assets\wsnc_map.ico dist\wsnc_map.ico
copy Install.bat dist\Install.bat
copy INSTALL_README.txt dist\INSTALL_README.txt
powershell -NoProfile -Command "Compress-Archive -Force -Path dist\HCPCSFeeApp.exe,dist\Install.bat,dist\wsnc_map.ico,dist\INSTALL_README.txt -DestinationPath dist\HCPCSFeeApp-Setup.zip"

echo.
echo Build complete!
echo   EXE : dist\HCPCSFeeApp.exe
echo   ZIP : dist\HCPCSFeeApp-Setup.zip
pause
