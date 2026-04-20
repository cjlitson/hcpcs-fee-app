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
pyinstaller --clean --noconfirm hcpcs_fee_app.spec
pyinstaller --clean --noconfirm --onefile --noconsole --name HCPCSFeeAppUpdater updater_main.py

echo Packaging ZIP...
:: Create AppFiles subfolder and copy app files into it
if not exist dist\AppFiles mkdir dist\AppFiles
copy /y assets\wsnc_map.ico dist\AppFiles\wsnc_map.ico
copy /y dist\HCPCSFeeApp.exe dist\AppFiles\HCPCSFeeApp.exe
copy /y dist\HCPCSFeeAppUpdater.exe dist\AppFiles\HCPCSFeeAppUpdater.exe
:: Keep Install.bat and INSTALL_README.txt at dist root
copy /y Install.bat dist\Install.bat
copy /y INSTALL_README.txt dist\INSTALL_README.txt
:: Build ZIP with Install.bat + INSTALL_README.txt at root and AppFiles/ subfolder
powershell -NoProfile -Command "Compress-Archive -Force -Path dist\Install.bat,dist\INSTALL_README.txt,dist\AppFiles -DestinationPath dist\HCPCSFeeApp-Setup.zip"

echo.
echo Build complete!
echo   EXE : dist\HCPCSFeeApp.exe
echo   ZIP : dist\HCPCSFeeApp-Setup.zip
pause
