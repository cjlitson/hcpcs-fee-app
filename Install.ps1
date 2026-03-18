# Install.ps1 — Per-user installer for VA HCPCS Fee Schedule Manager
# No administrator rights required.  Everything is installed to the user's
# Documents folder, which is always writable without elevation.

$ErrorActionPreference = "Stop"

$appName   = "VA HCPCS Fee Schedule Manager"
$exeName   = "HCPCSFeeApp.exe"
$icoName   = "app_icon.ico"
$shortcut  = "VA HCPCS Fee Schedule Manager.lnk"
$installDir = Join-Path ([Environment]::GetFolderPath("MyDocuments")) "HCPCSFeeApp"
$dataDir    = Join-Path $installDir "data"

Write-Host ""
Write-Host "========================================"
Write-Host "  $appName"
Write-Host "  Per-User Installer"
Write-Host "========================================"
Write-Host ""

# -- Locate files relative to this script ------------------------------------
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$srcExe    = Join-Path $scriptDir $exeName
$srcIco    = Join-Path $scriptDir $icoName

if (-not (Test-Path $srcExe)) {
    Write-Host "ERROR: $exeName not found next to Install.ps1." -ForegroundColor Red
    Write-Host "       Make sure you extracted the full ZIP before running this script."
    Read-Host "Press Enter to exit"
    exit 1
}

# -- Create install directory ------------------------------------------------
Write-Host "Creating install directory..."
New-Item -ItemType Directory -Force -Path $installDir | Out-Null
New-Item -ItemType Directory -Force -Path $dataDir    | Out-Null

# -- Copy application files --------------------------------------------------
Write-Host "Copying $exeName..."
Copy-Item -Force $srcExe (Join-Path $installDir $exeName)

if (Test-Path $srcIco) {
    Write-Host "Copying $icoName..."
    Copy-Item -Force $srcIco (Join-Path $installDir $icoName)
}

# -- Create desktop shortcut -------------------------------------------------
Write-Host "Creating desktop shortcut..."

$desktopPath  = [Environment]::GetFolderPath("Desktop")
$shortcutPath = Join-Path $desktopPath $shortcut
$targetExe    = Join-Path $installDir $exeName
$iconPath     = if (Test-Path (Join-Path $installDir $icoName)) {
                    Join-Path $installDir $icoName
                } else {
                    $targetExe
                }

$ws = New-Object -ComObject WScript.Shell
$sc = $ws.CreateShortcut($shortcutPath)
$sc.TargetPath       = $targetExe
$sc.WorkingDirectory = $installDir
$sc.Description      = $appName
$sc.IconLocation     = "$iconPath,0"
$sc.Save()

Write-Host ""
Write-Host "========================================"
Write-Host "  Installation complete!"
Write-Host "========================================"
Write-Host ""
Write-Host "  Installed to : $installDir"
Write-Host "  Shortcut     : $shortcutPath"
Write-Host ""
Write-Host "Double-click the desktop shortcut to launch the app."
Write-Host ""
Read-Host "Press Enter to close"
