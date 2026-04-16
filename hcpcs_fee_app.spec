from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules


PROJECT_ROOT = Path(SPECPATH).resolve()

# Collect all application modules so lazy imports do not break frozen startup.
HIDDENIMPORTS = sorted(
    set(
        collect_submodules("ui")
        + collect_submodules("core")
        + [
            "PyQt6.QtPrintSupport",
            "reportlab.graphics",
            "pyodbc",
            "databricks.sql",
            "databricks.sql.client",
        ]
    )
)

a = Analysis(
    ["main.py"],
    pathex=[str(PROJECT_ROOT)],
    binaries=[],
    datas=[(str(PROJECT_ROOT / "assets"), "assets")],
    hiddenimports=HIDDENIMPORTS,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="HCPCSFeeApp",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=[str(PROJECT_ROOT / "assets" / "wsnc_map.ico")],
    version=str(PROJECT_ROOT / "version_info.txt"),
)
