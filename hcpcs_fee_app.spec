from pathlib import Path
import sys

from PyInstaller.utils.hooks import collect_submodules


PROJECT_ROOT = Path(SPECPATH).resolve()

# Ensure the project root is on sys.path so collect_submodules can discover
# all ui and core packages even when PyInstaller is invoked from a different
# working directory.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Collect all application modules so lazy imports do not break frozen startup.
HIDDENIMPORTS = sorted(
    set(
        collect_submodules("ui")
        + collect_submodules("core")
        + [
            # Explicit entries as belt-and-suspenders for modules imported
            # lazily inside functions (collect_submodules may miss them if
            # sys.path was not set up before spec execution).
            "ui.export_dialog",
            "ui.generate_document_dialog",
            "ui.purchase_list_dialog",
            "ui.purchase_list_panel",
            "ui.group_browser_dialog",
            "ui.dev_tools_dialog",
            "ui.setup_wizard",
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
