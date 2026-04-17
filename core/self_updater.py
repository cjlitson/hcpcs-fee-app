"""In-app self-updater for VA HCPCS Fee Schedule Manager.

Downloads the latest HCPCSFeeApp.exe from GitHub Releases, saves it next to
the current executable as HCPCSFeeApp_new.exe, then launches a detached
dedicated updater helper executable (HCPCSFeeAppUpdater.exe).

Only works when running as a frozen PyInstaller .exe on Windows. All errors
are surfaced as exceptions so the caller can fall back gracefully.
"""

import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

UPDATE_LOG_FILENAME = "HCPCSFeeApp_update.log"
LAUNCHER_LOG_FILENAME = "HCPCSFeeApp_launcher.log"
UPDATER_HELPER_EXE_NAME = "HCPCSFeeAppUpdater.exe"


def _current_exe() -> Path:
    """Return the path to the running .exe.

    Raises RuntimeError when not running as a frozen executable.
    """
    if not getattr(sys, "frozen", False):
        raise RuntimeError("Self-update is only supported in the frozen .exe build.")
    return Path(sys.executable)


def download_update(asset_url: str, progress_callback=None) -> Path:
    """Download *asset_url* and save it next to the current exe.

    Parameters
    ----------
    asset_url:
        Direct download URL for the new HCPCSFeeApp.exe.
    progress_callback:
        Optional callable(downloaded_bytes, total_bytes) called periodically
        during the download.  Either argument may be 0/None if unknown.

    Returns the path to the downloaded file (``HCPCSFeeApp_new.exe``).
    Raises on any error.
    """
    import requests

    exe = _current_exe()
    dest = exe.parent / "HCPCSFeeApp_new.exe"

    resp = requests.get(asset_url, stream=True, timeout=30, verify=True)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0

    with open(dest, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=65536):
            if chunk:
                fh.write(chunk)
                downloaded += len(chunk)
                if progress_callback:
                    progress_callback(downloaded, total)

    # Verify the downloaded file is at least 1 MB (sanity-check for partial downloads)
    min_size = 1 * 1024 * 1024  # 1 MB
    actual_size = dest.stat().st_size
    if actual_size < min_size:
        dest.unlink(missing_ok=True)
        raise RuntimeError(
            f"Downloaded file is only {actual_size:,} bytes — expected at least "
            f"{min_size:,} bytes. The download may have been incomplete."
        )

    return dest


def apply_update(new_exe: Path) -> None:
    """Launch the updater helper executable and exit the current process.

    This function does not return on success — it calls ``sys.exit(0)`` after
    starting the helper process.
    """
    import subprocess

    exe = _current_exe()
    pid = os.getpid()
    log_path = Path(tempfile.gettempdir()) / UPDATE_LOG_FILENAME
    launcher_log_path = Path(tempfile.gettempdir()) / LAUNCHER_LOG_FILENAME
    helper_exe = exe.parent / UPDATER_HELPER_EXE_NAME

    helper_args = [
        str(helper_exe),
        "--current-exe",
        str(exe),
        "--new-exe",
        str(new_exe),
        "--pid",
        str(pid),
        "--log-path",
        str(log_path),
    ]

    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    creationflags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP

    def _launcher_log(message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(launcher_log_path, "a", encoding="utf-8") as fh:
            fh.write(f"[{timestamp}] {message}\n")

    _launcher_log("Launch attempt starting.")
    _launcher_log(f"Current exe: {exe}")
    _launcher_log(f"Downloaded exe: {new_exe}")
    _launcher_log(f"Updater helper path: {helper_exe}")
    _launcher_log(f"Updater log path: {log_path}")
    _launcher_log(f"Creation flags: {creationflags}")

    if not helper_exe.exists():
        _launcher_log("ERROR: Updater helper executable not found.")
        raise RuntimeError(f"Updater helper not found: {helper_exe}")

    try:
        subprocess.Popen(
            helper_args,
            creationflags=creationflags,
            close_fds=True,
        )
        _launcher_log("Updater helper launched successfully.")
    except Exception as exc:
        _launcher_log(f"ERROR: Failed to launch updater helper: {exc!r}")
        raise RuntimeError(f"Failed to launch updater helper: {exc}") from exc

    try:
        from PyQt6.QtWidgets import QApplication

        app = QApplication.instance()
        if app is not None:
            app.quit()
    except Exception as exc:
        _launcher_log(f"WARNING: QApplication quit failed: {exc!r}")

    sys.exit(0)
