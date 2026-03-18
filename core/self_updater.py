"""In-app self-updater for VA HCPCS Fee Schedule Manager.

Downloads the latest HCPCSFeeApp.exe from GitHub Releases, saves it next to
the current executable as HCPCSFeeApp_new.exe, then launches a small batch
script that waits for the current process to exit, swaps the files, restarts
the app, and deletes itself.

Only works when running as a frozen PyInstaller .exe on Windows.  All errors
are surfaced as exceptions so the caller can fall back gracefully.
"""

import os
import sys
import tempfile
from pathlib import Path


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
    """Launch the swap batch script and exit the current process.

    The batch script:
      1. Waits a few seconds for the current process to exit.
      2. Replaces the old exe with the new one.
      3. Re-launches the app.
      4. Deletes itself.

    This function does not return — it calls ``sys.exit(0)`` after launching
    the script.
    """
    import subprocess

    exe = _current_exe()
    pid = os.getpid()

    # Write the batch script to a temp file
    fd, bat_path = tempfile.mkstemp(suffix=".bat", prefix="hcpcs_update_")
    os.close(fd)

    bat_content = (
        "@echo off\r\n"
        "setlocal enabledelayedexpansion\r\n"
        f":: Waiting for process {pid} to exit...\r\n"
        f"set /a _tries=0\r\n"
        f":wait\r\n"
        f"set /a _tries=_tries+1\r\n"
        f"if !_tries! gtr 30 goto timeout\r\n"
        f"tasklist /FI \"PID eq {pid}\" /NH 2>NUL | find /I \"{pid}\" >NUL\r\n"
        f"if not errorlevel 1 (\r\n"
        f"    timeout /t 1 /nobreak >NUL\r\n"
        f"    goto wait\r\n"
        f")\r\n"
        f":do_swap\r\n"
        f":: Replace old exe with new exe\r\n"
        f"move /Y \"{new_exe}\" \"{exe}\"\r\n"
        f":: Restart the app\r\n"
        f"start \"\" \"{exe}\"\r\n"
        f"goto end\r\n"
        f":timeout\r\n"
        f":: Process did not exit within 30 seconds — attempt swap anyway (may fail if still running)\r\n"
        f"goto do_swap\r\n"
        f":end\r\n"
        f":: Delete this script\r\n"
        f"del \"%~f0\"\r\n"
    )

    with open(bat_path, "w", encoding="cp1252") as fh:
        fh.write(bat_content)

    # Launch the batch script detached (CREATE_NEW_PROCESS_GROUP + DETACHED_PROCESS)
    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    subprocess.Popen(
        ["cmd.exe", "/c", bat_path],
        creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
        close_fds=True,
    )

    sys.exit(0)
