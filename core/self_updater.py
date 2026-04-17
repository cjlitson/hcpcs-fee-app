"""In-app self-updater for VA HCPCS Fee Schedule Manager.

Downloads the latest HCPCSFeeApp.exe from GitHub Releases, saves it next to
the current executable as HCPCSFeeApp_new.exe, then launches a detached batch
script that:

  1. Waits up to 30 s for the current process to exit.
  2. Pauses 2 s so Windows fully releases the file handle.
  3. Deletes the old exe (up to 10 retries × 1 s each).
  4. Renames/moves the new exe into place (up to 5 retries × 2 s each).
  5. Verifies the replacement exists, then re-launches the app.
  6. Logs every major step to ``%TEMP%\\HCPCSFeeApp_update.log``.
  7. Leaves a clear manual-recovery message in the log on failure.
  8. Deletes itself.

Only works when running as a frozen PyInstaller .exe on Windows.  All errors
are surfaced as exceptions so the caller can fall back gracefully.
"""

import os
import sys
import tempfile
from pathlib import Path

UPDATE_LOG_FILENAME = "HCPCSFeeApp_update.log"


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
      1. Waits up to 30 s for the current process to exit.
      2. Pauses briefly to let Windows fully release the file handle.
      3. Deletes the old exe (up to 10 retries) before renaming.
      4. Renames/moves the new exe into place (up to 5 retries).
      5. Verifies the replacement exists, then re-launches the app.
      6. Logs every major step and leaves a manual-recovery message on failure.
      7. Deletes itself.

    This function does not return — it calls ``sys.exit(0)`` after launching
    the script.
    """
    import subprocess

    exe = _current_exe()
    pid = os.getpid()
    log_path = Path(tempfile.gettempdir()) / UPDATE_LOG_FILENAME

    # Write the batch script to a temp file
    fd, bat_path = tempfile.mkstemp(suffix=".bat", prefix="hcpcs_update_")
    os.close(fd)

    bat_content = (
        "@echo off\r\n"
        "setlocal enabledelayedexpansion\r\n"
        f"set \"LOG_PATH={log_path}\"\r\n"
        # ---- step 1: helper started ----
        "echo [%date% %time%] Helper started. > \"%LOG_PATH%\"\r\n"
        f"echo [%date% %time%] Waiting for process {pid} to exit... >> \"%LOG_PATH%\"\r\n"
        # ---- step 2: wait for the original process to exit ----
        "set /a _wait_tries=0\r\n"
        ":wait_pid\r\n"
        "set /a _wait_tries=_wait_tries+1\r\n"
        "if !_wait_tries! gtr 30 (\r\n"
        f"    echo [%date% %time%] WARNING: PID {pid} may still be running after 30 s. Continuing anyway. >> \"%LOG_PATH%\"\r\n"
        "    goto pid_gone\r\n"
        ")\r\n"
        f"tasklist /FI \"PID eq {pid}\" /FO CSV /NH 2>NUL | findstr /B \"\\\"{pid}\\\"\" >NUL 2>&1\r\n"
        "if not errorlevel 1 (\r\n"
        "    timeout /t 1 /nobreak >NUL\r\n"
        "    goto wait_pid\r\n"
        ")\r\n"
        ":pid_gone\r\n"
        f"echo [%date% %time%] Process {pid} no longer detected. >> \"%LOG_PATH%\"\r\n"
        # ---- step 3: brief settle pause so Windows releases file handles ----
        "echo [%date% %time%] Waiting 2 s for file handles to release... >> \"%LOG_PATH%\"\r\n"
        "timeout /t 2 /nobreak >NUL\r\n"
        # ---- step 4: delete old exe (up to 10 retries × 1 s) ----
        f"echo [%date% %time%] Attempting to remove old exe: {exe} >> \"%LOG_PATH%\"\r\n"
        "set /a _del_tries=0\r\n"
        ":del_retry\r\n"
        f"if not exist \"{exe}\" goto rename_step\r\n"
        "set /a _del_tries=_del_tries+1\r\n"
        f"del /F /Q \"{exe}\" >NUL 2>&1\r\n"
        f"if not exist \"{exe}\" goto rename_step\r\n"
        "echo [%date% %time%] Delete attempt !_del_tries! failed. >> \"%LOG_PATH%\"\r\n"
        "if !_del_tries! gtr 10 goto swap_failed\r\n"
        "timeout /t 1 /nobreak >NUL\r\n"
        "goto del_retry\r\n"
        # ---- step 5: rename new exe into place (up to 5 retries × 2 s) ----
        ":rename_step\r\n"
        f"echo [%date% %time%] Renaming new exe into place: {new_exe} -> {exe} >> \"%LOG_PATH%\"\r\n"
        "set /a _ren_tries=0\r\n"
        ":ren_retry\r\n"
        "set /a _ren_tries=_ren_tries+1\r\n"
        f"move /Y \"{new_exe}\" \"{exe}\" >NUL 2>&1\r\n"
        "if not errorlevel 1 goto verify_step\r\n"
        "echo [%date% %time%] Rename attempt !_ren_tries! failed. >> \"%LOG_PATH%\"\r\n"
        "if !_ren_tries! gtr 5 goto swap_failed\r\n"
        "timeout /t 2 /nobreak >NUL\r\n"
        "goto ren_retry\r\n"
        # ---- step 6: verify replacement exists ----
        ":verify_step\r\n"
        f"if not exist \"{exe}\" goto swap_failed\r\n"
        f"echo [%date% %time%] Replacement verified: {exe} >> \"%LOG_PATH%\"\r\n"
        # ---- step 7: relaunch ----
        "echo [%date% %time%] Relaunching application... >> \"%LOG_PATH%\"\r\n"
        f"start \"\" \"{exe}\"\r\n"
        "echo [%date% %time%] Relaunch command issued successfully. >> \"%LOG_PATH%\"\r\n"
        "goto end\r\n"
        # ---- failure path ----
        ":swap_failed\r\n"
        "echo [%date% %time%] ERROR: Update failed — could not replace the application file. >> \"%LOG_PATH%\"\r\n"
        "echo. >> \"%LOG_PATH%\"\r\n"
        "echo MANUAL RECOVERY INSTRUCTIONS: >> \"%LOG_PATH%\"\r\n"
        f"echo   1. Close any running instance of HCPCSFeeApp.exe. >> \"%LOG_PATH%\"\r\n"
        f"echo   2. In File Explorer, navigate to: {exe.parent} >> \"%LOG_PATH%\"\r\n"
        f"echo   3. Delete (or rename) HCPCSFeeApp.exe if it still exists. >> \"%LOG_PATH%\"\r\n"
        f"echo   4. Rename HCPCSFeeApp_new.exe to HCPCSFeeApp.exe. >> \"%LOG_PATH%\"\r\n"
        "echo   5. Launch HCPCSFeeApp.exe normally. >> \"%LOG_PATH%\"\r\n"
        "echo. >> \"%LOG_PATH%\"\r\n"
        "echo This log is saved to: %LOG_PATH% >> \"%LOG_PATH%\"\r\n"
        ":end\r\n"
        "echo [%date% %time%] Helper finished. >> \"%LOG_PATH%\"\r\n"
        "del \"%~f0\"\r\n"
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
