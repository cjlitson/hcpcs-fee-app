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
LAUNCH_SUCCESS_MESSAGE = "Updater helper launched successfully."


def _launch_success_marker(new_exe: Path) -> str:
    """Return a per-attempt success token that embeds the pending EXE path.

    Embedding the path makes the success marker specific to one update attempt
    so that a stale entry in a long-running launcher log cannot suppress the
    incomplete-update warning for a different (later) pending file.
    """
    return f"{LAUNCH_SUCCESS_MESSAGE} new_exe={new_exe}"


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


def get_launcher_log_paths(exe: Path | None = None) -> list[Path]:
    """Return launcher log paths, preferring the installed app folder."""
    if exe is None and getattr(sys, "frozen", False):
        try:
            exe = _current_exe()
        except Exception:
            exe = None

    paths: list[Path] = []
    if exe is not None:
        paths.append(exe.parent / LAUNCHER_LOG_FILENAME)
    paths.append(Path(tempfile.gettempdir()) / LAUNCHER_LOG_FILENAME)

    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def write_launcher_log(message: str, *, exe: Path | None = None) -> None:
    """Best-effort launcher logging for packaged update handoff diagnosis."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}\n"
    for log_path in get_launcher_log_paths(exe):
        try:
            with log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)
        except Exception:
            pass


def helper_launch_recorded_successfully(pending_exe: Path) -> bool:
    """Return True when launcher logs show a successful helper launch for *pending_exe*."""
    if not pending_exe.exists():
        return False

    pending_mtime = pending_exe.stat().st_mtime
    expected_marker = _launch_success_marker(pending_exe)
    for log_path in get_launcher_log_paths():
        try:
            if not log_path.exists():
                continue
            if log_path.stat().st_mtime < pending_mtime:
                continue
            content = log_path.read_text(encoding="utf-8")
            if expected_marker in content:
                return True
        except Exception:
            continue
    return False


def apply_update(new_exe: Path) -> None:
    """Launch the updater helper executable and exit the current process.

    This function does not return on success — it calls ``sys.exit(0)`` after
    starting the helper process.
    """
    import subprocess

    exe = _current_exe()
    pid = os.getpid()
    log_path = Path(tempfile.gettempdir()) / UPDATE_LOG_FILENAME
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

    write_launcher_log("Launch attempt starting.", exe=exe)
    write_launcher_log(f"Current exe: {exe}", exe=exe)
    write_launcher_log(f"Downloaded exe: {new_exe}", exe=exe)
    write_launcher_log(f"Updater helper path: {helper_exe}", exe=exe)
    write_launcher_log(f"Updater log path: {log_path}", exe=exe)
    write_launcher_log(
        "Launch strategy: subprocess.Popen(args, cwd=<app_dir>, close_fds=False)",
        exe=exe,
    )

    if not helper_exe.exists():
        write_launcher_log("ERROR: Updater helper executable not found.", exe=exe)
        raise RuntimeError(f"Updater helper not found: {helper_exe}")
    if not new_exe.exists():
        write_launcher_log("ERROR: Downloaded update executable not found.", exe=exe)
        raise RuntimeError(f"Downloaded update not found: {new_exe}")

    try:
        subprocess.Popen(
            helper_args,
            cwd=str(exe.parent),
            close_fds=False,
        )
        write_launcher_log(_launch_success_marker(new_exe), exe=exe)
    except Exception as exc:
        write_launcher_log(f"ERROR: Failed to launch updater helper: {exc!r}", exe=exe)
        raise RuntimeError(
            f"Failed to launch updater helper ({type(exc).__name__}): {exc}"
        ) from exc

    try:
        from PyQt6.QtWidgets import QApplication

        app = QApplication.instance()
        if app is not None:
            app.quit()
    except Exception as exc:
        write_launcher_log(f"WARNING: QApplication quit failed: {exc!r}", exe=exe)

    sys.exit(0)


def launch_updater_workflow(
    asset_url: str,
    *,
    version: str | None = None,
    release_url: str | None = None,
) -> None:
    """Launch the updater executable as the primary workflow owner and exit.

    The updater process performs download/apply/relaunch after handoff.
    """
    import subprocess

    exe = _current_exe()
    pid = os.getpid()
    log_path = Path(tempfile.gettempdir()) / UPDATE_LOG_FILENAME
    helper_exe = exe.parent / UPDATER_HELPER_EXE_NAME

    helper_args = [
        str(helper_exe),
        "--current-exe",
        str(exe),
        "--pid",
        str(pid),
        "--log-path",
        str(log_path),
        "--asset-url",
        asset_url,
    ]
    if version:
        helper_args.extend(["--version", version])
    if release_url:
        helper_args.extend(["--release-url", release_url])

    write_launcher_log("[UI] Updater handoff requested.", exe=exe)
    write_launcher_log(f"[UI] Current exe: {exe}", exe=exe)
    write_launcher_log(f"[UI] Updater helper path: {helper_exe}", exe=exe)
    write_launcher_log(f"[UI] Updater log path: {log_path}", exe=exe)
    write_launcher_log(f"[UI] Updater asset URL: {asset_url}", exe=exe)
    if release_url:
        write_launcher_log(f"[UI] Updater release URL: {release_url}", exe=exe)
    if version:
        write_launcher_log(f"[UI] Updater target version: {version}", exe=exe)

    if not helper_exe.exists():
        write_launcher_log("ERROR: Updater helper executable not found.", exe=exe)
        raise RuntimeError(f"Updater helper not found: {helper_exe}")
    if not asset_url.strip():
        write_launcher_log("ERROR: Empty updater asset URL.", exe=exe)
        raise RuntimeError("Updater asset URL is empty.")

    try:
        subprocess.Popen(
            helper_args,
            cwd=str(exe.parent),
            close_fds=False,
        )
        write_launcher_log("[UI] Updater workflow launched successfully.", exe=exe)
    except Exception as exc:
        write_launcher_log(f"ERROR: Failed to launch updater workflow: {exc!r}", exe=exe)
        raise RuntimeError(
            f"Failed to launch updater workflow ({type(exc).__name__}): {exc}"
        ) from exc

    try:
        from PyQt6.QtWidgets import QApplication

        app = QApplication.instance()
        if app is not None:
            app.quit()
    except Exception as exc:
        write_launcher_log(f"WARNING: QApplication quit failed: {exc!r}", exe=exe)

    sys.exit(0)
