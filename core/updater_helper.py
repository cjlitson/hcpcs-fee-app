"""Standalone updater helper logic for packaged Windows self-update."""

from __future__ import annotations

import argparse
import ctypes
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

DOWNLOAD_PROGRESS_LOG_STEP_BYTES = 10 * 1024 * 1024
ASSET_DOWNLOAD_TIMEOUT_SECONDS = 120
MB_ICONERROR = 0x10
MB_ICONINFORMATION = 0x40
UPDATER_PROGRESS_WINDOW_GEOMETRY = "560x180"
UPDATER_PROGRESS_BAR_LENGTH = 520
UPDATER_PROGRESS_ANIMATION_INTERVAL_MS = 12


def _log_message(log_path: Path, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(f"[{timestamp}] {message}\n")


def _is_process_running(pid: int) -> bool:
    if pid <= 0:
        return False

    if os.name != "nt":
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    kernel32 = ctypes.windll.kernel32
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if handle:
        kernel32.CloseHandle(handle)
        return True

    # Access denied can mean process exists but we lack rights.
    return ctypes.get_last_error() == 5


def wait_for_process_exit(
    pid: int,
    timeout_seconds: float = 30.0,
    poll_interval: float = 0.5,
    process_running=_is_process_running,
    progress_callback=None,
) -> bool:
    started = time.monotonic()
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not process_running(pid):
            return True
        if progress_callback is not None:
            try:
                progress_callback(time.monotonic() - started, timeout_seconds)
            except Exception:
                pass
        time.sleep(poll_interval)
    return not process_running(pid)


def remove_file_with_retries(
    target_path: Path,
    retries: int = 10,
    delay_seconds: float = 1.0,
) -> bool:
    for attempt in range(1, retries + 1):
        if not target_path.exists():
            return True
        try:
            target_path.unlink()
            return True
        except OSError:
            if attempt == retries:
                return False
            time.sleep(delay_seconds)
    return False


def replace_file_with_retries(
    new_path: Path,
    target_path: Path,
    retries: int = 5,
    delay_seconds: float = 2.0,
) -> bool:
    for attempt in range(1, retries + 1):
        try:
            os.replace(new_path, target_path)
            return True
        except OSError:
            if attempt == retries:
                return False
            time.sleep(delay_seconds)
    return False


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HCPCSFeeApp updater helper")
    parser.add_argument("--current-exe", required=True)
    payload_group = parser.add_mutually_exclusive_group(required=True)
    payload_group.add_argument("--new-exe")
    payload_group.add_argument("--asset-url")
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--log-path")
    parser.add_argument("--version")
    parser.add_argument("--release-url")
    return parser


def _show_message_box(title: str, message: str, *, error: bool = False) -> None:
    """Best-effort native message box for updater status/errors on Windows."""
    if os.name != "nt":
        return
    try:
        flags = MB_ICONERROR if error else MB_ICONINFORMATION
        ctypes.windll.user32.MessageBoxW(None, message, title, flags)
    except Exception:
        pass


def _build_manual_recovery_text(
    *,
    current_exe: Path,
    new_exe: Path | None,
    log_path: Path,
    release_url: str | None,
) -> str:
    lines = [
        f"Current exe: {current_exe}",
        f"Update log: {log_path}",
    ]
    if new_exe is not None:
        lines.append(f"Pending update file: {new_exe}")
    if release_url:
        lines.append(f"Release page: {release_url}")
    lines.extend(
        [
            "",
            "Manual recovery:",
            "1) Close HCPCS Fee App if it is still running.",
            "2) If HCPCSFeeApp_new.exe exists, rename it to HCPCSFeeApp.exe in the app folder.",
            "3) If needed, run Install.bat from the latest setup ZIP.",
        ]
    )
    return "\n".join(lines)


class _UpdaterProgressUI:
    """Best-effort native progress window for the standalone updater workflow."""

    def __init__(self, log_path: Path) -> None:
        self._log_path = log_path
        self._root = None
        self._phase_var = None
        self._detail_var = None
        self._progress = None
        self._indeterminate_running = False

        if os.name != "nt":
            return

        try:
            import tkinter as tk
            from tkinter import ttk
        except Exception as exc:
            _log_message(log_path, f"WARNING: Progress UI unavailable: {exc!r}")
            return

        try:
            self._root = tk.Tk()
            self._root.title("HCPCS Fee App Update")
            self._root.geometry(UPDATER_PROGRESS_WINDOW_GEOMETRY)
            self._root.resizable(False, False)
            self._root.attributes("-topmost", True)

            container = ttk.Frame(self._root, padding=16)
            container.pack(fill="both", expand=True)

            title = ttk.Label(
                container,
                text="Installing update…",
            )
            title.pack(anchor="w")

            self._phase_var = tk.StringVar(value="Initializing updater window…")
            phase_label = ttk.Label(
                container,
                textvariable=self._phase_var,
            )
            phase_label.pack(anchor="w", pady=(10, 2))

            self._detail_var = tk.StringVar(value="Preparing status updates…")
            detail_label = ttk.Label(
                container,
                textvariable=self._detail_var,
            )
            detail_label.pack(anchor="w", pady=(0, 10))

            self._progress = ttk.Progressbar(
                container,
                mode="indeterminate",
                length=UPDATER_PROGRESS_BAR_LENGTH,
            )
            self._progress.pack(fill="x")
            self._set_indeterminate()
            self._pump()
        except Exception as exc:
            _log_message(log_path, f"WARNING: Failed to initialize progress UI: {exc!r}")
            self.close()

    def _pump(self) -> None:
        if self._root is None:
            return
        try:
            self._root.update_idletasks()
            self._root.update()
        except Exception:
            self.close()

    def _set_indeterminate(self) -> None:
        if self._progress is None:
            return
        try:
            self._progress.configure(mode="indeterminate")
            if not self._indeterminate_running:
                self._progress.start(UPDATER_PROGRESS_ANIMATION_INTERVAL_MS)
                self._indeterminate_running = True
        except Exception:
            self.close()

    def set_phase(self, phase: str, detail: str) -> None:
        if self._phase_var is not None:
            self._phase_var.set(f"Step: {phase}")
        if self._detail_var is not None:
            self._detail_var.set(detail)
        self._set_indeterminate()
        self._pump()

    def set_download_progress(self, downloaded: int, total: int) -> None:
        if self._phase_var is not None:
            self._phase_var.set("Step: Downloading release asset")
        if self._progress is not None:
            try:
                if total > 0:
                    if self._indeterminate_running:
                        self._progress.stop()
                        self._indeterminate_running = False
                    self._progress.configure(mode="determinate", maximum=total)
                    self._progress["value"] = min(downloaded, total)
                else:
                    self._set_indeterminate()
            except Exception:
                self.close()
                return
        if self._detail_var is not None:
            if total > 0:
                pct = min(100.0, (downloaded / total) * 100)
                self._detail_var.set(
                    f"Downloaded {downloaded:,} of {total:,} bytes ({pct:.1f}%)…"
                )
            else:
                self._detail_var.set(f"Downloaded {downloaded:,} bytes…")
        self._pump()

    def close(self) -> None:
        if self._progress is not None and self._indeterminate_running:
            try:
                self._progress.stop()
            except Exception:
                pass
        self._indeterminate_running = False
        self._progress = None
        self._phase_var = None
        self._detail_var = None
        if self._root is not None:
            try:
                self._root.destroy()
            except Exception:
                pass
        self._root = None


def _download_release_asset(
    *,
    asset_url: str,
    current_exe: Path,
    log_path: Path,
    progress_callback=None,
) -> Path:
    import requests

    dest = current_exe.parent / "HCPCSFeeApp_new.exe"
    _log_message(log_path, f"Downloading update asset: {asset_url}")

    try:
        resp = requests.get(
            asset_url,
            stream=True,
            timeout=ASSET_DOWNLOAD_TIMEOUT_SECONDS,
            verify=True,
        )
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(
            "Unable to download update asset. Check your internet connection and "
            f"release URL: {asset_url}"
        ) from exc

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    next_log_bytes = DOWNLOAD_PROGRESS_LOG_STEP_BYTES

    with open(dest, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=65536):
            if not chunk:
                continue
            fh.write(chunk)
            downloaded += len(chunk)
            if progress_callback is not None:
                try:
                    progress_callback(downloaded, total)
                except Exception:
                    pass
            if downloaded >= next_log_bytes:
                if total > 0:
                    pct = (downloaded / total) * 100
                    _log_message(
                        log_path,
                        f"Download progress: {downloaded:,}/{total:,} bytes ({pct:.1f}%).",
                    )
                else:
                    _log_message(log_path, f"Download progress: {downloaded:,} bytes.")
                next_log_bytes += DOWNLOAD_PROGRESS_LOG_STEP_BYTES

    if total > 0 and downloaded != total:
        raise RuntimeError(
            f"Downloaded {downloaded:,} bytes, expected {total:,} bytes."
        )
    min_size = 1 * 1024 * 1024
    actual_size = dest.stat().st_size
    if actual_size < min_size:
        dest.unlink(missing_ok=True)
        raise RuntimeError(
            f"Downloaded file is only {actual_size:,} bytes — expected at least {min_size:,} bytes."
        )

    _log_message(log_path, f"Download completed: {dest} ({actual_size:,} bytes)")
    return dest


def run(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    current_exe = Path(args.current_exe)
    log_path = (
        Path(args.log_path)
        if args.log_path
        else Path(tempfile.gettempdir()) / "HCPCSFeeApp_update.log"
    )
    new_exe = Path(args.new_exe) if args.new_exe else None

    backup_exe: Path | None = None
    progress_ui = _UpdaterProgressUI(log_path)

    try:
        _log_message(log_path, "Updater workflow started.")
        _log_message(log_path, f"Current exe: {current_exe}")
        if args.version:
            _log_message(log_path, f"Target version: {args.version}")
        if args.asset_url:
            _log_message(log_path, f"Asset URL: {args.asset_url}")
        if new_exe is not None:
            _log_message(log_path, f"New exe: {new_exe}")
        _log_message(log_path, f"Waiting for PID {args.pid} to exit.")
        progress_ui.set_phase(
            "Waiting for app to close",
            "Waiting for the running app to exit…",
        )

        def _on_wait_progress(elapsed: float, _timeout: float) -> None:
            progress_ui.set_phase(
                "Waiting for app to close",
                f"Waiting for the running app to exit… ({elapsed:.0f}s elapsed)",
            )

        if not wait_for_process_exit(args.pid, progress_callback=_on_wait_progress):
            _log_message(
                log_path,
                f"WARNING: PID {args.pid} still appears to be running after timeout.",
            )
        else:
            _log_message(log_path, f"PID {args.pid} exited.")

        _log_message(log_path, "Waiting 2 seconds for file handles to release.")
        progress_ui.set_phase(
            "Waiting for app to close",
            "App exited. Waiting for file handles to release…",
        )
        time.sleep(2.0)

        if args.asset_url:
            try:
                progress_ui.set_phase(
                    "Downloading release asset",
                    "Downloading update package…",
                )
                new_exe = _download_release_asset(
                    asset_url=args.asset_url,
                    current_exe=current_exe,
                    log_path=log_path,
                    progress_callback=progress_ui.set_download_progress,
                )
            except Exception as exc:
                _log_message(log_path, f"ERROR: Failed to download update asset: {exc!r}")
                _show_message_box(
                    "HCPCS Fee App Update Failed",
                    "The updater could not download the new version.\n\n"
                    + _build_manual_recovery_text(
                        current_exe=current_exe,
                        new_exe=None,
                        log_path=log_path,
                        release_url=args.release_url,
                    ),
                    error=True,
                )
                return 12

        if not new_exe.exists():
            _log_message(log_path, f"ERROR: Downloaded update file does not exist: {new_exe}")
            _show_message_box(
                "HCPCS Fee App Update Failed",
                "The updater could not find the downloaded update file.\n\n"
                + _build_manual_recovery_text(
                    current_exe=current_exe,
                    new_exe=new_exe,
                    log_path=log_path,
                    release_url=args.release_url,
                ),
                error=True,
            )
            return 3

        progress_ui.set_phase(
            "Applying update",
            "Replacing application executable…",
        )
        _log_message(log_path, f"Replacing executable in-place: {new_exe} -> {current_exe}")
        replaced = replace_file_with_retries(new_exe, current_exe)
        if not replaced:
            _log_message(log_path, "WARNING: In-place replace failed; attempting remove+replace fallback.")

            if not new_exe.exists():
                _log_message(
                    log_path,
                    "ERROR: Update file missing before fallback; refusing to remove current executable.",
                )
                return 4

            backup_exe = current_exe.with_name(f"{current_exe.name}.backup")
            if backup_exe.exists():
                _log_message(log_path, f"Removing stale backup executable: {backup_exe}")
                if not remove_file_with_retries(backup_exe):
                    _log_message(log_path, "ERROR: Unable to remove stale backup executable.")
                    return 11

            _log_message(log_path, f"Backing up existing executable: {current_exe} -> {backup_exe}")
            if not replace_file_with_retries(current_exe, backup_exe):
                _log_message(log_path, "ERROR: Unable to back up old executable before fallback.")
                return 5

            _log_message(log_path, f"Retrying replacement: {new_exe} -> {current_exe}")
            if not replace_file_with_retries(new_exe, current_exe):
                _log_message(log_path, "ERROR: Unable to replace executable after fallback.")
                if backup_exe.exists():
                    _log_message(log_path, f"Restoring backup executable: {backup_exe} -> {current_exe}")
                    if replace_file_with_retries(backup_exe, current_exe):
                        _log_message(log_path, "Backup executable restored after fallback failure.")
                    else:
                        _log_message(log_path, "ERROR: Unable to restore backup executable.")
                _show_message_box(
                    "HCPCS Fee App Update Failed",
                    "The updater could not replace the application executable.\n\n"
                    + _build_manual_recovery_text(
                        current_exe=current_exe,
                        new_exe=new_exe,
                        log_path=log_path,
                        release_url=args.release_url,
                    ),
                    error=True,
                )
                return 6

        if not current_exe.exists():
            if backup_exe is not None and backup_exe.exists():
                _log_message(log_path, f"Restoring backup after failed verification: {backup_exe} -> {current_exe}")
                if replace_file_with_retries(backup_exe, current_exe):
                    _log_message(log_path, "Backup executable restored after failed verification.")
                else:
                    _log_message(log_path, "ERROR: Unable to restore backup after failed verification.")
            if not current_exe.exists():
                _log_message(log_path, "ERROR: Replacement verification failed.")
                _show_message_box(
                    "HCPCS Fee App Update Failed",
                    "Replacement verification failed.\n\n"
                    + _build_manual_recovery_text(
                        current_exe=current_exe,
                        new_exe=new_exe,
                        log_path=log_path,
                        release_url=args.release_url,
                    ),
                    error=True,
                )
                return 7

        if backup_exe is not None and backup_exe.exists():
            _log_message(log_path, f"Removing backup executable: {backup_exe}")
            if not remove_file_with_retries(backup_exe):
                _log_message(log_path, "WARNING: Unable to remove backup executable.")

        progress_ui.set_phase(
            "Relaunching app",
            "Launching the updated HCPCS Fee App…",
        )
        _log_message(log_path, "Replacement verified. Relaunching application.")
        try:
            subprocess.Popen([str(current_exe)], close_fds=True)
        except Exception as exc:
            _log_message(log_path, f"ERROR: Failed to relaunch application: {exc!r}")
            _show_message_box(
                "HCPCS Fee App Update Failed",
                "The app was updated but could not be relaunched automatically.\n\n"
                + _build_manual_recovery_text(
                    current_exe=current_exe,
                    new_exe=new_exe,
                    log_path=log_path,
                    release_url=args.release_url,
                ),
                error=True,
            )
            return 8
        _log_message(log_path, "Relaunch command issued successfully.")
        _log_message(log_path, "Updater workflow finished.")
        return 0
    except Exception as exc:
        _log_message(log_path, f"ERROR: Unexpected helper failure: {exc!r}")
        _show_message_box(
            "HCPCS Fee App Update Failed",
            "Unexpected updater failure.\n\n"
            + _build_manual_recovery_text(
                current_exe=current_exe,
                new_exe=new_exe,
                log_path=log_path,
                release_url=args.release_url,
            ),
            error=True,
        )
        return 10
    finally:
        progress_ui.close()


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
