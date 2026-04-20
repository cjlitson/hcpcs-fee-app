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
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not process_running(pid):
            return True
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
    parser.add_argument("--new-exe", required=True)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--log-path")
    return parser


def run(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    current_exe = Path(args.current_exe)
    new_exe = Path(args.new_exe)
    log_path = (
        Path(args.log_path)
        if args.log_path
        else Path(tempfile.gettempdir()) / "HCPCSFeeApp_update.log"
    )

    backup_exe: Path | None = None

    try:
        _log_message(log_path, "Helper started.")
        _log_message(log_path, f"Current exe: {current_exe}")
        _log_message(log_path, f"New exe: {new_exe}")
        _log_message(log_path, f"Waiting for PID {args.pid} to exit.")

        if not wait_for_process_exit(args.pid):
            _log_message(
                log_path,
                f"WARNING: PID {args.pid} still appears to be running after timeout.",
            )
        else:
            _log_message(log_path, f"PID {args.pid} exited.")

        _log_message(log_path, "Waiting 2 seconds for file handles to release.")
        time.sleep(2.0)

        if not new_exe.exists():
            _log_message(log_path, f"ERROR: Downloaded update file does not exist: {new_exe}")
            return 3

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
                    return 5

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
                return 6

        if not current_exe.exists():
            if backup_exe is not None and backup_exe.exists():
                _log_message(log_path, f"Restoring backup after failed verification: {backup_exe} -> {current_exe}")
                if replace_file_with_retries(backup_exe, current_exe):
                    _log_message(log_path, "Backup executable restored after failed verification.")
            if not current_exe.exists():
                _log_message(log_path, "ERROR: Replacement verification failed.")
                return 7

        if backup_exe is not None and backup_exe.exists():
            _log_message(log_path, f"Removing backup executable: {backup_exe}")
            if not remove_file_with_retries(backup_exe):
                _log_message(log_path, "WARNING: Unable to remove backup executable.")

        _log_message(log_path, "Replacement verified. Relaunching application.")
        try:
            subprocess.Popen([str(current_exe)], close_fds=True)
        except Exception as exc:
            _log_message(log_path, f"ERROR: Failed to relaunch application: {exc!r}")
            return 8
        _log_message(log_path, "Relaunch command issued successfully.")
        _log_message(log_path, "Helper finished.")
        return 0
    except Exception as exc:
        _log_message(log_path, f"ERROR: Unexpected helper failure: {exc!r}")
        return 10


def main() -> None:
    sys.exit(run())


if __name__ == "__main__":
    main()
