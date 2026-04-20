import os

from core import updater_helper


def test_wait_for_process_exit_times_out_with_running_process():
    assert (
        updater_helper.wait_for_process_exit(
            1234,
            timeout_seconds=0.01,
            poll_interval=0.001,
            process_running=lambda _pid: True,
        )
        is False
    )


def test_remove_file_with_retries_succeeds_after_retry(tmp_path, monkeypatch):
    target = tmp_path / "HCPCSFeeApp.exe"
    target.write_bytes(b"old")
    calls = {"count": 0}
    original_unlink = updater_helper.Path.unlink

    def _flaky_unlink(self):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("locked")
        return original_unlink(self)

    monkeypatch.setattr(updater_helper.Path, "unlink", _flaky_unlink)
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)

    assert updater_helper.remove_file_with_retries(target, retries=2, delay_seconds=0.0)
    assert calls["count"] == 2


def test_replace_file_with_retries_succeeds_after_retry(tmp_path, monkeypatch):
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    new_exe.write_bytes(b"new")

    calls = {"count": 0}
    original_replace = updater_helper.os.replace

    def _flaky_replace(src, dst):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("busy")
        return original_replace(src, dst)

    monkeypatch.setattr(updater_helper.os, "replace", _flaky_replace)
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)

    assert updater_helper.replace_file_with_retries(new_exe, current_exe, retries=2, delay_seconds=0.0)
    assert calls["count"] == 2
    assert current_exe.exists()
    assert current_exe.read_bytes() == b"new"


def test_run_invokes_wait_replace_and_relaunch(tmp_path, monkeypatch):
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    log_path = tmp_path / "HCPCSFeeApp_update.log"
    current_exe.write_bytes(b"old")
    new_exe.write_bytes(b"new")

    calls = {}
    elapsed_seconds = 0.5
    timeout_seconds = 30.0

    def _wait(pid, progress_callback=None):
        calls["wait_pid"] = pid
        if progress_callback is not None:
            progress_callback(elapsed_seconds, timeout_seconds)
        return True

    def _remove(path):
        calls["remove_path"] = path
        if path.exists():
            path.unlink()
        return True

    def _replace(src, dst):
        calls["replace_paths"] = (src, dst)
        os.replace(src, dst)
        return True

    class _DummyPopen:
        def __init__(self, args, close_fds):
            calls["popen_args"] = args
            calls["close_fds"] = close_fds

    monkeypatch.setattr(updater_helper, "wait_for_process_exit", _wait)
    monkeypatch.setattr(updater_helper, "remove_file_with_retries", _remove)
    monkeypatch.setattr(updater_helper, "replace_file_with_retries", _replace)
    monkeypatch.setattr(updater_helper.subprocess, "Popen", _DummyPopen)
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)

    code = updater_helper.run(
        [
            "--current-exe",
            str(current_exe),
            "--new-exe",
            str(new_exe),
            "--pid",
            "4321",
            "--log-path",
            str(log_path),
        ]
    )

    assert code == 0
    assert calls["wait_pid"] == 4321
    assert "remove_path" not in calls
    assert calls["replace_paths"] == (new_exe, current_exe)
    assert calls["popen_args"] == [str(current_exe)]
    assert calls["close_fds"] is True
    assert "Updater workflow started." in log_path.read_text(encoding="utf-8")


def test_run_falls_back_to_backup_then_replace_when_in_place_replace_fails(tmp_path, monkeypatch):
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    log_path = tmp_path / "HCPCSFeeApp_update.log"
    current_exe.write_bytes(b"old")
    new_exe.write_bytes(b"new")

    backup_exe = tmp_path / "HCPCSFeeApp.exe.backup"
    calls = {"replace_count": 0, "remove_paths": []}

    monkeypatch.setattr(
        updater_helper, "wait_for_process_exit", lambda _pid, progress_callback=None: True
    )

    def _remove(path):
        calls["remove_paths"].append(path)
        if path.exists():
            path.unlink()
        return True

    def _replace(src, dst):
        calls["replace_count"] += 1
        if calls["replace_count"] == 1:
            return False
        os.replace(src, dst)
        calls.setdefault("replace_paths", []).append((src, dst))
        return True

    class _DummyPopen:
        def __init__(self, args, close_fds):
            calls["popen_args"] = args
            calls["close_fds"] = close_fds

    monkeypatch.setattr(updater_helper, "remove_file_with_retries", _remove)
    monkeypatch.setattr(updater_helper, "replace_file_with_retries", _replace)
    monkeypatch.setattr(updater_helper.subprocess, "Popen", _DummyPopen)
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)

    code = updater_helper.run(
        [
            "--current-exe",
            str(current_exe),
            "--new-exe",
            str(new_exe),
            "--pid",
            "4321",
            "--log-path",
            str(log_path),
        ]
    )

    assert code == 0
    assert calls["replace_count"] == 3
    assert calls["replace_paths"] == [
        (current_exe, backup_exe),
        (new_exe, current_exe),
    ]
    assert calls["remove_paths"] == [backup_exe]
    assert calls["popen_args"] == [str(current_exe)]
    assert calls["close_fds"] is True


def test_run_does_not_remove_current_exe_if_update_file_disappears_before_fallback(
    tmp_path, monkeypatch
):
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    log_path = tmp_path / "HCPCSFeeApp_update.log"
    current_exe.write_bytes(b"old")
    new_exe.write_bytes(b"new")

    calls = {"replace_count": 0}

    monkeypatch.setattr(
        updater_helper, "wait_for_process_exit", lambda _pid, progress_callback=None: True
    )
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)

    def _remove(_path):
        calls["remove_called"] = True
        return True

    def _replace(src, _dst):
        calls["replace_count"] += 1
        if calls["replace_count"] == 1:
            if src.exists():
                src.unlink()
            return False
        return False

    monkeypatch.setattr(updater_helper, "remove_file_with_retries", _remove)
    monkeypatch.setattr(updater_helper, "replace_file_with_retries", _replace)

    code = updater_helper.run(
        [
            "--current-exe",
            str(current_exe),
            "--new-exe",
            str(new_exe),
            "--pid",
            "4321",
            "--log-path",
            str(log_path),
        ]
    )

    assert code == 4
    assert calls["replace_count"] == 1
    assert "remove_called" not in calls
    assert current_exe.exists()
    assert current_exe.read_bytes() == b"old"


def test_run_restores_backup_if_fallback_replace_fails(tmp_path, monkeypatch):
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    log_path = tmp_path / "HCPCSFeeApp_update.log"
    backup_exe = tmp_path / "HCPCSFeeApp.exe.backup"
    current_exe.write_bytes(b"old")
    new_exe.write_bytes(b"new")

    calls = {"replace_count": 0, "replace_attempts": []}

    monkeypatch.setattr(
        updater_helper, "wait_for_process_exit", lambda _pid, progress_callback=None: True
    )
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)
    monkeypatch.setattr(updater_helper, "remove_file_with_retries", lambda _path: True)

    def _replace(src, dst):
        calls["replace_count"] += 1
        calls["replace_attempts"].append((src, dst))
        if calls["replace_count"] in (1, 3):
            return False
        os.replace(src, dst)
        calls.setdefault("replace_paths", []).append((src, dst))
        return True

    monkeypatch.setattr(updater_helper, "replace_file_with_retries", _replace)

    code = updater_helper.run(
        [
            "--current-exe",
            str(current_exe),
            "--new-exe",
            str(new_exe),
            "--pid",
            "4321",
            "--log-path",
            str(log_path),
        ]
    )

    assert code == 6
    assert calls["replace_count"] == 4
    assert calls["replace_attempts"] == [
        (new_exe, current_exe),
        (current_exe, backup_exe),
        (new_exe, current_exe),
        (backup_exe, current_exe),
    ]
    assert calls["replace_paths"] == [
        (current_exe, backup_exe),
        (backup_exe, current_exe),
    ]
    assert current_exe.exists()
    assert current_exe.read_bytes() == b"old"


def test_run_downloads_asset_then_replaces_and_relaunches(tmp_path, monkeypatch):
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    log_path = tmp_path / "HCPCSFeeApp_update.log"
    current_exe.write_bytes(b"old")
    downloaded_exe = tmp_path / "HCPCSFeeApp_new.exe"

    calls = {}

    monkeypatch.setattr(
        updater_helper, "wait_for_process_exit", lambda _pid, progress_callback=None: True
    )
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)

    def _download(*, asset_url, current_exe, log_path, progress_callback=None):
        calls["download"] = (asset_url, current_exe, log_path)
        downloaded_exe.write_bytes(b"new")
        if progress_callback is not None:
            progress_callback(100, 200)
        return downloaded_exe

    def _replace(src, dst):
        calls["replace_paths"] = (src, dst)
        os.replace(src, dst)
        return True

    class _DummyPopen:
        def __init__(self, args, close_fds):
            calls["popen_args"] = args
            calls["close_fds"] = close_fds

    monkeypatch.setattr(updater_helper, "_download_release_asset", _download)
    monkeypatch.setattr(updater_helper, "replace_file_with_retries", _replace)
    monkeypatch.setattr(updater_helper.subprocess, "Popen", _DummyPopen)

    code = updater_helper.run(
        [
            "--current-exe",
            str(current_exe),
            "--asset-url",
            "https://example.invalid/HCPCSFeeApp.exe",
            "--pid",
            "4321",
            "--log-path",
            str(log_path),
            "--version",
            "1.2.3",
            "--release-url",
            "https://github.com/cjlitson/hcpcs-fee-app/releases/tag/v1.2.3",
        ]
    )

    assert code == 0
    assert calls["download"][0] == "https://example.invalid/HCPCSFeeApp.exe"
    assert calls["download"][1] == current_exe
    assert calls["download"][2] == log_path
    assert calls["replace_paths"] == (downloaded_exe, current_exe)
    assert calls["popen_args"] == [str(current_exe)]
    assert calls["close_fds"] is True


def test_wait_for_process_exit_reports_progress(tmp_path):
    calls = []

    def _running(_pid):
        return len(calls) < 2

    assert updater_helper.wait_for_process_exit(
        1234,
        timeout_seconds=1.0,
        poll_interval=0.0,
        process_running=_running,
        progress_callback=lambda elapsed_seconds, timeout_seconds: calls.append(
            (elapsed_seconds, timeout_seconds)
        ),
    )
    assert len(calls) == 2
    assert all(timeout == 1.0 for _elapsed, timeout in calls)


def test_run_updates_progress_ui_phases(tmp_path, monkeypatch):
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    log_path = tmp_path / "HCPCSFeeApp_update.log"
    current_exe.write_bytes(b"old")
    downloaded_exe = tmp_path / "HCPCSFeeApp_new.exe"

    events = []
    elapsed_seconds = 1.2
    timeout_seconds = 30.0

    class _DummyProgressUI:
        def __init__(self, _log_path):
            events.append(("init",))

        def set_phase(self, phase, detail):
            events.append(("phase", phase, detail))

        def set_download_progress(self, downloaded, total):
            events.append(("download", downloaded, total))

        def close(self):
            events.append(("close",))

    monkeypatch.setattr(updater_helper, "_UpdaterProgressUI", _DummyProgressUI)
    monkeypatch.setattr(updater_helper.time, "sleep", lambda _s: None)
    monkeypatch.setattr(
        updater_helper,
        "wait_for_process_exit",
        lambda _pid, progress_callback=None: (
            progress_callback(elapsed_seconds, timeout_seconds)
            if progress_callback
            else None
        )
        or True,
    )

    def _download(*, asset_url, current_exe, log_path, progress_callback=None):
        downloaded_exe.write_bytes(b"new")
        if progress_callback is not None:
            progress_callback(50, 100)
        return downloaded_exe

    monkeypatch.setattr(updater_helper, "_download_release_asset", _download)
    monkeypatch.setattr(updater_helper, "replace_file_with_retries", lambda src, dst: os.replace(src, dst) or True)
    monkeypatch.setattr(
        updater_helper.subprocess,
        "Popen",
        lambda args, close_fds: events.append(("popen", args, close_fds)),
    )

    code = updater_helper.run(
        [
            "--current-exe",
            str(current_exe),
            "--asset-url",
            "https://example.invalid/HCPCSFeeApp.exe",
            "--pid",
            "4321",
            "--log-path",
            str(log_path),
        ]
    )

    assert code == 0
    phase_names = [entry[1] for entry in events if entry[0] == "phase"]
    assert "Waiting for app to close" in phase_names
    assert "Downloading release asset" in phase_names
    assert "Applying update" in phase_names
    assert "Relaunching app" in phase_names
    assert ("download", 50, 100) in events
    assert events[-1] == ("close",)
