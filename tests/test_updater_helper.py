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

    def _wait(pid):
        calls["wait_pid"] = pid
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
    assert "Helper started." in log_path.read_text(encoding="utf-8")


def test_run_falls_back_to_remove_then_replace_when_in_place_replace_fails(tmp_path, monkeypatch):
    current_exe = tmp_path / "HCPCSFeeApp.exe"
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    log_path = tmp_path / "HCPCSFeeApp_update.log"
    current_exe.write_bytes(b"old")
    new_exe.write_bytes(b"new")

    calls = {"replace_count": 0}

    monkeypatch.setattr(updater_helper, "wait_for_process_exit", lambda _pid: True)

    def _remove(path):
        calls["remove_path"] = path
        if path.exists():
            path.unlink()
        return True

    def _replace(src, dst):
        calls["replace_count"] += 1
        if calls["replace_count"] == 1:
            return False
        os.replace(src, dst)
        calls["replace_paths"] = (src, dst)
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
    assert calls["replace_count"] == 2
    assert calls["remove_path"] == current_exe
    assert calls["replace_paths"] == (new_exe, current_exe)
    assert calls["popen_args"] == [str(current_exe)]
    assert calls["close_fds"] is True
