import pytest


def _raise_system_exit(code):
    raise SystemExit(code)


def _setup_apply_update(tmp_path, monkeypatch):
    """Shared helper: patch sys/os/subprocess so apply_update() can run in tests."""
    from core import self_updater

    exe_path = tmp_path / "HCPCSFeeApp.exe"
    exe_path.write_bytes(b"old")
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    new_exe.write_bytes(b"new")
    helper_exe = tmp_path / self_updater.UPDATER_HELPER_EXE_NAME
    helper_exe.write_bytes(b"helper")

    monkeypatch.setattr(self_updater.sys, "frozen", True, raising=False)
    monkeypatch.setattr(self_updater.sys, "executable", str(exe_path), raising=False)
    temp_log_dir = tmp_path / "temp_logs"
    temp_log_dir.mkdir()
    monkeypatch.setattr(self_updater.tempfile, "gettempdir", lambda: str(temp_log_dir))
    monkeypatch.setattr(self_updater.sys, "exit", _raise_system_exit)

    popen_calls = {}

    class _DummyPopen:
        def __init__(self, args, **kwargs):
            popen_calls["args"] = args
            popen_calls["kwargs"] = kwargs

    monkeypatch.setattr("subprocess.Popen", _DummyPopen)

    return self_updater, exe_path, new_exe, helper_exe, popen_calls, temp_log_dir


def test_apply_update_launches_updater_helper_with_expected_args(tmp_path, monkeypatch):
    self_updater, exe_path, new_exe, helper_exe, popen_calls, temp_log_dir = _setup_apply_update(
        tmp_path, monkeypatch
    )

    try:
        self_updater.apply_update(new_exe)
        raise AssertionError("apply_update should exit the process")
    except SystemExit as exc:
        assert exc.code == 0

    pid = self_updater.os.getpid()
    update_log_path = temp_log_dir / self_updater.UPDATE_LOG_FILENAME
    launcher_log_path = exe_path.parent / self_updater.LAUNCHER_LOG_FILENAME
    temp_launcher_log = temp_log_dir / self_updater.LAUNCHER_LOG_FILENAME

    assert popen_calls["args"] == [
        str(helper_exe),
        "--current-exe",
        str(exe_path),
        "--new-exe",
        str(new_exe),
        "--pid",
        str(pid),
        "--log-path",
        str(update_log_path),
    ]
    assert popen_calls["kwargs"]["cwd"] == str(exe_path.parent)
    assert popen_calls["kwargs"]["close_fds"] is False

    log_content = launcher_log_path.read_text(encoding="utf-8")
    assert "Launch attempt starting." in log_content
    assert f"Updater helper path: {helper_exe}" in log_content
    assert f"Updater log path: {update_log_path}" in log_content
    expected_marker = self_updater._launch_success_marker(new_exe)
    assert expected_marker in log_content
    assert expected_marker in temp_launcher_log.read_text(encoding="utf-8")


def test_apply_update_uses_simple_launch_strategy(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, _helper, calls, _temp_log_dir = _setup_apply_update(
        tmp_path, monkeypatch
    )

    try:
        self_updater.apply_update(new_exe)
    except SystemExit:
        pass

    assert "creationflags" not in calls["kwargs"]
    assert calls["kwargs"]["close_fds"] is False


def test_apply_update_exits_with_code_zero(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, _helper, _calls, _temp_log_dir = _setup_apply_update(
        tmp_path, monkeypatch
    )

    with pytest.raises(SystemExit) as exc_info:
        self_updater.apply_update(new_exe)
    assert exc_info.value.code == 0


def test_apply_update_raises_if_helper_missing(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, helper_exe, _calls, _temp_log_dir = _setup_apply_update(
        tmp_path, monkeypatch
    )
    helper_exe.unlink()

    with pytest.raises(RuntimeError, match="Updater helper not found"):
        self_updater.apply_update(new_exe)


def test_helper_launch_recorded_successfully_uses_pending_file_mtime(tmp_path, monkeypatch):
    from core import self_updater

    exe_path = tmp_path / "HCPCSFeeApp.exe"
    exe_path.write_bytes(b"old")
    pending = tmp_path / "HCPCSFeeApp_new.exe"
    pending.write_bytes(b"new")
    launcher_log = tmp_path / self_updater.LAUNCHER_LOG_FILENAME

    monkeypatch.setattr(self_updater.sys, "frozen", True, raising=False)
    monkeypatch.setattr(self_updater.sys, "executable", str(exe_path), raising=False)
    monkeypatch.setattr(self_updater.tempfile, "gettempdir", lambda: str(tmp_path))

    launcher_log.write_text("older log\n", encoding="utf-8")
    launcher_log.touch()

    assert not self_updater.helper_launch_recorded_successfully(pending)

    self_updater.write_launcher_log(self_updater._launch_success_marker(pending))
    assert self_updater.helper_launch_recorded_successfully(pending)


def test_helper_launch_recorded_ignores_stale_success_for_different_exe(tmp_path, monkeypatch):
    """Stale success from a previous attempt must not suppress warning for a newer pending file."""
    from core import self_updater

    exe_path = tmp_path / "HCPCSFeeApp.exe"
    exe_path.write_bytes(b"old")
    monkeypatch.setattr(self_updater.sys, "frozen", True, raising=False)
    monkeypatch.setattr(self_updater.sys, "executable", str(exe_path), raising=False)
    monkeypatch.setattr(self_updater.tempfile, "gettempdir", lambda: str(tmp_path))

    # Simulate a previous successful attempt for exe_v1
    exe_v1 = tmp_path / "HCPCSFeeApp_new.exe"
    exe_v1.write_bytes(b"v1")
    self_updater.write_launcher_log(self_updater._launch_success_marker(exe_v1))

    # The previous helper ran, so exe_v1 no longer exists on disk
    exe_v1.unlink()

    # A later failed attempt downloads exe_v2 (new pending file)
    exe_v2 = tmp_path / "HCPCSFeeApp_new.exe"
    exe_v2.write_bytes(b"v2")

    # The launcher log still contains the stale v1 success marker but NOT a v2 marker
    # The mtime on exe_v2 is now > log mtime only if we touch the file after writing the log
    import time
    time.sleep(0.01)  # ensure mtime ordering is stable
    exe_v2.touch()

    assert not self_updater.helper_launch_recorded_successfully(exe_v2), (
        "Stale success marker for a previous exe must not suppress the warning for "
        "the current pending file."
    )
