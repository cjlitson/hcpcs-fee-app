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
    monkeypatch.setattr(self_updater.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(self_updater.sys, "exit", _raise_system_exit)

    popen_calls = {}

    class _DummyPopen:
        def __init__(self, args, creationflags, close_fds):
            popen_calls["args"] = args
            popen_calls["creationflags"] = creationflags
            popen_calls["close_fds"] = close_fds

    monkeypatch.setattr("subprocess.Popen", _DummyPopen)

    return self_updater, exe_path, new_exe, helper_exe, popen_calls


def test_apply_update_launches_updater_helper_with_expected_args(tmp_path, monkeypatch):
    self_updater, exe_path, new_exe, helper_exe, popen_calls = _setup_apply_update(
        tmp_path, monkeypatch
    )

    try:
        self_updater.apply_update(new_exe)
        raise AssertionError("apply_update should exit the process")
    except SystemExit as exc:
        assert exc.code == 0

    pid = self_updater.os.getpid()
    update_log_path = tmp_path / self_updater.UPDATE_LOG_FILENAME
    launcher_log_path = tmp_path / self_updater.LAUNCHER_LOG_FILENAME

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
    assert popen_calls["close_fds"] is True

    log_content = launcher_log_path.read_text(encoding="utf-8")
    assert "Launch attempt starting." in log_content
    assert f"Updater helper path: {helper_exe}" in log_content
    assert f"Updater log path: {update_log_path}" in log_content
    assert "Updater helper launched successfully." in log_content


def test_apply_update_uses_detached_creation_flags(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, _helper, calls = _setup_apply_update(tmp_path, monkeypatch)

    try:
        self_updater.apply_update(new_exe)
    except SystemExit:
        pass

    assert calls["creationflags"] & 0x00000008  # DETACHED_PROCESS
    assert calls["creationflags"] & 0x00000200  # CREATE_NEW_PROCESS_GROUP


def test_apply_update_exits_with_code_zero(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, _helper, _calls = _setup_apply_update(tmp_path, monkeypatch)

    with __import__("pytest").raises(SystemExit) as exc_info:
        self_updater.apply_update(new_exe)
    assert exc_info.value.code == 0


def test_apply_update_raises_if_helper_missing(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, helper_exe, _calls = _setup_apply_update(tmp_path, monkeypatch)
    helper_exe.unlink()

    with __import__("pytest").raises(RuntimeError, match="Updater helper not found"):
        self_updater.apply_update(new_exe)
