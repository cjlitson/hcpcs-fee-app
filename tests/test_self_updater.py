def test_apply_update_generates_robust_swap_script(tmp_path, monkeypatch):
    from core import self_updater

    exe_path = tmp_path / "HCPCSFeeApp.exe"
    exe_path.write_bytes(b"old")
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    new_exe.write_bytes(b"new")
    bat_path = tmp_path / "swap.bat"

    monkeypatch.setattr(self_updater.sys, "frozen", True, raising=False)
    monkeypatch.setattr(self_updater.sys, "executable", str(exe_path), raising=False)
    monkeypatch.setattr(self_updater.tempfile, "mkstemp", lambda **_kwargs: (1, str(bat_path)))
    monkeypatch.setattr(self_updater.os, "close", lambda _fd: None)

    popen_calls = {}

    class _DummyPopen:
        def __init__(self, args, creationflags, close_fds):
            popen_calls["args"] = args
            popen_calls["creationflags"] = creationflags
            popen_calls["close_fds"] = close_fds

    monkeypatch.setattr("subprocess.Popen", _DummyPopen)
    monkeypatch.setattr(self_updater.sys, "exit", lambda code: (_ for _ in ()).throw(SystemExit(code)))

    try:
        self_updater.apply_update(new_exe)
        raise AssertionError("apply_update should exit the process")
    except SystemExit as exc:
        assert exc.code == 0

    content = bat_path.read_text(encoding="cp1252")
    pid = self_updater.os.getpid()

    assert f'tasklist /FI "PID eq {pid}" /FO CSV /NH' in content
    assert f'findstr /B "\\"{pid}\\""' in content
    assert "set /a _swap_tries=0" in content
    assert "if !_swap_tries! gtr 5 goto swap_failed" in content
    assert "if not errorlevel 1 goto swap_ok" in content
    assert f'if not exist "{exe_path}" goto swap_failed' in content
    assert "HCPCSFeeApp_update_" in content
    assert popen_calls["args"] == ["cmd.exe", "/c", str(bat_path)]


def test_apply_update_uses_detached_creation_flags(tmp_path, monkeypatch):
    from core import self_updater

    exe_path = tmp_path / "HCPCSFeeApp.exe"
    exe_path.write_bytes(b"old")
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    new_exe.write_bytes(b"new")
    bat_path = tmp_path / "swap.bat"

    monkeypatch.setattr(self_updater.sys, "frozen", True, raising=False)
    monkeypatch.setattr(self_updater.sys, "executable", str(exe_path), raising=False)
    monkeypatch.setattr(self_updater.tempfile, "mkstemp", lambda **_kwargs: (1, str(bat_path)))
    monkeypatch.setattr(self_updater.os, "close", lambda _fd: None)
    monkeypatch.setattr(self_updater.sys, "exit", lambda code: (_ for _ in ()).throw(SystemExit(code)))

    calls = {}

    class _DummyPopen:
        def __init__(self, args, creationflags, close_fds):
            calls["creationflags"] = creationflags

    monkeypatch.setattr("subprocess.Popen", _DummyPopen)

    try:
        self_updater.apply_update(new_exe)
    except SystemExit:
        pass

    assert calls["creationflags"] & 0x00000008  # DETACHED_PROCESS
    assert calls["creationflags"] & 0x00000200  # CREATE_NEW_PROCESS_GROUP
