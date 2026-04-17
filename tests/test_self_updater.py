def _raise_system_exit(code):
    raise SystemExit(code)


def _setup_apply_update(tmp_path, monkeypatch):
    """Shared helper: patch sys/os/subprocess so apply_update() can run in tests."""
    from core import self_updater

    exe_path = tmp_path / "HCPCSFeeApp.exe"
    exe_path.write_bytes(b"old")
    new_exe = tmp_path / "HCPCSFeeApp_new.exe"
    new_exe.write_bytes(b"new")
    bat_path = tmp_path / "swap.bat"

    monkeypatch.setattr(self_updater.sys, "frozen", True, raising=False)
    monkeypatch.setattr(self_updater.sys, "executable", str(exe_path), raising=False)
    monkeypatch.setattr(
        self_updater.tempfile, "mkstemp", lambda **_kwargs: (1, str(bat_path))
    )
    monkeypatch.setattr(self_updater.os, "close", lambda _fd: None)
    monkeypatch.setattr(self_updater.sys, "exit", _raise_system_exit)

    popen_calls = {}

    class _DummyPopen:
        def __init__(self, args, creationflags, close_fds):
            popen_calls["args"] = args
            popen_calls["creationflags"] = creationflags
            popen_calls["close_fds"] = close_fds

    monkeypatch.setattr("subprocess.Popen", _DummyPopen)

    return self_updater, exe_path, new_exe, bat_path, popen_calls


def test_apply_update_generates_robust_swap_script(tmp_path, monkeypatch):
    self_updater, exe_path, new_exe, bat_path, popen_calls = _setup_apply_update(
        tmp_path, monkeypatch
    )

    try:
        self_updater.apply_update(new_exe)
        raise AssertionError("apply_update should exit the process")
    except SystemExit as exc:
        assert exc.code == 0

    content = bat_path.read_text(encoding="cp1252")
    pid = self_updater.os.getpid()

    # ---- PID wait loop ----
    assert f'tasklist /FI "PID eq {pid}" /FO CSV /NH' in content
    assert f'findstr /B "\\"{pid}\\""' in content
    assert ":wait_pid" in content
    assert "goto wait_pid" in content

    # ---- process-gone confirmation ----
    assert f"Process {pid} no longer detected" in content

    # ---- settle pause ----
    assert "Waiting 2 s for file handles to release" in content
    assert "timeout /t 2 /nobreak >NUL" in content

    # ---- delete-old-exe loop (up to 10 retries) ----
    assert "Attempting to remove old exe" in content
    assert f'del /F /Q "{exe_path}"' in content
    assert ":del_retry" in content
    assert "goto del_retry" in content
    assert "_del_tries! gtr 10" in content

    # ---- rename-new-exe loop (up to 5 retries) ----
    assert "Renaming new exe into place" in content
    assert f'move /Y "{new_exe}" "{exe_path}"' in content
    assert ":ren_retry" in content
    assert "goto ren_retry" in content
    assert "_ren_tries! gtr 5" in content

    # ---- verify + relaunch ----
    assert ":verify_step" in content
    assert f'if not exist "{exe_path}" goto swap_failed' in content
    assert "Replacement verified" in content
    assert "Relaunching application" in content
    assert f'start "" "{exe_path}"' in content
    assert "Relaunch command issued successfully" in content

    # ---- manual recovery on failure ----
    assert "MANUAL RECOVERY INSTRUCTIONS" in content
    assert "Rename HCPCSFeeApp_new.exe to HCPCSFeeApp.exe" in content

    # ---- log filename present ----
    assert "HCPCSFeeApp_update.log" in content

    # ---- helper started / finished markers ----
    assert "Helper started" in content
    assert "Helper finished" in content

    # ---- subprocess call ----
    assert popen_calls["args"] == ["cmd.exe", "/c", str(bat_path)]


def test_apply_update_uses_detached_creation_flags(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, _bat, calls = _setup_apply_update(tmp_path, monkeypatch)

    try:
        self_updater.apply_update(new_exe)
    except SystemExit:
        pass

    assert calls["creationflags"] & 0x00000008  # DETACHED_PROCESS
    assert calls["creationflags"] & 0x00000200  # CREATE_NEW_PROCESS_GROUP


def test_apply_update_exits_with_code_zero(tmp_path, monkeypatch):
    self_updater, _exe, new_exe, _bat, _calls = _setup_apply_update(tmp_path, monkeypatch)

    with __import__("pytest").raises(SystemExit) as exc_info:
        self_updater.apply_update(new_exe)
    assert exc_info.value.code == 0
