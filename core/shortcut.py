"""Desktop shortcut creation helper.

Only functional when running as a frozen PyInstaller .exe on Windows.
Silently no-ops on other platforms / when running from source so that the
same code path is safe to call unconditionally.
"""

import sys
from pathlib import Path


def _exe_path() -> Path | None:
    """Return the path to the running .exe, or None when running from source."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable)
    return None


def can_create_shortcut() -> bool:
    """Return True if this platform and runtime support shortcut creation."""
    return sys.platform == "win32" and _exe_path() is not None


def create_desktop_shortcut(name: str = "VA HCPCS Fee Schedule Manager") -> bool:
    """Create a shortcut on the current user's Desktop pointing to the .exe.

    Returns True on success, False on failure (errors are swallowed so callers
    need not handle exceptions).  Always returns False when not on Windows or
    when running from Python source rather than a frozen .exe.
    """
    if not can_create_shortcut():
        return False

    exe = _exe_path()
    if exe is None:
        return False

    try:
        import subprocess
        import os
        import tempfile

        desktop = _get_desktop_path()
        if desktop is None:
            return False

        shortcut_path = str(desktop / f"{name}.lnk")

        # Use VBScript instead of PowerShell (VA blocks PowerShell)
        vbs_content = (
            f'Set ws = CreateObject("WScript.Shell")\n'
            f'Set sc = ws.CreateShortcut("{shortcut_path}")\n'
            f'sc.TargetPath = "{str(exe)}"\n'
            f'sc.WorkingDirectory = "{str(exe.parent)}"\n'
            f'sc.Description = "{name}"\n'
            f'sc.IconLocation = "{str(exe)},0"\n'
            f'sc.Save\n'
        )

        vbs_path = os.path.join(tempfile.gettempdir(), f"create_shortcut_{os.getpid()}_{os.urandom(4).hex()}.vbs")
        with open(vbs_path, "w") as f:
            f.write(vbs_content)

        try:
            result = subprocess.run(
                ["cscript", "//nologo", vbs_path],
                capture_output=True,
                timeout=10,
            )
            return result.returncode == 0
        finally:
            try:
                os.unlink(vbs_path)
            except Exception:
                pass
    except Exception:
        return False


def _get_desktop_path() -> Path | None:
    """Return the current user's Desktop folder path on Windows.

    Uses the Windows Shell API (SHGetFolderPathW) which correctly resolves
    OneDrive Known Folder Move redirections. No PowerShell or VBScript needed.
    """
    try:
        import ctypes
        buf = ctypes.create_unicode_buffer(260)
        # CSIDL_DESKTOPDIRECTORY = 0x0010
        ctypes.windll.shell32.SHGetFolderPathW(None, 0x0010, None, 0, buf)
        if buf.value:
            return Path(buf.value)
    except Exception:
        pass

    # Fallback: USERPROFILE\Desktop (least reliable but better than nothing)
    try:
        import os
        user_profile = os.environ.get("USERPROFILE", "")
        if user_profile:
            return Path(user_profile) / "Desktop"
    except Exception:
        pass

    return None


def shortcut_exists(name: str = "VA HCPCS Fee Schedule Manager") -> bool:
    """Return True if a shortcut with the given name already exists on the Desktop."""
    desktop = _get_desktop_path()
    if desktop is None:
        return False
    return (desktop / f"{name}.lnk").exists()
