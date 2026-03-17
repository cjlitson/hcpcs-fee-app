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
        import winreg  # noqa: F401 — confirms we are on Windows

        # Use PowerShell's WScript.Shell COM object — available on all
        # Windows Vista+ without any extra installs.
        desktop = _get_desktop_path()
        if desktop is None:
            return False

        shortcut_path = str(desktop / f"{name}.lnk")
        ps_script = (
            f'$ws = New-Object -ComObject WScript.Shell; '
            f'$sc = $ws.CreateShortcut("{shortcut_path}"); '
            f'$sc.TargetPath = "{str(exe)}"; '
            f'$sc.WorkingDirectory = "{str(exe.parent)}"; '
            f'$sc.Description = "{name}"; '
            f'$sc.Save()'
        )

        import subprocess
        result = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive",
             "-ExecutionPolicy", "Bypass", "-Command", ps_script],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


def _get_desktop_path() -> Path | None:
    """Return the current user's Desktop folder path on Windows."""
    try:
        import subprocess
        result = subprocess.run(
            [
                "powershell", "-NoProfile", "-NonInteractive",
                "-Command",
                "[Environment]::GetFolderPath('Desktop')",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        desktop = result.stdout.strip()
        if desktop:
            return Path(desktop)
    except Exception:
        pass

    # Fallback: USERPROFILE\Desktop
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
