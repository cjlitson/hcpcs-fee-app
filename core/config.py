"""App-level configuration stored in a JSON file alongside the executable.

This module intentionally does NOT import from core.database — the config
file is used to *locate* the database, so reading it must be possible before
the database connection is opened.
"""

import json
import sys
from pathlib import Path

_CONFIG_FILENAME = "hcpcs_app_config.json"


def _get_app_dir() -> Path:
    """Return the directory where the app (or .exe) lives."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).parent.parent


def _config_path() -> Path:
    return _get_app_dir() / _CONFIG_FILENAME


def _load() -> dict:
    path = _config_path()
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save(cfg: dict) -> None:
    try:
        with open(_config_path(), "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except Exception:
        pass


def get_data_dir() -> Path:
    """Return the configured data directory (where hcpcs_fees.db lives).

    Falls back to ``{app_dir}/data/`` when no custom path has been set or
    the configured path no longer exists.
    """
    cfg = _load()
    if "data_dir" in cfg:
        d = Path(cfg["data_dir"])
        try:
            d.mkdir(parents=True, exist_ok=True)
            return d
        except Exception:
            pass  # Fall through to default
    default = _get_app_dir() / "data"
    default.mkdir(parents=True, exist_ok=True)
    return default


def set_data_dir(path: Path) -> None:
    """Persist a custom data directory path."""
    cfg = _load()
    cfg["data_dir"] = str(path)
    _save(cfg)


def get_config_value(key: str, default=None):
    """Get an arbitrary top-level config value."""
    return _load().get(key, default)


def set_config_value(key: str, value) -> None:
    """Set an arbitrary top-level config value."""
    cfg = _load()
    cfg[key] = value
    _save(cfg)
