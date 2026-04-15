import json
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from core.config import get_data_dir, _config_path
from core.database import get_available_years, get_selected_states
from core.version import APP_VERSION


def _db_path() -> Path:
    return get_data_dir() / "hcpcs_fees.db"


def _record_counts(db_path: Path) -> dict:
    counts = {}
    if not db_path.exists():
        return counts
    conn = sqlite3.connect(str(db_path))
    try:
        tables = [
            "hcpcs_fees",
            "purchase_list_bundles",
            "purchase_list_bundle_items",
            "selected_states",
            "import_log",
            "rural_zips",
        ]
        for table in tables:
            try:
                counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            except sqlite3.Error:
                counts[table] = 0
    finally:
        conn.close()
    return counts


def create_backup(output_path):
    output = Path(output_path)
    if output.suffix.lower() != ".zip":
        output = output.with_suffix(".zip")
    output.parent.mkdir(parents=True, exist_ok=True)

    db_path = _db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    states = [abbr for abbr, _name in get_selected_states()]
    years = get_available_years()
    manifest = {
        "app_version": APP_VERSION,
        "backup_date": datetime.now(timezone.utc).isoformat(),
        "database_size": db_path.stat().st_size,
        "record_counts": _record_counts(db_path),
        "selected_states": states,
        "available_years": years,
    }

    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(db_path, arcname="hcpcs_fees.db")
        cfg_path = _config_path()
        if cfg_path.exists():
            zf.write(cfg_path, arcname="hcpcs_app_config.json")
        zf.writestr("manifest.json", json.dumps(manifest, indent=2))
    return str(output)


def inspect_backup(zip_path):
    path = Path(zip_path)
    if not path.exists():
        raise FileNotFoundError(f"Backup file not found: {path}")
    with zipfile.ZipFile(path, "r") as zf:
        names = set(zf.namelist())
        required = {"hcpcs_fees.db", "manifest.json"}
        missing = required - names
        if missing:
            raise ValueError(f"Invalid backup archive. Missing: {', '.join(sorted(missing))}")
        try:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        except Exception as exc:
            raise ValueError("Invalid backup manifest.json") from exc
    return manifest


def restore_backup(zip_path):
    manifest = inspect_backup(zip_path)
    data_dir = get_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    db_dest = data_dir / "hcpcs_fees.db"
    cfg_dest = _config_path()
    cfg_dest.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        db_tmp = data_dir / "hcpcs_fees.db.restore_tmp"
        with db_tmp.open("wb") as f:
            f.write(zf.read("hcpcs_fees.db"))
        db_tmp.replace(db_dest)

        if "hcpcs_app_config.json" in zf.namelist():
            cfg_tmp = cfg_dest.parent / "hcpcs_app_config.json.restore_tmp"
            with cfg_tmp.open("wb") as f:
                f.write(zf.read("hcpcs_app_config.json"))
            cfg_tmp.replace(cfg_dest)

    return {
        "manifest": manifest,
        "database_path": str(db_dest),
        "record_counts": manifest.get("record_counts", {}),
    }
