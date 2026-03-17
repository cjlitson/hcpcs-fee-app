import sys
import sqlite3
from datetime import date
from pathlib import Path


def _get_app_dir() -> Path:
    """Return the directory where the app (or .exe) lives."""
    if getattr(sys, "frozen", False):
        # Running as a PyInstaller .exe — use the directory of the .exe
        return Path(sys.executable).parent
    else:
        # Running from source — use the project root
        return Path(__file__).parent.parent


def _get_db_path() -> Path:
    """Return the path to the SQLite database file.

    Checks ``core.config`` for a user-configured data directory first;
    falls back to ``{app_dir}/data/hcpcs_fees.db`` when no custom path is set.
    The config import is done lazily so that ``database.py`` can still be
    imported in environments where ``core.config`` might not yet exist.
    """
    try:
        from core.config import get_data_dir
        return get_data_dir() / "hcpcs_fees.db"
    except Exception:
        return _get_app_dir() / "data" / "hcpcs_fees.db"


# DB_PATH is set at import time from the config (or default location).
# Tests may monkeypatch this module-level name to redirect to a temp DB.
DB_PATH = _get_db_path()


def _get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = _get_conn()
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS modifiers (
                modifier_code TEXT PRIMARY KEY,
                description   TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS states (
                state_abbr    TEXT PRIMARY KEY,
                state_name    TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                key           TEXT PRIMARY KEY,
                value         TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS selected_states (
                state_abbr    TEXT PRIMARY KEY,
                state_name    TEXT NOT NULL,
                added_at      DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hcpcs_fees (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                hcpcs_code    TEXT NOT NULL,
                description   TEXT NOT NULL,
                state_abbr    TEXT NOT NULL,
                year          INTEGER NOT NULL,
                allowable     REAL,
                allowable_nr  REAL,
                allowable_r   REAL,
                modifier      TEXT,
                data_source   TEXT,
                imported_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (state_abbr) REFERENCES states(state_abbr)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name     TEXT NOT NULL,
                source        TEXT NOT NULL,
                imported_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                record_count  INTEGER,
                states        TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rural_zips (
                year       INTEGER NOT NULL,
                zip5       TEXT NOT NULL,
                state_abbr TEXT,
                PRIMARY KEY (year, zip5)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hcpcs_code ON hcpcs_fees(hcpcs_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_year_state ON hcpcs_fees(year, state_abbr)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rural_zips ON rural_zips(year, zip5)")

        # Lightweight migration: add new columns to hcpcs_fees if they don't exist
        _migrate_hcpcs_fees(conn)
    conn.close()


def _migrate_hcpcs_fees(conn):
    """Add allowable_nr and allowable_r columns if they don't already exist."""
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(hcpcs_fees)").fetchall()
    }
    for col, coltype in (("allowable_nr", "REAL"), ("allowable_r", "REAL")):
        if col not in existing:
            conn.execute(f"ALTER TABLE hcpcs_fees ADD COLUMN {col} {coltype}")


def get_selected_states():
    """Return list of (state_abbr, state_name) for user-selected states."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT state_abbr, state_name FROM selected_states ORDER BY state_abbr"
    ).fetchall()
    conn.close()
    return [(r["state_abbr"], r["state_name"]) for r in rows]


def save_selected_states(states):
    """Replace all selected states. states: list of (state_abbr, state_name)."""
    conn = _get_conn()
    with conn:
        conn.execute("DELETE FROM selected_states")
        conn.executemany(
            "INSERT INTO selected_states (state_abbr, state_name) VALUES (?, ?)",
            states,
        )
    conn.close()


def insert_fees(records, data_source="import"):
    """Bulk-insert fee records. Each record is a dict with keys:
    hcpcs_code, description, state_abbr, year, allowable, modifier.
    Optional keys: allowable_nr, allowable_r.
    """
    if not records:
        return
    rows = [{**r, "data_source": data_source} for r in records]
    conn = _get_conn()
    with conn:
        conn.executemany(
            """
            INSERT INTO hcpcs_fees
                (hcpcs_code, description, state_abbr, year, allowable,
                 allowable_nr, allowable_r, modifier, data_source)
            VALUES
                (:hcpcs_code, :description, :state_abbr, :year, :allowable,
                 :allowable_nr, :allowable_r, :modifier, :data_source)
            """,
            [
                {
                    "hcpcs_code": r.get("hcpcs_code", ""),
                    "description": r.get("description", ""),
                    "state_abbr": r.get("state_abbr", ""),
                    "year": r.get("year"),
                    "allowable": r.get("allowable"),
                    "allowable_nr": r.get("allowable_nr"),
                    "allowable_r": r.get("allowable_r"),
                    "modifier": r.get("modifier"),
                    "data_source": r.get("data_source", data_source),
                }
                for r in rows
            ],
        )
    conn.close()


def delete_fees_by_year_state_source(state_abbr, year, data_source="cms_download"):
    """Delete all fee records matching the given state, year, and data_source.

    Used by the CMS sync to implement "replace" semantics: existing rows for a
    (state, year, data_source) triplet are removed before new rows are inserted,
    so repeated quarterly syncs update rather than duplicate data.
    """
    conn = _get_conn()
    with conn:
        conn.execute(
            "DELETE FROM hcpcs_fees WHERE state_abbr = ? AND year = ? AND data_source = ?",
            (state_abbr, year, data_source),
        )
    conn.close()


def get_fees(state_abbr=None, year=None, hcpcs_code=None, keyword=None, hcpcs_group=None):
    """Query fee records with optional filters. Returns list of dicts."""
    query = "SELECT * FROM hcpcs_fees WHERE 1=1"
    params = []
    if state_abbr:
        query += " AND state_abbr = ?"
        params.append(state_abbr)
    if year:
        query += " AND year = ?"
        params.append(year)
    if hcpcs_group:
        query += " AND hcpcs_code LIKE ?"
        params.append(f"{hcpcs_group.upper()}%")
    if hcpcs_code:
        query += " AND hcpcs_code LIKE ?"
        params.append(f"%{hcpcs_code.upper()}%")
    if keyword:
        query += " AND description LIKE ?"
        params.append(f"%{keyword}%")
    query += " ORDER BY hcpcs_code, state_abbr, year"
    conn = _get_conn()
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_available_years():
    """Return sorted list of years present in hcpcs_fees."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT DISTINCT year FROM hcpcs_fees ORDER BY year DESC"
    ).fetchall()
    conn.close()
    return [r["year"] for r in rows]


def add_import_log(file_name, source, record_count, states):
    conn = _get_conn()
    with conn:
        conn.execute(
            "INSERT INTO import_log (file_name, source, record_count, states) VALUES (?, ?, ?, ?)",
            (file_name, source, record_count, states),
        )
    conn.close()


def get_import_log():
    """Return all import log entries, newest first."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM import_log ORDER BY imported_at DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_preference(key, default=None):
    conn = _get_conn()
    row = conn.execute(
        "SELECT value FROM user_preferences WHERE key = ?", (key,)
    ).fetchone()
    conn.close()
    return row["value"] if row else default


def set_preference(key, value):
    conn = _get_conn()
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO user_preferences (key, value) VALUES (?, ?)",
            (key, value),
        )
    conn.close()


def get_selected_years():
    """Return list of persisted selected years, or empty list if none saved."""
    val = get_preference("selected_years")
    if not val:
        return []
    try:
        return [int(y) for y in val.split(",") if y.strip()]
    except ValueError:
        return []


def save_selected_years(years):
    """Persist selected years to user preferences."""
    set_preference("selected_years", ",".join(str(y) for y in sorted(years)))


# ---------------------------------------------------------------------------
# Rural ZIP helpers
# ---------------------------------------------------------------------------

def _normalize_zip5(zip5):
    """Normalize a ZIP code string to a 5-character zero-padded string."""
    return str(zip5).strip().zfill(5)


def insert_rural_zips(records):
    """Bulk-insert rural ZIP records.

    Each record is a dict with keys: year (int), zip5 (str), state_abbr (str or None).
    Uses INSERT OR REPLACE to handle duplicates.
    """
    if not records:
        return
    conn = _get_conn()
    with conn:
        conn.executemany(
            "INSERT OR REPLACE INTO rural_zips (year, zip5, state_abbr) VALUES (:year, :zip5, :state_abbr)",
            [
                {
                    "year": r["year"],
                    "zip5": _normalize_zip5(r["zip5"]),
                    "state_abbr": r.get("state_abbr"),
                }
                for r in records
            ],
        )
    conn.close()


def get_rural_zips(year=None):
    """Return rural ZIP records as list of dicts.

    Each dict has keys: year, zip5, state_abbr.
    If *year* is given only records for that year are returned.
    """
    conn = _get_conn()
    if year is not None:
        rows = conn.execute(
            "SELECT year, zip5, state_abbr FROM rural_zips WHERE year = ? ORDER BY zip5",
            (year,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT year, zip5, state_abbr FROM rural_zips ORDER BY year, zip5"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_rural_zips_by_year(year):
    """Delete all rural ZIP records for the given year."""
    conn = _get_conn()
    with conn:
        conn.execute("DELETE FROM rural_zips WHERE year = ?", (year,))
    conn.close()


def is_rural_zip(year, zip5):
    """Return True if *zip5* is classified as rural for *year*, else False.

    If *year* has no rural ZIP data stored, returns False (default NR).
    """
    z = _normalize_zip5(zip5)
    conn = _get_conn()
    row = conn.execute(
        "SELECT 1 FROM rural_zips WHERE year = ? AND zip5 = ?", (year, z)
    ).fetchone()
    conn.close()
    return row is not None


# ---------------------------------------------------------------------------
# Year fallback helper
# ---------------------------------------------------------------------------

def get_current_year_or_fallback():
    """Return the current calendar year if it has data in DB, else the most recent year with data.

    Returns None if the database has no fee data at all.
    """
    current = date.today().year
    available = get_available_years()  # sorted DESC
    if not available:
        return None
    if current in available:
        return current
    return available[0]  # most recent year present


def get_default_selected_years(supported_years=None):
    """Return the default set of years to select on first run.

    Current year + last 3 years, bounded by *supported_years*.
    If *supported_years* is None, imports from cms_downloader at call time.
    """
    current = date.today().year
    if supported_years is None:
        from core.cms_downloader import SUPPORTED_YEARS
        supported_years = SUPPORTED_YEARS
    supported_set = set(supported_years)
    candidates = [current - i for i in range(4)]  # current, -1, -2, -3
    return sorted([y for y in candidates if y in supported_set], reverse=True)
