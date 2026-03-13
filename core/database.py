import sys
import sqlite3
from pathlib import Path


def _get_app_dir() -> Path:
    """Return the directory where the app (or .exe) lives."""
    if getattr(sys, "frozen", False):
        # Running as a PyInstaller .exe — use the directory of the .exe
        return Path(sys.executable).parent
    else:
        # Running from source — use the project root
        return Path(__file__).parent.parent


DB_PATH = _get_app_dir() / "data" / "hcpcs_fees.db"


def _get_conn():
    DB_PATH.parent.mkdir(exist_ok=True)
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
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hcpcs_code ON hcpcs_fees(hcpcs_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_year_state ON hcpcs_fees(year, state_abbr)")
    conn.close()


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
    """
    if not records:
        return
    rows = [{**r, "data_source": data_source} for r in records]
    conn = _get_conn()
    with conn:
        conn.executemany(
            """
            INSERT INTO hcpcs_fees
                (hcpcs_code, description, state_abbr, year, allowable, modifier, data_source)
            VALUES
                (:hcpcs_code, :description, :state_abbr, :year, :allowable, :modifier, :data_source)
            """,
            rows,
        )
    conn.close()


def get_fees(state_abbr=None, year=None, hcpcs_code=None, keyword=None):
    """Query fee records with optional filters. Returns list of dicts."""
    query = "SELECT * FROM hcpcs_fees WHERE 1=1"
    params = []
    if state_abbr:
        query += " AND state_abbr = ?"
        params.append(state_abbr)
    if year:
        query += " AND year = ?"
        params.append(year)
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
