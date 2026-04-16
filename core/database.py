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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS purchase_list_bundles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE,
                category_id INTEGER,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES purchase_list_bundle_categories(id) ON DELETE SET NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS purchase_list_bundle_categories (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL UNIQUE,
                created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS purchase_list_bundle_items (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                bundle_id   INTEGER NOT NULL,
                hcpcs_code  TEXT NOT NULL,
                quantity    INTEGER NOT NULL DEFAULT 1,
                sort_order  INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (bundle_id) REFERENCES purchase_list_bundles(id) ON DELETE CASCADE
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hcpcs_code ON hcpcs_fees(hcpcs_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_year_state ON hcpcs_fees(year, state_abbr)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rural_zips ON rural_zips(year, zip5)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bundle_items_bundle ON purchase_list_bundle_items(bundle_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bundle_items_code ON purchase_list_bundle_items(hcpcs_code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bundles_category_id ON purchase_list_bundles(category_id)")

        # Lightweight migration: add new columns to hcpcs_fees if they don't exist
        _migrate_hcpcs_fees(conn)
        _migrate_purchase_bundles(conn)
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


def _migrate_purchase_bundles(conn):
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(purchase_list_bundles)").fetchall()
    }
    if "category_id" not in existing:
        conn.execute("ALTER TABLE purchase_list_bundles ADD COLUMN category_id INTEGER")


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



def get_available_hcpcs_prefixes():
    """Return the set of HCPCS code prefix letters that have data in the database."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT DISTINCT UPPER(SUBSTR(hcpcs_code, 1, 1)) AS prefix FROM hcpcs_fees"
        ).fetchall()
        return {r["prefix"] for r in rows}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Purchase list bundle helpers
# ---------------------------------------------------------------------------

def save_bundle(name, items, category_id=None):
    """Create or replace a named purchase-list bundle.

    *name* is a unique bundle name.
    *items* is a list of dicts with keys: hcpcs_code, quantity, sort_order.
    Returns the bundle ID.
    """
    bundle_name = (name or "").strip()
    if not bundle_name:
        raise ValueError("Bundle name is required.")

    normalized_items = []
    for idx, item in enumerate(items or []):
        code = (item.get("hcpcs_code") or "").strip().upper()
        if not code:
            continue
        qty = int(item.get("quantity", 1) or 1)
        if qty < 1:
            qty = 1
        normalized_items.append(
            {
                "hcpcs_code": code,
                "quantity": qty,
                "sort_order": int(item.get("sort_order", idx) or idx),
            }
        )

    conn = _get_conn()
    with conn:
        existing = conn.execute(
            "SELECT id FROM purchase_list_bundles WHERE name = ?",
            (bundle_name,),
        ).fetchone()
        if existing:
            bundle_id = existing["id"]
            conn.execute(
                """
                UPDATE purchase_list_bundles
                SET updated_at = CURRENT_TIMESTAMP,
                    category_id = ?
                WHERE id = ?
                """,
                (category_id, bundle_id),
            )
            conn.execute("DELETE FROM purchase_list_bundle_items WHERE bundle_id = ?", (bundle_id,))
        else:
            cur = conn.execute(
                "INSERT INTO purchase_list_bundles (name, category_id) VALUES (?, ?)",
                (bundle_name, category_id),
            )
            bundle_id = cur.lastrowid
        if normalized_items:
            conn.executemany(
                """
                INSERT INTO purchase_list_bundle_items (bundle_id, hcpcs_code, quantity, sort_order)
                VALUES (:bundle_id, :hcpcs_code, :quantity, :sort_order)
                """,
                [{"bundle_id": bundle_id, **item} for item in normalized_items],
            )
    conn.close()
    return bundle_id


def load_bundle(bundle_id):
    """Load a bundle and its items by ID. Returns dict or None."""
    conn = _get_conn()
    bundle = conn.execute(
        """
        SELECT id, name, category_id, created_at, updated_at
        FROM purchase_list_bundles
        WHERE id = ?
        """,
        (bundle_id,),
    ).fetchone()
    if not bundle:
        conn.close()
        return None
    items = conn.execute(
        """
        SELECT hcpcs_code, quantity, sort_order
        FROM purchase_list_bundle_items
        WHERE bundle_id = ?
        ORDER BY sort_order, id
        """,
        (bundle_id,),
    ).fetchall()
    conn.close()
    return {
        "id": bundle["id"],
        "name": bundle["name"],
        "category_id": bundle["category_id"] if "category_id" in bundle.keys() else None,
        "created_at": bundle["created_at"],
        "updated_at": bundle["updated_at"],
        "items": [dict(r) for r in items],
    }


def list_bundles():
    """Return all bundles with metadata and item counts."""
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT
            b.id,
            b.name,
            b.category_id,
            c.name AS category_name,
            b.created_at,
            b.updated_at,
            COUNT(i.id) AS item_count
        FROM purchase_list_bundles b
        LEFT JOIN purchase_list_bundle_categories c ON c.id = b.category_id
        LEFT JOIN purchase_list_bundle_items i ON i.bundle_id = b.id
        GROUP BY b.id, b.name, b.category_id, c.name, b.created_at, b.updated_at
        ORDER BY LOWER(COALESCE(c.name, '')), LOWER(b.name), b.id
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_bundle(bundle_id):
    """Delete a bundle and all of its items."""
    conn = _get_conn()
    with conn:
        conn.execute("DELETE FROM purchase_list_bundle_items WHERE bundle_id = ?", (bundle_id,))
        conn.execute("DELETE FROM purchase_list_bundles WHERE id = ?", (bundle_id,))
    conn.close()


def rename_bundle(bundle_id, new_name):
    """Rename a bundle by ID."""
    name = (new_name or "").strip()
    if not name:
        raise ValueError("Bundle name is required.")
    conn = _get_conn()
    with conn:
        conn.execute(
            """
            UPDATE purchase_list_bundles
            SET name = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (name, bundle_id),
        )
    conn.close()


def list_bundle_categories():
    conn = _get_conn()
    rows = conn.execute(
        """
        SELECT id, name, created_at, updated_at
        FROM purchase_list_bundle_categories
        ORDER BY LOWER(name), id
        """
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_bundle_category(name):
    category_name = (name or "").strip()
    if not category_name:
        raise ValueError("Category name is required.")
    conn = _get_conn()
    with conn:
        cur = conn.execute(
            "INSERT INTO purchase_list_bundle_categories (name) VALUES (?)",
            (category_name,),
        )
        category_id = cur.lastrowid
    conn.close()
    return category_id


def rename_bundle_category(category_id, new_name):
    category_name = (new_name or "").strip()
    if not category_name:
        raise ValueError("Category name is required.")
    conn = _get_conn()
    with conn:
        conn.execute(
            """
            UPDATE purchase_list_bundle_categories
            SET name = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (category_name, category_id),
        )
    conn.close()


def delete_bundle_category(category_id):
    conn = _get_conn()
    with conn:
        conn.execute(
            "UPDATE purchase_list_bundles SET category_id = NULL WHERE category_id = ?",
            (category_id,),
        )
        conn.execute(
            "DELETE FROM purchase_list_bundle_categories WHERE id = ?",
            (category_id,),
        )
    conn.close()


def set_bundle_category(bundle_id, category_id):
    conn = _get_conn()
    with conn:
        conn.execute(
            """
            UPDATE purchase_list_bundles
            SET category_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (category_id, bundle_id),
        )
    conn.close()
