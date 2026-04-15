'-- HCPCS Fee Schedule Database Schema

CREATE TABLE IF NOT EXISTS modifiers (
    modifier_code TEXT PRIMARY KEY,
    description   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS states (
    state_abbr    TEXT PRIMARY KEY,
    state_name    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_preferences (
    key           TEXT PRIMARY KEY,
    value         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS selected_states (
    state_abbr    TEXT PRIMARY KEY,
    state_name    TEXT NOT NULL,
    added_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

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
);

CREATE TABLE IF NOT EXISTS import_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name     TEXT NOT NULL,
    source        TEXT NOT NULL,
    imported_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    record_count  INTEGER,
    states        TEXT
);

CREATE TABLE IF NOT EXISTS rural_zips (
    year       INTEGER NOT NULL,
    zip5       TEXT NOT NULL,
    state_abbr TEXT,
    PRIMARY KEY (year, zip5)
);

CREATE TABLE IF NOT EXISTS purchase_list_bundles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchase_list_bundle_items (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    bundle_id   INTEGER NOT NULL,
    hcpcs_code  TEXT NOT NULL,
    quantity    INTEGER NOT NULL DEFAULT 1,
    sort_order  INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (bundle_id) REFERENCES purchase_list_bundles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_hcpcs_code  ON hcpcs_fees(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_year_state  ON hcpcs_fees(year, state_abbr);
CREATE INDEX IF NOT EXISTS idx_rural_zips ON rural_zips(year, zip5);
CREATE INDEX IF NOT EXISTS idx_bundle_items_bundle ON purchase_list_bundle_items(bundle_id);
CREATE INDEX IF NOT EXISTS idx_bundle_items_code ON purchase_list_bundle_items(hcpcs_code);
'
