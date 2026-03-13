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

CREATE INDEX IF NOT EXISTS idx_hcpcs_code  ON hcpcs_fees(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_year_state  ON hcpcs_fees(year, state_abbr);
'