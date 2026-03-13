"""SQL publishing — push hcpcs_fees records to SQL Server or Databricks."""

from datetime import datetime, timezone

# --- SQL Server DDL ---
_MSSQL_CREATE_TABLE = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
)
CREATE TABLE [{schema}].[{table}] (
    id            INT IDENTITY(1,1) PRIMARY KEY,
    hcpcs_code    NVARCHAR(20)  NOT NULL,
    description   NVARCHAR(500) NOT NULL,
    state_abbr    NVARCHAR(2)   NOT NULL,
    year          INT           NOT NULL,
    allowable     FLOAT,
    modifier      NVARCHAR(10),
    data_source   NVARCHAR(100),
    imported_at   DATETIME2     DEFAULT GETUTCDATE()
);
"""

# --- Databricks DDL ---
_DATABRICKS_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (
    id            BIGINT GENERATED ALWAYS AS IDENTITY,
    hcpcs_code    STRING NOT NULL,
    description   STRING NOT NULL,
    state_abbr    STRING NOT NULL,
    year          INT    NOT NULL,
    allowable     DOUBLE,
    modifier      STRING,
    data_source   STRING,
    imported_at   TIMESTAMP DEFAULT current_timestamp()
)
"""

_CHUNK_SIZE = 500


def get_sqlserver_connection(server, database, username, password, use_windows_auth=False):
    """Return a pyodbc connection to SQL Server."""
    try:
        import pyodbc
    except ImportError:
        raise RuntimeError(
            "pyodbc is not installed. Install it with: pip install pyodbc"
        )

    # Find the best available ODBC driver
    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        raise RuntimeError(
            "SQL Server ODBC Driver not found. Please install 'ODBC Driver 17 for SQL Server' "
            "or later from Microsoft."
        )
    # Prefer newer drivers
    driver = sorted(drivers)[-1]

    if use_windows_auth:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )

    return pyodbc.connect(conn_str, timeout=15)


def get_databricks_connection(server_hostname, http_path, access_token, catalog, schema):
    """Return a databricks-sql-connector connection."""
    try:
        from databricks import sql as databricks_sql
    except ImportError:
        raise RuntimeError(
            "databricks-sql-connector is not installed. "
            "Install it with: pip install databricks-sql-connector"
        )

    conn = databricks_sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema=schema,
    )
    return conn


def test_connection(conn):
    """Test a connection by running SELECT 1. Returns True or raises."""
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchone()
    cursor.close()
    return True


def ensure_table_exists(conn, db_type, table_name, schema="dbo"):
    """Create the hcpcs_fees table if it doesn't exist."""
    cursor = conn.cursor()
    if db_type == "sqlserver":
        ddl = _MSSQL_CREATE_TABLE.format(schema=schema, table=table_name)
    else:
        ddl = _DATABRICKS_CREATE_TABLE.format(schema=schema, table=table_name)
    cursor.execute(ddl)
    conn.commit()
    cursor.close()


def publish_records(conn, db_type, records, table_name, mode, schema="dbo"):
    """
    Push records to the remote table.

    mode: "merge" or "replace"

    For "replace": delete rows where (state_abbr, year) matches any in the batch,
    then bulk insert.
    For "merge": upsert on (hcpcs_code, state_abbr, year, modifier).

    Returns count of rows pushed.
    """
    if not records:
        return 0

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # Normalise records — fill in missing keys
    normalised = []
    for r in records:
        normalised.append({
            "hcpcs_code": r.get("hcpcs_code", "") or "",
            "description": r.get("description", "") or "",
            "state_abbr": r.get("state_abbr", "") or "",
            "year": int(r.get("year", 0) or 0),
            "allowable": r.get("allowable"),
            "modifier": r.get("modifier") or "",
            "data_source": r.get("data_source") or "",
            "imported_at": now,
        })

    if db_type == "sqlserver":
        if mode == "replace":
            _replace_sqlserver(conn, normalised, table_name, schema)
        else:
            _merge_sqlserver(conn, normalised, table_name, schema)
    else:
        if mode == "replace":
            _replace_databricks(conn, normalised, table_name, schema)
        else:
            _merge_databricks(conn, normalised, table_name, schema)

    return len(normalised)


# ----------------------------------------------------------------- SQL Server helpers --

def _replace_sqlserver(conn, records, table_name, schema):
    """Delete matching scope rows, then bulk insert."""
    cursor = conn.cursor()

    # Collect distinct (state_abbr, year) pairs in the batch
    scope = list({(r["state_abbr"], r["year"]) for r in records})

    # Delete existing rows for each (state_abbr, year) pair using parameterized queries
    delete_sql = f"DELETE FROM [{schema}].[{table_name}] WHERE state_abbr = ? AND year = ?"
    for state_abbr, year in scope:
        cursor.execute(delete_sql, (state_abbr, year))

    # Bulk insert in chunks
    _bulk_insert_sqlserver(cursor, records, table_name, schema)
    conn.commit()
    cursor.close()


def _merge_sqlserver(conn, records, table_name, schema):
    """Upsert via MERGE on (hcpcs_code, state_abbr, year, modifier)."""
    cursor = conn.cursor()

    merge_sql = f"""
        MERGE [{schema}].[{table_name}] AS target
        USING (SELECT ? AS hcpcs_code, ? AS description, ? AS state_abbr,
                      ? AS year, ? AS allowable, ? AS modifier,
                      ? AS data_source, ? AS imported_at) AS source
            ON (target.hcpcs_code = source.hcpcs_code
            AND target.state_abbr = source.state_abbr
            AND target.year       = source.year
            AND ISNULL(target.modifier, '') = ISNULL(source.modifier, ''))
        WHEN MATCHED THEN UPDATE SET
            target.allowable    = source.allowable,
            target.description  = source.description,
            target.data_source  = source.data_source,
            target.imported_at  = source.imported_at
        WHEN NOT MATCHED THEN INSERT
            (hcpcs_code, description, state_abbr, year,
             allowable, modifier, data_source, imported_at)
        VALUES
            (source.hcpcs_code, source.description, source.state_abbr,
             source.year, source.allowable, source.modifier,
             source.data_source, source.imported_at);
    """

    for i in range(0, len(records), _CHUNK_SIZE):
        chunk = records[i: i + _CHUNK_SIZE]
        for r in chunk:
            cursor.execute(
                merge_sql,
                (
                    r["hcpcs_code"], r["description"], r["state_abbr"],
                    r["year"], r["allowable"], r["modifier"],
                    r["data_source"], r["imported_at"],
                ),
            )
    conn.commit()
    cursor.close()


def _bulk_insert_sqlserver(cursor, records, table_name, schema):
    insert_sql = (
        f"INSERT INTO [{schema}].[{table_name}] "
        "(hcpcs_code, description, state_abbr, year, allowable, modifier, data_source, imported_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    for i in range(0, len(records), _CHUNK_SIZE):
        chunk = records[i: i + _CHUNK_SIZE]
        cursor.executemany(
            insert_sql,
            [
                (
                    r["hcpcs_code"], r["description"], r["state_abbr"],
                    r["year"], r["allowable"], r["modifier"],
                    r["data_source"], r["imported_at"],
                )
                for r in chunk
            ],
        )


# ----------------------------------------------------------------- Databricks helpers --

def _replace_databricks(conn, records, table_name, schema):
    """Delete matching scope rows, then bulk insert."""
    cursor = conn.cursor()

    scope = list({(r["state_abbr"], r["year"]) for r in records})
    # Use parameterized individual deletes per (state_abbr, year) pair
    delete_sql = f"DELETE FROM `{schema}`.`{table_name}` WHERE state_abbr = ? AND year = ?"
    for state_abbr, year in scope:
        cursor.execute(delete_sql, (state_abbr, year))

    _bulk_insert_databricks(cursor, records, table_name, schema)
    conn.commit()
    cursor.close()


def _merge_databricks(conn, records, table_name, schema):
    """Upsert via Delta Lake MERGE INTO."""
    cursor = conn.cursor()

    merge_sql = f"""
        MERGE INTO `{schema}`.`{table_name}` AS target
        USING (SELECT
            ? AS hcpcs_code,
            ? AS description,
            ? AS state_abbr,
            ? AS year,
            ? AS allowable,
            ? AS modifier,
            ? AS data_source,
            ? AS imported_at
        ) AS source
        ON (target.hcpcs_code = source.hcpcs_code
        AND target.state_abbr = source.state_abbr
        AND target.year       = source.year
        AND COALESCE(target.modifier, '') = COALESCE(source.modifier, ''))
        WHEN MATCHED THEN UPDATE SET
            target.allowable   = source.allowable,
            target.description = source.description,
            target.data_source = source.data_source,
            target.imported_at = source.imported_at
        WHEN NOT MATCHED THEN INSERT
            (hcpcs_code, description, state_abbr, year,
             allowable, modifier, data_source, imported_at)
        VALUES
            (source.hcpcs_code, source.description, source.state_abbr,
             source.year, source.allowable, source.modifier,
             source.data_source, source.imported_at)
    """

    for i in range(0, len(records), _CHUNK_SIZE):
        chunk = records[i: i + _CHUNK_SIZE]
        for r in chunk:
            cursor.execute(
                merge_sql,
                (
                    r["hcpcs_code"], r["description"], r["state_abbr"],
                    r["year"], r["allowable"], r["modifier"],
                    r["data_source"], r["imported_at"],
                ),
            )
    conn.commit()
    cursor.close()


def _bulk_insert_databricks(cursor, records, table_name, schema):
    insert_sql = (
        f"INSERT INTO `{schema}`.`{table_name}` "
        "(hcpcs_code, description, state_abbr, year, allowable, modifier, data_source, imported_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    for i in range(0, len(records), _CHUNK_SIZE):
        chunk = records[i: i + _CHUNK_SIZE]
        cursor.executemany(
            insert_sql,
            [
                (
                    r["hcpcs_code"], r["description"], r["state_abbr"],
                    r["year"], r["allowable"], r["modifier"],
                    r["data_source"], r["imported_at"],
                )
                for r in chunk
            ],
        )
