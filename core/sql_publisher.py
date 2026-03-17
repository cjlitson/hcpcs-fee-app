"""SQL publishing — push hcpcs_fees records to SQL Server or Databricks."""

from datetime import datetime, timezone

# --- SQL Server DDL (fee table) ---
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

# --- SQL Server DDL (zip table) ---
_MSSQL_CREATE_ZIP_TABLE = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
)
CREATE TABLE [{schema}].[{table}] (
    year          INT           NOT NULL,
    zip5          NVARCHAR(5)   NOT NULL,
    state_abbr    NVARCHAR(2),
    imported_at   DATETIME2     DEFAULT GETUTCDATE(),
    CONSTRAINT PK_{table} PRIMARY KEY (year, zip5)
);
"""

# --- Databricks DDL (fee table) ---
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

# --- Databricks DDL (zip table) ---
_DATABRICKS_CREATE_ZIP_TABLE = """
CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (
    year          INT    NOT NULL,
    zip5          STRING NOT NULL,
    state_abbr    STRING,
    imported_at   TIMESTAMP DEFAULT current_timestamp()
)
"""

_CHUNK_SIZE = 1000


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

    conn = pyodbc.connect(conn_str, timeout=15)
    try:
        conn.fast_executemany = True
    except AttributeError:
        pass  # Older pyodbc versions don't support this -- bulk inserts still work, just slower
    return conn


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


def ensure_zip_table_exists(conn, db_type, table_name, schema="dbo"):
    """Create the rural_zips table if it doesn't exist."""
    cursor = conn.cursor()
    if db_type == "sqlserver":
        ddl = _MSSQL_CREATE_ZIP_TABLE.format(schema=schema, table=table_name)
    else:
        ddl = _DATABRICKS_CREATE_ZIP_TABLE.format(schema=schema, table=table_name)
    cursor.execute(ddl)
    conn.commit()
    cursor.close()


def publish_records(conn, db_type, records, table_name, mode, schema="dbo",
                    progress_callback=None):
    """
    Push records to the remote table.

    mode: "merge" or "replace"

    For "replace": delete rows where (state_abbr, year) matches any in the batch,
    then bulk insert.
    For "merge": upsert on (hcpcs_code, state_abbr, year, modifier).

    progress_callback: optional callable(rows_done, total_rows) invoked after
    each chunk is committed.

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
            _replace_sqlserver(conn, normalised, table_name, schema,
                               progress_callback=progress_callback)
        else:
            _merge_sqlserver(conn, normalised, table_name, schema,
                             progress_callback=progress_callback)
    else:
        if mode == "replace":
            _replace_databricks(conn, normalised, table_name, schema,
                                progress_callback=progress_callback)
        else:
            _merge_databricks(conn, normalised, table_name, schema,
                              progress_callback=progress_callback)

    return len(normalised)


def publish_zip_records(conn, db_type, records, table_name, mode, schema="dbo",
                        progress_callback=None):
    """
    Push rural ZIP records to the remote table.

    mode: "merge" or "replace"

    For "replace": delete rows where year matches any in the batch, then bulk insert.
    For "merge": upsert on (year, zip5).

    progress_callback: optional callable(rows_done, total_rows) invoked after
    each chunk is committed.

    Returns count of rows pushed.
    """
    if not records:
        return 0

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    normalised = []
    for r in records:
        raw_zip = str(r.get("zip5", "")).strip()
        if not raw_zip:
            continue  # skip records with empty zip codes
        normalised.append({
            "year": int(r.get("year", 0) or 0),
            "zip5": raw_zip.zfill(5),
            "state_abbr": r.get("state_abbr") or "",
            "imported_at": now,
        })

    if db_type == "sqlserver":
        if mode == "replace":
            _replace_zip_sqlserver(conn, normalised, table_name, schema,
                                   progress_callback=progress_callback)
        else:
            _merge_zip_sqlserver(conn, normalised, table_name, schema,
                                 progress_callback=progress_callback)
    else:
        if mode == "replace":
            _replace_zip_databricks(conn, normalised, table_name, schema,
                                    progress_callback=progress_callback)
        else:
            _merge_zip_databricks(conn, normalised, table_name, schema,
                                  progress_callback=progress_callback)

    return len(normalised)


# ----------------------------------------------------------------- SQL Server helpers --

def _replace_sqlserver(conn, records, table_name, schema, progress_callback=None):
    """Delete matching scope rows, then bulk insert."""
    cursor = conn.cursor()

    # Collect distinct (state_abbr, year) pairs in the batch
    scope = list({(r["state_abbr"], r["year"]) for r in records})

    # Delete existing rows for each (state_abbr, year) pair using parameterized queries
    delete_sql = f"DELETE FROM [{schema}].[{table_name}] WHERE state_abbr = ? AND year = ?"
    for state_abbr, year in scope:
        cursor.execute(delete_sql, (state_abbr, year))

    # Bulk insert in chunks
    _bulk_insert_sqlserver(cursor, records, table_name, schema,
                           progress_callback=progress_callback)
    conn.commit()
    cursor.close()


def _merge_sqlserver(conn, records, table_name, schema, progress_callback=None):
    """Upsert via staging temp table + single MERGE on (hcpcs_code, state_abbr, year, modifier).

    This is significantly faster than the previous per-row MERGE approach because it
    eliminates one network round-trip per record.  Instead:
      1. All rows are bulk-inserted into a session-local temp table via executemany
         (benefits from fast_executemany on the connection).
      2. A single T-SQL MERGE statement reconciles the staging table with the target.
    """
    cursor = conn.cursor()
    try:
        # Guard against a leftover staging table from a prior aborted run on this connection
        cursor.execute(
            "IF OBJECT_ID('tempdb..#hcpcs_staging') IS NOT NULL "
            "DROP TABLE #hcpcs_staging"
        )
        cursor.execute(
            "CREATE TABLE #hcpcs_staging ("
            "    hcpcs_code  NVARCHAR(20),  description NVARCHAR(500),"
            "    state_abbr  NVARCHAR(2),   year        INT,"
            "    allowable   FLOAT,         modifier    NVARCHAR(10),"
            "    data_source NVARCHAR(100), imported_at NVARCHAR(30)"
            ")"
        )

        # Bulk-insert all records into staging (benefits from fast_executemany)
        insert_sql = (
            "INSERT INTO #hcpcs_staging "
            "(hcpcs_code, description, state_abbr, year, allowable, modifier, data_source, imported_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        total = len(records)
        for i in range(0, total, _CHUNK_SIZE):
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
            if progress_callback:
                progress_callback(min(i + _CHUNK_SIZE, total), total)

        # Single MERGE from staging into target — one round-trip regardless of dataset size
        cursor.execute(f"""
            MERGE [{schema}].[{table_name}] AS target
            USING #hcpcs_staging AS source
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
        """)
        conn.commit()
    finally:
        try:
            cursor.execute(
                "IF OBJECT_ID('tempdb..#hcpcs_staging') IS NOT NULL "
                "DROP TABLE #hcpcs_staging"
            )
        except Exception:
            pass
        cursor.close()


def _bulk_insert_sqlserver(cursor, records, table_name, schema,
                           progress_callback=None):
    insert_sql = (
        f"INSERT INTO [{schema}].[{table_name}] "
        "(hcpcs_code, description, state_abbr, year, allowable, modifier, data_source, imported_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    total = len(records)
    for i in range(0, total, _CHUNK_SIZE):
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
        if progress_callback:
            progress_callback(min(i + _CHUNK_SIZE, total), total)


# ----------------------------------------------------------------- Databricks helpers --

def _replace_databricks(conn, records, table_name, schema, progress_callback=None):
    """Delete matching scope rows, then bulk insert."""
    cursor = conn.cursor()

    scope = list({(r["state_abbr"], r["year"]) for r in records})
    # Use parameterized individual deletes per (state_abbr, year) pair
    delete_sql = f"DELETE FROM `{schema}`.`{table_name}` WHERE state_abbr = ? AND year = ?"
    for state_abbr, year in scope:
        cursor.execute(delete_sql, (state_abbr, year))

    _bulk_insert_databricks(cursor, records, table_name, schema,
                            progress_callback=progress_callback)
    conn.commit()
    cursor.close()


def _merge_databricks(conn, records, table_name, schema, progress_callback=None):
    """Upsert via staging Delta table + single MERGE INTO on (hcpcs_code, state_abbr, year, modifier).

    A temporary Delta table is created, all rows are bulk-inserted via executemany, and a
    single MERGE INTO reconciles the staging data with the target table.  The staging table
    is dropped in a finally block regardless of success or failure.
    """
    cursor = conn.cursor()
    staging = f"_staging_{table_name}"
    try:
        cursor.execute(f"""
            CREATE OR REPLACE TABLE `{schema}`.`{staging}` (
                hcpcs_code STRING,
                description STRING,
                state_abbr STRING,
                year INT,
                allowable DOUBLE,
                modifier STRING,
                data_source STRING,
                imported_at STRING
            ) USING DELTA
        """)

        insert_sql = (
            f"INSERT INTO `{schema}`.`{staging}` "
            "(hcpcs_code, description, state_abbr, year, allowable, modifier, data_source, imported_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        total = len(records)
        for i in range(0, total, _CHUNK_SIZE):
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
            if progress_callback:
                progress_callback(min(i + _CHUNK_SIZE, total), total)

        cursor.execute(f"""
            MERGE INTO `{schema}`.`{table_name}` AS target
            USING `{schema}`.`{staging}` AS source
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
        """)
        conn.commit()
    finally:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{schema}`.`{staging}`")
        except Exception:
            pass
        cursor.close()


def _bulk_insert_databricks(cursor, records, table_name, schema,
                            progress_callback=None):
    insert_sql = (
        f"INSERT INTO `{schema}`.`{table_name}` "
        "(hcpcs_code, description, state_abbr, year, allowable, modifier, data_source, imported_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    total = len(records)
    for i in range(0, total, _CHUNK_SIZE):
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
        if progress_callback:
            progress_callback(min(i + _CHUNK_SIZE, total), total)


# ----------------------------------------------------------- ZIP table helpers --

def _replace_zip_sqlserver(conn, records, table_name, schema,
                           progress_callback=None):
    cursor = conn.cursor()
    scope = list({r["year"] for r in records})
    delete_sql = f"DELETE FROM [{schema}].[{table_name}] WHERE year = ?"
    for year in scope:
        cursor.execute(delete_sql, (year,))
    _bulk_insert_zip_sqlserver(cursor, records, table_name, schema,
                               progress_callback=progress_callback)
    conn.commit()
    cursor.close()


def _merge_zip_sqlserver(conn, records, table_name, schema,
                         progress_callback=None):
    """Upsert via staging temp table + single MERGE on (year, zip5)."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            "IF OBJECT_ID('tempdb..#zip_staging') IS NOT NULL "
            "DROP TABLE #zip_staging"
        )
        cursor.execute(
            "CREATE TABLE #zip_staging ("
            "    year INT, zip5 NVARCHAR(5), state_abbr NVARCHAR(2), imported_at NVARCHAR(30)"
            ")"
        )

        insert_sql = (
            "INSERT INTO #zip_staging (year, zip5, state_abbr, imported_at) "
            "VALUES (?, ?, ?, ?)"
        )
        total = len(records)
        for i in range(0, total, _CHUNK_SIZE):
            chunk = records[i: i + _CHUNK_SIZE]
            cursor.executemany(
                insert_sql,
                [(r["year"], r["zip5"], r["state_abbr"], r["imported_at"]) for r in chunk],
            )
            if progress_callback:
                progress_callback(min(i + _CHUNK_SIZE, total), total)

        cursor.execute(f"""
            MERGE [{schema}].[{table_name}] AS target
            USING #zip_staging AS source
                ON (target.year = source.year AND target.zip5 = source.zip5)
            WHEN MATCHED THEN UPDATE SET
                target.state_abbr  = source.state_abbr,
                target.imported_at = source.imported_at
            WHEN NOT MATCHED THEN INSERT (year, zip5, state_abbr, imported_at)
            VALUES (source.year, source.zip5, source.state_abbr, source.imported_at);
        """)
        conn.commit()
    finally:
        try:
            cursor.execute(
                "IF OBJECT_ID('tempdb..#zip_staging') IS NOT NULL "
                "DROP TABLE #zip_staging"
            )
        except Exception:
            pass
        cursor.close()


def _bulk_insert_zip_sqlserver(cursor, records, table_name, schema,
                               progress_callback=None):
    insert_sql = (
        f"INSERT INTO [{schema}].[{table_name}] "
        "(year, zip5, state_abbr, imported_at) VALUES (?, ?, ?, ?)"
    )
    total = len(records)
    for i in range(0, total, _CHUNK_SIZE):
        chunk = records[i: i + _CHUNK_SIZE]
        cursor.executemany(
            insert_sql,
            [(r["year"], r["zip5"], r["state_abbr"], r["imported_at"])
             for r in chunk],
        )
        if progress_callback:
            progress_callback(min(i + _CHUNK_SIZE, total), total)


def _replace_zip_databricks(conn, records, table_name, schema,
                            progress_callback=None):
    cursor = conn.cursor()
    scope = list({r["year"] for r in records})
    delete_sql = f"DELETE FROM `{schema}`.`{table_name}` WHERE year = ?"
    for year in scope:
        cursor.execute(delete_sql, (year,))
    _bulk_insert_zip_databricks(cursor, records, table_name, schema,
                                progress_callback=progress_callback)
    conn.commit()
    cursor.close()


def _merge_zip_databricks(conn, records, table_name, schema,
                          progress_callback=None):
    """Upsert via staging Delta table + single MERGE INTO on (year, zip5)."""
    cursor = conn.cursor()
    staging = f"_staging_{table_name}"
    try:
        cursor.execute(f"""
            CREATE OR REPLACE TABLE `{schema}`.`{staging}` (
                year INT,
                zip5 STRING,
                state_abbr STRING,
                imported_at STRING
            ) USING DELTA
        """)

        insert_sql = (
            f"INSERT INTO `{schema}`.`{staging}` (year, zip5, state_abbr, imported_at) "
            "VALUES (?, ?, ?, ?)"
        )
        total = len(records)
        for i in range(0, total, _CHUNK_SIZE):
            chunk = records[i: i + _CHUNK_SIZE]
            cursor.executemany(
                insert_sql,
                [(r["year"], r["zip5"], r["state_abbr"], r["imported_at"]) for r in chunk],
            )
            if progress_callback:
                progress_callback(min(i + _CHUNK_SIZE, total), total)

        cursor.execute(f"""
            MERGE INTO `{schema}`.`{table_name}` AS target
            USING `{schema}`.`{staging}` AS source
                ON (target.year = source.year AND target.zip5 = source.zip5)
            WHEN MATCHED THEN UPDATE SET
                target.state_abbr  = source.state_abbr,
                target.imported_at = source.imported_at
            WHEN NOT MATCHED THEN INSERT (year, zip5, state_abbr, imported_at)
            VALUES (source.year, source.zip5, source.state_abbr, source.imported_at)
        """)
        conn.commit()
    finally:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{schema}`.`{staging}`")
        except Exception:
            pass
        cursor.close()


def _bulk_insert_zip_databricks(cursor, records, table_name, schema,
                                progress_callback=None):
    insert_sql = (
        f"INSERT INTO `{schema}`.`{table_name}` "
        "(year, zip5, state_abbr, imported_at) VALUES (?, ?, ?, ?)"
    )
    total = len(records)
    for i in range(0, total, _CHUNK_SIZE):
        chunk = records[i: i + _CHUNK_SIZE]
        cursor.executemany(
            insert_sql,
            [(r["year"], r["zip5"], r["state_abbr"], r["imported_at"])
             for r in chunk],
        )
        if progress_callback:
            progress_callback(min(i + _CHUNK_SIZE, total), total)
