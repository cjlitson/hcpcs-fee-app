"""Tests for the staging-table merge helpers in core/sql_publisher.py.

Each merge helper should:
  1. Create a staging table / temp table.
  2. Bulk-insert all records via executemany in _CHUNK_SIZE chunks.
  3. Execute exactly ONE MERGE statement against the real target table.
  4. Commit, then drop the staging table in a finally block.
"""

import pytest
from unittest.mock import MagicMock, call, patch

from core.sql_publisher import (
    _CHUNK_SIZE,
    _merge_sqlserver,
    _merge_zip_sqlserver,
    _merge_databricks,
    _merge_zip_databricks,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_fee_records(n=3):
    return [
        {
            "hcpcs_code": f"A{i:04d}",
            "description": f"Item {i}",
            "state_abbr": "AZ",
            "year": 2025,
            "allowable": 1.00 + i,
            "modifier": "",
            "data_source": "test",
            "imported_at": "2025-01-01 00:00:00",
        }
        for i in range(n)
    ]


def _make_zip_records(n=3):
    return [
        {
            "year": 2025,
            "zip5": f"{85000 + i:05d}",
            "state_abbr": "AZ",
            "imported_at": "2025-01-01 00:00:00",
        }
        for i in range(n)
    ]


def _mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ─────────────────────────────────────────────────────────────────────────────
# _merge_sqlserver
# ─────────────────────────────────────────────────────────────────────────────

class TestMergeSqlserver:
    def test_creates_staging_table(self):
        conn, cursor = _mock_conn()
        _merge_sqlserver(conn, _make_fee_records(), "hcpcs_fees", "dbo")
        sql_calls = [c.args[0].strip() for c in cursor.execute.call_args_list]
        assert any("#hcpcs_staging" in s and "CREATE TABLE" in s for s in sql_calls)

    def test_drops_leftover_staging_before_create(self):
        conn, cursor = _mock_conn()
        _merge_sqlserver(conn, _make_fee_records(), "hcpcs_fees", "dbo")
        sql_calls = [c.args[0].strip() for c in cursor.execute.call_args_list]
        # The first statement must be the guard DROP (before CREATE)
        assert "DROP TABLE #hcpcs_staging" in sql_calls[0]

    def test_executemany_called_for_insert(self):
        conn, cursor = _mock_conn()
        records = _make_fee_records(5)
        _merge_sqlserver(conn, records, "hcpcs_fees", "dbo")
        assert cursor.executemany.called
        em_sql = cursor.executemany.call_args_list[0].args[0]
        assert "INSERT INTO #hcpcs_staging" in em_sql

    def test_single_merge_executed(self):
        conn, cursor = _mock_conn()
        _merge_sqlserver(conn, _make_fee_records(5), "hcpcs_fees", "dbo")
        merge_calls = [
            c for c in cursor.execute.call_args_list
            if "MERGE" in c.args[0]
        ]
        assert len(merge_calls) == 1
        assert "hcpcs_fees" in merge_calls[0].args[0]
        assert "#hcpcs_staging" in merge_calls[0].args[0]

    def test_chunk_splitting(self):
        """With more than _CHUNK_SIZE records, executemany is called once per chunk."""
        n = _CHUNK_SIZE + 1
        conn, cursor = _mock_conn()
        _merge_sqlserver(conn, _make_fee_records(n), "hcpcs_fees", "dbo")
        assert cursor.executemany.call_count == 2

    def test_progress_callback_called(self):
        records = _make_fee_records(5)
        conn, cursor = _mock_conn()
        progress = MagicMock()
        _merge_sqlserver(conn, records, "hcpcs_fees", "dbo", progress_callback=progress)
        assert progress.called
        last_call = progress.call_args_list[-1]
        done, total = last_call.args
        assert done == total == len(records)

    def test_conn_committed(self):
        conn, cursor = _mock_conn()
        _merge_sqlserver(conn, _make_fee_records(), "hcpcs_fees", "dbo")
        conn.commit.assert_called_once()

    def test_staging_dropped_in_finally(self):
        """Staging table is dropped even when the MERGE raises an exception."""
        conn, cursor = _mock_conn()
        # Make the MERGE call raise
        def _raise_on_merge(sql, *args, **kwargs):
            if "MERGE" in sql:
                raise RuntimeError("simulated MERGE failure")
        cursor.execute.side_effect = _raise_on_merge

        with pytest.raises(RuntimeError, match="simulated MERGE failure"):
            _merge_sqlserver(conn, _make_fee_records(), "hcpcs_fees", "dbo")

        drop_calls = [
            c for c in cursor.execute.call_args_list
            if "DROP TABLE #hcpcs_staging" in c.args[0]
        ]
        assert len(drop_calls) >= 1  # at least the finally-block cleanup

    def test_cursor_closed(self):
        conn, cursor = _mock_conn()
        _merge_sqlserver(conn, _make_fee_records(), "hcpcs_fees", "dbo")
        cursor.close.assert_called_once()

    def test_schema_and_table_in_merge(self):
        conn, cursor = _mock_conn()
        _merge_sqlserver(conn, _make_fee_records(), "my_table", "my_schema")
        merge_calls = [
            c for c in cursor.execute.call_args_list
            if "MERGE" in c.args[0]
        ]
        assert "[my_schema].[my_table]" in merge_calls[0].args[0]


# ─────────────────────────────────────────────────────────────────────────────
# _merge_zip_sqlserver
# ─────────────────────────────────────────────────────────────────────────────

class TestMergeZipSqlserver:
    def test_creates_staging_table(self):
        conn, cursor = _mock_conn()
        _merge_zip_sqlserver(conn, _make_zip_records(), "rural_zips", "dbo")
        sql_calls = [c.args[0].strip() for c in cursor.execute.call_args_list]
        assert any("#zip_staging" in s and "CREATE TABLE" in s for s in sql_calls)

    def test_single_merge_executed(self):
        conn, cursor = _mock_conn()
        _merge_zip_sqlserver(conn, _make_zip_records(5), "rural_zips", "dbo")
        merge_calls = [c for c in cursor.execute.call_args_list if "MERGE" in c.args[0]]
        assert len(merge_calls) == 1
        assert "rural_zips" in merge_calls[0].args[0]

    def test_executemany_called(self):
        conn, cursor = _mock_conn()
        _merge_zip_sqlserver(conn, _make_zip_records(5), "rural_zips", "dbo")
        assert cursor.executemany.called
        em_sql = cursor.executemany.call_args_list[0].args[0]
        assert "INSERT INTO #zip_staging" in em_sql

    def test_staging_dropped_on_error(self):
        conn, cursor = _mock_conn()

        def _raise_on_merge(sql, *args, **kwargs):
            if "MERGE" in sql:
                raise RuntimeError("boom")
        cursor.execute.side_effect = _raise_on_merge

        with pytest.raises(RuntimeError):
            _merge_zip_sqlserver(conn, _make_zip_records(), "rural_zips", "dbo")

        drop_calls = [
            c for c in cursor.execute.call_args_list
            if "DROP TABLE #zip_staging" in c.args[0]
        ]
        assert len(drop_calls) >= 1

    def test_progress_callback_called(self):
        records = _make_zip_records(5)
        conn, cursor = _mock_conn()
        progress = MagicMock()
        _merge_zip_sqlserver(conn, records, "rural_zips", "dbo", progress_callback=progress)
        assert progress.called

    def test_conn_committed(self):
        conn, cursor = _mock_conn()
        _merge_zip_sqlserver(conn, _make_zip_records(), "rural_zips", "dbo")
        conn.commit.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# _merge_databricks
# ─────────────────────────────────────────────────────────────────────────────

class TestMergeDatabricks:
    def test_creates_staging_table(self):
        conn, cursor = _mock_conn()
        _merge_databricks(conn, _make_fee_records(), "hcpcs_fees", "my_schema")
        sql_calls = [c.args[0].strip() for c in cursor.execute.call_args_list]
        assert any(
            "_staging_hcpcs_fees" in s and "CREATE OR REPLACE TABLE" in s
            for s in sql_calls
        )

    def test_executemany_inserts_into_staging(self):
        conn, cursor = _mock_conn()
        _merge_databricks(conn, _make_fee_records(5), "hcpcs_fees", "my_schema")
        assert cursor.executemany.called
        em_sql = cursor.executemany.call_args_list[0].args[0]
        assert "_staging_hcpcs_fees" in em_sql

    def test_single_merge_executed(self):
        conn, cursor = _mock_conn()
        _merge_databricks(conn, _make_fee_records(5), "hcpcs_fees", "my_schema")
        merge_calls = [c for c in cursor.execute.call_args_list if "MERGE INTO" in c.args[0]]
        assert len(merge_calls) == 1
        assert "`my_schema`.`hcpcs_fees`" in merge_calls[0].args[0]
        assert "`my_schema`.`_staging_hcpcs_fees`" in merge_calls[0].args[0]

    def test_staging_dropped_in_finally(self):
        conn, cursor = _mock_conn()

        def _raise_on_merge(sql, *args, **kwargs):
            if "MERGE INTO" in sql:
                raise RuntimeError("simulated failure")
        cursor.execute.side_effect = _raise_on_merge

        with pytest.raises(RuntimeError):
            _merge_databricks(conn, _make_fee_records(), "hcpcs_fees", "my_schema")

        drop_calls = [
            c for c in cursor.execute.call_args_list
            if "DROP TABLE IF EXISTS" in c.args[0] and "_staging_hcpcs_fees" in c.args[0]
        ]
        assert len(drop_calls) >= 1

    def test_staging_dropped_on_success(self):
        conn, cursor = _mock_conn()
        _merge_databricks(conn, _make_fee_records(), "hcpcs_fees", "my_schema")
        drop_calls = [
            c for c in cursor.execute.call_args_list
            if "DROP TABLE IF EXISTS" in c.args[0] and "_staging_hcpcs_fees" in c.args[0]
        ]
        assert len(drop_calls) >= 1

    def test_chunk_splitting(self):
        n = _CHUNK_SIZE + 1
        conn, cursor = _mock_conn()
        _merge_databricks(conn, _make_fee_records(n), "hcpcs_fees", "my_schema")
        assert cursor.executemany.call_count == 2

    def test_progress_callback_called(self):
        records = _make_fee_records(5)
        conn, cursor = _mock_conn()
        progress = MagicMock()
        _merge_databricks(conn, records, "hcpcs_fees", "my_schema", progress_callback=progress)
        assert progress.called

    def test_conn_committed(self):
        conn, cursor = _mock_conn()
        _merge_databricks(conn, _make_fee_records(), "hcpcs_fees", "my_schema")
        conn.commit.assert_called_once()

    def test_cursor_closed(self):
        conn, cursor = _mock_conn()
        _merge_databricks(conn, _make_fee_records(), "hcpcs_fees", "my_schema")
        cursor.close.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# _merge_zip_databricks
# ─────────────────────────────────────────────────────────────────────────────

class TestMergeZipDatabricks:
    def test_creates_staging_table(self):
        conn, cursor = _mock_conn()
        _merge_zip_databricks(conn, _make_zip_records(), "rural_zips", "my_schema")
        sql_calls = [c.args[0].strip() for c in cursor.execute.call_args_list]
        assert any(
            "_staging_rural_zips" in s and "CREATE OR REPLACE TABLE" in s
            for s in sql_calls
        )

    def test_single_merge_executed(self):
        conn, cursor = _mock_conn()
        _merge_zip_databricks(conn, _make_zip_records(5), "rural_zips", "my_schema")
        merge_calls = [c for c in cursor.execute.call_args_list if "MERGE INTO" in c.args[0]]
        assert len(merge_calls) == 1
        assert "`my_schema`.`rural_zips`" in merge_calls[0].args[0]

    def test_executemany_called(self):
        conn, cursor = _mock_conn()
        _merge_zip_databricks(conn, _make_zip_records(5), "rural_zips", "my_schema")
        assert cursor.executemany.called
        em_sql = cursor.executemany.call_args_list[0].args[0]
        assert "_staging_rural_zips" in em_sql

    def test_staging_dropped_in_finally(self):
        conn, cursor = _mock_conn()

        def _raise_on_merge(sql, *args, **kwargs):
            if "MERGE INTO" in sql:
                raise RuntimeError("boom")
        cursor.execute.side_effect = _raise_on_merge

        with pytest.raises(RuntimeError):
            _merge_zip_databricks(conn, _make_zip_records(), "rural_zips", "my_schema")

        drop_calls = [
            c for c in cursor.execute.call_args_list
            if "DROP TABLE IF EXISTS" in c.args[0] and "_staging_rural_zips" in c.args[0]
        ]
        assert len(drop_calls) >= 1

    def test_progress_callback_called(self):
        records = _make_zip_records(5)
        conn, cursor = _mock_conn()
        progress = MagicMock()
        _merge_zip_databricks(conn, records, "rural_zips", "my_schema", progress_callback=progress)
        assert progress.called

    def test_conn_committed(self):
        conn, cursor = _mock_conn()
        _merge_zip_databricks(conn, _make_zip_records(), "rural_zips", "my_schema")
        conn.commit.assert_called_once()
