"""Tests for the new importer functionality:
- DMEPOS tilde-delimited TXT parser
- CSV preamble skipping and NR/R grid expansion
- Rural ZIP lookup
- Year fallback selection logic
"""

import io
import os
import sqlite3
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Tilde-delimited TXT parser
# ─────────────────────────────────────────────────────────────────────────────

from core.importer import parse_dmepos_tilde_txt, parse_dmepos_grid_csv, _find_csv_header_line


class TestTildeTxtParser:
    """Tests for parse_dmepos_tilde_txt."""

    _SAMPLE = textwrap.dedent("""\
        2025~A4216~  ~  ~J~OS~A~00~AZ     ~000000.35~000000.62~000000.53~000000.61~1~1~ ~Sterile water/saline, 10 ml
        2025~A4216~  ~  ~J~OS~A~00~CA     ~000000.40~000000.70~000000.60~000000.65~1~1~ ~Sterile water/saline, 10 ml
        2025~E0601~  ~  ~J~OS~A~00~AZ     ~000001.00~000002.00~000001.50~000001.80~0~1~ ~CPAP device
    """)

    def _write_tmp(self, content):
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8")
        tmp.write(content)
        tmp.close()
        return tmp.name

    def test_parses_records(self):
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path)
            assert len(records) == 3
        finally:
            os.unlink(path)

    def test_correct_hcpcs(self):
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path)
            hcpcs_codes = {r["hcpcs_code"] for r in records}
            assert "A4216" in hcpcs_codes
            assert "E0601" in hcpcs_codes
        finally:
            os.unlink(path)

    def test_correct_nr_amount(self):
        """allowable_nr should come from the updated NR fee column (col 11)."""
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path)
            az_a4216 = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert az_a4216["allowable_nr"] == pytest.approx(0.53, abs=1e-4)
        finally:
            os.unlink(path)

    def test_correct_r_amount_when_rural_indicator_1(self):
        """allowable_r should be populated when rural indicator is '1'."""
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path)
            az_a4216 = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert az_a4216["allowable_r"] == pytest.approx(0.61, abs=1e-4)
        finally:
            os.unlink(path)

    def test_r_amount_none_when_rural_indicator_0(self):
        """allowable_r should be None when rural indicator is '0'."""
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path)
            az_e0601 = next(r for r in records if r["hcpcs_code"] == "E0601" and r["state_abbr"] == "AZ")
            assert az_e0601["allowable_r"] is None
        finally:
            os.unlink(path)

    def test_state_filter(self):
        """state_abbr filter limits results to requested state."""
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path, state_abbr="CA")
            assert all(r["state_abbr"] == "CA" for r in records)
            assert len(records) == 1
        finally:
            os.unlink(path)

    def test_year_from_line(self):
        """Year is extracted from column 0 of each line."""
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path)
            assert all(r["year"] == 2025 for r in records)
        finally:
            os.unlink(path)

    def test_year_override(self):
        """year parameter overrides the year embedded in the file."""
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path, year=2024)
            assert all(r["year"] == 2024 for r in records)
        finally:
            os.unlink(path)

    def test_description_extracted(self):
        path = self._write_tmp(self._SAMPLE)
        try:
            records = parse_dmepos_tilde_txt(path)
            az_a4216 = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert "Sterile water" in az_a4216["description"]
        finally:
            os.unlink(path)

    def test_empty_file_returns_empty_list(self):
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
        tmp.close()
        try:
            records = parse_dmepos_tilde_txt(tmp.name)
            assert records == []
        finally:
            os.unlink(tmp.name)


# ─────────────────────────────────────────────────────────────────────────────
# CSV preamble skipping
# ─────────────────────────────────────────────────────────────────────────────

_SAMPLE_CSV = textwrap.dedent("""\
    "Durable Medical Equipment,",,,,,,
    "Prosthetics, Orthotics, and Supplies",,,,,,
    (DMEPOS),,,,,,
    January 2025 Fee Schedule,,,,,,
    ,,,,,,
    HCPCS,Mod,Mod2,JURIS,CATG,AZ (NR),AZ (R),CA (NR),CA (R),Description
    A4216,,,,J,0.53,0.61,0.60,0.65,Sterile water/saline 10 ml
    E0601,,,,J,1.50,,2.00,,CPAP device
""")


class TestFindCsvHeaderLine:
    """Tests for _find_csv_header_line."""

    def test_finds_header_with_hcpcs_and_description(self):
        fp = io.StringIO(_SAMPLE_CSV)
        idx, line = _find_csv_header_line(fp)
        assert idx == 5  # 0-indexed; preamble is 5 lines
        assert "HCPCS" in line
        assert "Description" in line

    def test_returns_none_when_no_header(self):
        fp = io.StringIO("no header here\njust data\n")
        idx, line = _find_csv_header_line(fp)
        assert idx is None
        assert line is None


class TestParseDmeposGridCsv:
    """Tests for parse_dmepos_grid_csv."""

    def _write_tmp(self, content):
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8")
        tmp.write(content)
        tmp.close()
        return tmp.name

    def test_skips_preamble_and_parses_records(self):
        path = self._write_tmp(_SAMPLE_CSV)
        try:
            records = parse_dmepos_grid_csv(path)
            hcpcs_codes = {r["hcpcs_code"] for r in records}
            assert "A4216" in hcpcs_codes
            assert "E0601" in hcpcs_codes
        finally:
            os.unlink(path)

    def test_nr_amount_correct(self):
        path = self._write_tmp(_SAMPLE_CSV)
        try:
            records = parse_dmepos_grid_csv(path)
            az_a4216 = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert az_a4216["allowable_nr"] == pytest.approx(0.53, abs=1e-4)
        finally:
            os.unlink(path)

    def test_r_amount_correct(self):
        path = self._write_tmp(_SAMPLE_CSV)
        try:
            records = parse_dmepos_grid_csv(path)
            az_a4216 = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert az_a4216["allowable_r"] == pytest.approx(0.61, abs=1e-4)
        finally:
            os.unlink(path)

    def test_r_amount_none_when_missing(self):
        """E0601 AZ has no R amount → allowable_r is None."""
        path = self._write_tmp(_SAMPLE_CSV)
        try:
            records = parse_dmepos_grid_csv(path)
            az_e0601 = next(r for r in records if r["hcpcs_code"] == "E0601" and r["state_abbr"] == "AZ")
            assert az_e0601["allowable_r"] is None
        finally:
            os.unlink(path)

    def test_state_filter(self):
        path = self._write_tmp(_SAMPLE_CSV)
        try:
            records = parse_dmepos_grid_csv(path, state_abbr="CA")
            assert all(r["state_abbr"] == "CA" for r in records)
            assert len(records) == 2  # A4216 and E0601 for CA
        finally:
            os.unlink(path)

    def test_description_present(self):
        path = self._write_tmp(_SAMPLE_CSV)
        try:
            records = parse_dmepos_grid_csv(path)
            az_a4216 = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert "Sterile" in az_a4216["description"]
        finally:
            os.unlink(path)


# ─────────────────────────────────────────────────────────────────────────────
# Rural ZIP lookup
# ─────────────────────────────────────────────────────────────────────────────

from core.database import (
    is_rural_zip, insert_rural_zips, delete_rural_zips_by_year,
    get_current_year_or_fallback, get_default_selected_years, init_db, DB_PATH,
)


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    """Provide an isolated SQLite database for each test."""
    db_file = tmp_path / "test_hcpcs.db"
    monkeypatch.setattr("core.database.DB_PATH", db_file)
    init_db()
    yield db_file


class TestRuralZipLookup:
    """Tests for is_rural_zip and insert_rural_zips."""

    def test_rural_zip_returns_true(self, tmp_db):
        insert_rural_zips([{"year": 2025, "zip5": "85001", "state_abbr": "AZ"}])
        assert is_rural_zip(2025, "85001") is True

    def test_non_rural_zip_returns_false(self, tmp_db):
        assert is_rural_zip(2025, "90210") is False

    def test_zip_different_year_not_found(self, tmp_db):
        insert_rural_zips([{"year": 2025, "zip5": "85001", "state_abbr": "AZ"}])
        assert is_rural_zip(2024, "85001") is False

    def test_zero_padded_zip(self, tmp_db):
        """ZIP codes shorter than 5 digits should be zero-padded."""
        insert_rural_zips([{"year": 2025, "zip5": "01234", "state_abbr": "MA"}])
        assert is_rural_zip(2025, "1234") is True  # caller passes without leading zero

    def test_delete_rural_zips_by_year(self, tmp_db):
        insert_rural_zips([
            {"year": 2025, "zip5": "85001", "state_abbr": "AZ"},
            {"year": 2024, "zip5": "85002", "state_abbr": "AZ"},
        ])
        delete_rural_zips_by_year(2025)
        assert is_rural_zip(2025, "85001") is False
        assert is_rural_zip(2024, "85002") is True  # different year unaffected

    def test_insert_replace_duplicate(self, tmp_db):
        """INSERT OR REPLACE should not raise on duplicate (year, zip5)."""
        insert_rural_zips([{"year": 2025, "zip5": "85001", "state_abbr": "AZ"}])
        insert_rural_zips([{"year": 2025, "zip5": "85001", "state_abbr": "AZ"}])  # no error
        assert is_rural_zip(2025, "85001") is True


# ─────────────────────────────────────────────────────────────────────────────
# Year fallback selection logic
# ─────────────────────────────────────────────────────────────────────────────

from core.database import get_available_years


class TestYearFallback:
    """Tests for get_current_year_or_fallback."""

    def _seed_years(self, tmp_db, years):
        """Insert minimal fee records for each year in *years*."""
        import sqlite3
        conn = sqlite3.connect(str(tmp_db))
        for y in years:
            conn.execute(
                "INSERT INTO hcpcs_fees (hcpcs_code, description, state_abbr, year) "
                "VALUES (?, ?, ?, ?)",
                ("A0000", "Test", "AZ", y),
            )
        conn.commit()
        conn.close()

    def test_returns_current_year_when_present(self, tmp_db):
        from datetime import date
        current = date.today().year
        self._seed_years(tmp_db, [current])
        result = get_current_year_or_fallback()
        assert result == current

    def test_falls_back_to_most_recent_year(self, tmp_db):
        from datetime import date
        current = date.today().year
        past_years = [current - 2, current - 1]
        self._seed_years(tmp_db, past_years)
        result = get_current_year_or_fallback()
        assert result == current - 1  # most recent year available

    def test_returns_none_when_db_empty(self, tmp_db):
        result = get_current_year_or_fallback()
        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# Default years helper
# ─────────────────────────────────────────────────────────────────────────────

from core.database import get_default_selected_years


class TestDefaultSelectedYears:
    def test_includes_current_year(self):
        from datetime import date
        current = date.today().year
        defaults = get_default_selected_years()
        assert current in defaults

    def test_at_most_4_years(self):
        defaults = get_default_selected_years()
        assert len(defaults) <= 4

    def test_all_within_supported_years(self):
        from core.cms_downloader import SUPPORTED_YEARS
        defaults = get_default_selected_years()
        for y in defaults:
            assert y in SUPPORTED_YEARS

    def test_descending_order(self):
        defaults = get_default_selected_years()
        assert defaults == sorted(defaults, reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# Effective allowable selection logic
# ─────────────────────────────────────────────────────────────────────────────

class TestEffectiveAllowableSelection:
    """Tests for the NR-vs-R selection logic used by the history dialog and main grid.

    The effective allowable is:
      - allowable_r  when rural=True and allowable_r is not None
      - allowable_nr otherwise (including fallback when allowable_r is None)
    """

    @staticmethod
    def _effective(allowable_nr, allowable_r, is_rural):
        """Mirror the selection logic used in _HcpcsHistoryDialog._build_primary_table."""
        if is_rural and allowable_r is not None:
            return allowable_r
        return allowable_nr

    def test_non_rural_returns_nr(self):
        assert self._effective(10.0, 12.0, False) == 10.0

    def test_rural_returns_r(self):
        assert self._effective(10.0, 12.0, True) == 12.0

    def test_rural_fallback_to_nr_when_r_is_none(self):
        """If rural but no R rate is available, fall back to NR."""
        assert self._effective(10.0, None, True) == 10.0

    def test_non_rural_r_present_but_ignored(self):
        """Non-rural should ignore an available R rate."""
        assert self._effective(5.0, 8.0, False) == 5.0

    def test_both_none_returns_none(self):
        assert self._effective(None, None, False) is None
        assert self._effective(None, None, True) is None

    def test_only_r_present_but_non_rural(self):
        """Non-rural with only R available → NR returned (None)."""
        assert self._effective(None, 12.0, False) is None

    def test_only_nr_present_rural(self):
        """Rural with only NR available → NR returned as fallback."""
        assert self._effective(9.0, None, True) == 9.0


# ─────────────────────────────────────────────────────────────────────────────
# CSV column header normalization (state NR/R variants)
# ─────────────────────────────────────────────────────────────────────────────

import textwrap as _textwrap
import tempfile as _tempfile


class TestGridCsvHeaderNormalization:
    """Tests that parse_dmepos_grid_csv handles alternate NR/R column header formats."""

    def _write_tmp(self, content):
        tmp = _tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8")
        tmp.write(content)
        tmp.close()
        return tmp.name

    def test_no_space_before_paren(self):
        """'AZ(NR)' and 'AZ(R)' (no space) should be recognized."""
        csv_content = _textwrap.dedent("""\
            HCPCS,Description,AZ(NR),AZ(R)
            A4216,Sterile water,0.53,0.61
        """)
        path = self._write_tmp(csv_content)
        try:
            records = parse_dmepos_grid_csv(path)
            az = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert az["allowable_nr"] == pytest.approx(0.53, abs=1e-4)
            assert az["allowable_r"] == pytest.approx(0.61, abs=1e-4)
        finally:
            os.unlink(path)

    def test_space_separated_without_parens(self):
        """'AZ NR' and 'AZ R' (space, no parens) should be recognized."""
        csv_content = _textwrap.dedent("""\
            HCPCS,Description,AZ NR,AZ R
            A4216,Sterile water,0.53,0.61
        """)
        path = self._write_tmp(csv_content)
        try:
            records = parse_dmepos_grid_csv(path)
            az = next(r for r in records if r["hcpcs_code"] == "A4216" and r["state_abbr"] == "AZ")
            assert az["allowable_nr"] == pytest.approx(0.53, abs=1e-4)
            assert az["allowable_r"] == pytest.approx(0.61, abs=1e-4)
        finally:
            os.unlink(path)

    def test_preamble_then_alternate_header(self):
        """Preamble skip + alternate column format combined."""
        csv_content = _textwrap.dedent("""\
            CMS DMEPOS Fee Schedule
            January 2026
            HCPCS,Description,AZ(NR),AZ(R),CA(NR),CA(R)
            E0601,CPAP device,1.50,1.80,2.00,2.40
        """)
        path = self._write_tmp(csv_content)
        try:
            records = parse_dmepos_grid_csv(path)
            hcpcs_set = {r["hcpcs_code"] for r in records}
            states_set = {r["state_abbr"] for r in records}
            assert "E0601" in hcpcs_set
            assert {"AZ", "CA"} == states_set
        finally:
            os.unlink(path)


# ─────────────────────────────────────────────────────────────────────────────
# Preference persistence
# ─────────────────────────────────────────────────────────────────────────────

from core.database import get_preference, set_preference


class TestPreferencePersistence:
    """Tests for get_preference / set_preference round-trip."""

    def test_set_and_get(self, tmp_db):
        set_preference("test_key", "hello")
        assert get_preference("test_key") == "hello"

    def test_default_when_missing(self, tmp_db):
        assert get_preference("nonexistent_key", "default_val") == "default_val"

    def test_default_none_when_missing_no_default(self, tmp_db):
        assert get_preference("nonexistent_key") is None

    def test_overwrite_existing_value(self, tmp_db):
        set_preference("k", "v1")
        set_preference("k", "v2")
        assert get_preference("k") == "v2"

    def test_filter_preferences_round_trip(self, tmp_db):
        """Simulate saving and restoring the full set of filter preferences."""
        prefs = {
            "filter_year": "2025",
            "filter_state": "AZ",
            "filter_zip": "86409",
            "filter_hcpcs": "E0601",
            "filter_keyword": "CPAP",
        }
        for key, val in prefs.items():
            set_preference(key, val)
        for key, expected in prefs.items():
            assert get_preference(key) == expected

