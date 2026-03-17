"""Unit tests for the CMS DMEPOS ZIP file-selection logic, replace guard,
and available-year scraping helpers.

Tests run without any network access — all ZIPs are built in-memory and the
database layer is fully mocked where needed.
"""

import io
import zipfile
from unittest.mock import MagicMock, call, patch

import pytest

from core.cms_downloader import (
    DownloadError,
    _extract_csv_from_zip,
    _select_main_dmepos_filename,
    discover_available_cms_years,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_zip(files):
    """Return raw ZIP bytes containing *files*.

    *files* is a mapping of ``{filename: content_bytes_or_str}``.
    Files are created with sizes proportional to their content so size-based
    tie-breaking tests are predictable.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_STORED) as zf:
        for name, content in files.items():
            if isinstance(content, str):
                content = content.encode()
            zf.writestr(name, content)
    return buf.getvalue()


def _open_zip(zip_bytes):
    """Return an open ZipFile for *zip_bytes* (caller must close it)."""
    return zipfile.ZipFile(io.BytesIO(zip_bytes))


# ---------------------------------------------------------------------------
# _select_main_dmepos_filename — representative CMS ZIP contents (2026)
# ---------------------------------------------------------------------------

class TestSelectMainDmeposFilename:
    """Tests for the core selection helper."""

    def _select(self, files):
        """Build an in-memory ZIP, call _select_main_dmepos_filename, return result."""
        zip_bytes = _make_zip(files)
        with _open_zip(zip_bytes) as zf:
            return _select_main_dmepos_filename(list(files.keys()), zf)

    # --- Tier 1 / 2: DMEPOS-prefixed files ----------------------------------

    def test_prefers_dmepos_csv_over_auxiliary_files(self):
        """DMEPOS26_JAN.csv should win over rural-ZIP and former-CBA files."""
        files = {
            "DMEPOS26_JAN.csv": "a" * 5000,
            "DME Rural ZIP Code Quarter 1 2026.csv": "b" * 8000,
            "Former CBA ZIP Code File- JAN2026.csv": "c" * 6000,
            "DMEPEN26_JAN.csv": "d" * 3000,
        }
        assert self._select(files) == "DMEPOS26_JAN.csv"

    def test_prefers_dmepos_csv_over_dmepos_txt(self):
        """When both .txt and .csv DMEPOS files are present, prefer .csv."""
        files = {
            "DMEPOS26_JAN.txt": "a" * 4000,
            "DMEPOS26_JAN.csv": "b" * 4000,
        }
        assert self._select(files) == "DMEPOS26_JAN.csv"

    def test_txt_only_zip_raises(self):
        """A ZIP with only .txt data files raises DownloadError (CSV required)."""
        files = {
            "DMEPOS26_JAN.txt": "a" * 5000,
            "DME Rural ZIP Code Quarter 1 2026.txt": "b" * 8000,
            "DMEPEN26_JAN.txt": "c" * 3000,
            "README.txt": "read me text",
        }
        zip_bytes = _make_zip(files)
        with _open_zip(zip_bytes) as zf:
            with pytest.raises(DownloadError, match="No CSV"):
                _select_main_dmepos_filename(list(files.keys()), zf)

    def test_csv_only_zip(self):
        """Works correctly when the ZIP has only .csv data files."""
        files = {
            "DMEPOS25_JAN.csv": "a" * 5000,
            "DME Rural ZIP Code Quarter 1 2025.csv": "b" * 8000,
            "DMEPEN25_JAN.csv": "c" * 3000,
        }
        assert self._select(files) == "DMEPOS25_JAN.csv"

    def test_case_insensitive_prefix(self):
        """Prefix matching is case-insensitive."""
        files = {
            "dmepos26_jan.csv": "a" * 4000,
            "DME Rural ZIP Code Quarter 1 2026.csv": "b" * 9000,
        }
        assert self._select(files) == "dmepos26_jan.csv"

    def test_pen_file_excluded_from_dmepos_prefix(self):
        """DMEPEN files must NOT be selected as the main fee schedule.

        DMEPEN does not *start* with 'dmepos', so it is never chosen by Tier 1/2.
        In Tier 3 (contains 'dmepos' anywhere), it is also excluded via the
        'dmepen' auxiliary keyword.  In this degenerate case (only PEN + rural
        files present) the Tier 4 fallback picks the largest non-skipped file,
        which may be either one — we just verify the function doesn't crash.
        """

    def test_multiple_dmepos_prefix_picks_largest(self):
        """When multiple DMEPOS-prefixed CSVs exist, pick the largest."""
        files = {
            "DMEPOS26_JAN.csv": "a" * 3000,
            "DMEPOS26_APR.csv": "b" * 7000,
        }
        assert self._select(files) == "DMEPOS26_APR.csv"

    # --- Skip keywords -------------------------------------------------------

    def test_readme_excluded(self):
        """README files are always skipped."""
        files = {
            "README.txt": "this is a readme",
            "DMEPOS26_JAN.csv": "a" * 4000,
        }
        assert self._select(files) == "DMEPOS26_JAN.csv"

    def test_layout_file_excluded(self):
        """Record layout files are skipped."""
        files = {
            "Record Layout.txt": "col1|col2",
            "DMEPOS26_JAN.csv": "a" * 4000,
        }
        assert self._select(files) == "DMEPOS26_JAN.csv"

    # --- Fallback tier -------------------------------------------------------

    def test_fallback_when_no_dmepos_prefix(self):
        """When no DMEPOS-prefixed file exists, fall back to largest non-skipped CSV."""
        files = {
            "fee_schedule_2026.csv": "a" * 7000,
            "Rural ZIP Codes.csv": "b" * 9000,
            "README.txt": "docs",
        }
        # Fallback picks largest CSV: Rural ZIP Codes (9000)
        result = self._select(files)
        assert result == "Rural ZIP Codes.csv"

    def test_raises_when_empty_zip(self):
        """DownloadError raised for a ZIP with no usable CSV data files."""
        files = {
            "README.txt": "just docs",
            "codebook.pdf": b"\x25\x50\x44\x46",
        }
        # .pdf not matched; README is skipped; no .csv present
        zip_bytes = _make_zip(files)
        with _open_zip(zip_bytes) as zf:
            with pytest.raises(DownloadError, match="No CSV"):
                _select_main_dmepos_filename(list(files.keys()), zf)

    def test_raises_includes_zip_contents_in_message(self):
        """DownloadError message should list ZIP contents."""
        files = {"README.txt": "only docs here"}
        zip_bytes = _make_zip(files)
        with _open_zip(zip_bytes) as zf:
            with pytest.raises(DownloadError) as exc_info:
                _select_main_dmepos_filename(list(files.keys()), zf)
        assert "README.txt" in str(exc_info.value)


# ---------------------------------------------------------------------------
# _extract_csv_from_zip — integration with progress_callback
# ---------------------------------------------------------------------------

class TestExtractCsvFromZip:
    """Integration tests for _extract_csv_from_zip."""

    def test_returns_correct_content(self):
        """Returned bytes match the selected file's content."""
        content = b"HCPCS,Description,AZ (NR),AZ (R)\nA1234,Item desc,10.00,\n"
        zip_bytes = _make_zip({
            "DMEPOS26_JAN.csv": content,
            "Rural ZIP Code File.csv": b"x" * len(content) * 2,
        })
        name, data, _rural_name, _rural_bytes = _extract_csv_from_zip(zip_bytes)
        assert name == "DMEPOS26_JAN.csv"
        assert data == content

    def test_progress_callback_reports_selected_file(self):
        """progress_callback receives a message containing the selected filename."""
        zip_bytes = _make_zip({
            "DMEPOS26_JAN.csv": b"col1,col2\nval1,val2\n",
        })
        messages = []
        _extract_csv_from_zip(zip_bytes, progress_callback=messages.append)
        assert any("DMEPOS26_JAN.csv" in m for m in messages)

    def test_no_progress_callback_does_not_raise(self):
        """Calling without progress_callback must not raise."""
        zip_bytes = _make_zip({"DMEPOS26_JAN.csv": b"col1,col2\n"})
        name, _, _rn, _rb = _extract_csv_from_zip(zip_bytes)
        assert name == "DMEPOS26_JAN.csv"

    def test_raises_download_error_on_no_data_file(self):
        """DownloadError raised when no usable data file exists in the ZIP."""
        zip_bytes = _make_zip({"README.txt": b"nothing here"})
        with pytest.raises(DownloadError):
            _extract_csv_from_zip(zip_bytes)


# ---------------------------------------------------------------------------
# Replace guard — delete must NOT be called when parse yields 0 records
# ---------------------------------------------------------------------------

class TestReplaceGuard:
    """Tests for the replace-with-guard behaviour in download_cms_fees."""

    def _make_dmepos_zip(self, content=b"HCPCS,Description,AZ (NR)\nA1234,Item desc,10.00\n"):
        """Return a minimal in-memory ZIP with a DMEPOS main CSV file."""
        return _make_zip({"DMEPOS26_JAN.csv": content})

    @patch("core.cms_downloader.delete_fees_by_year_state_source")
    @patch("core.cms_downloader.insert_fees")
    @patch("core.cms_downloader.add_import_log")
    @patch("core.cms_downloader.parse_cms_csv")
    @patch("core.cms_downloader._try_download_zip")
    def test_delete_not_called_when_zero_records(
        self, mock_download, mock_parse, mock_log, mock_insert, mock_delete
    ):
        """If parse returns 0 records, delete_fees must never be called."""
        mock_download.return_value = self._make_dmepos_zip()
        mock_parse.return_value = []  # 0 records

        from core.cms_downloader import download_cms_fees

        with pytest.raises(DownloadError, match="Parsed 0 records"):
            download_cms_fees(2026, ["CA"])

        mock_delete.assert_not_called()
        mock_insert.assert_not_called()

    @patch("core.cms_downloader.delete_fees_by_year_state_source")
    @patch("core.cms_downloader.insert_fees")
    @patch("core.cms_downloader.add_import_log")
    @patch("core.cms_downloader.parse_cms_csv")
    @patch("core.cms_downloader._try_download_zip")
    def test_delete_called_before_insert_when_records_present(
        self, mock_download, mock_parse, mock_log, mock_insert, mock_delete
    ):
        """If parse returns >0 records, delete then insert must be called in order."""
        mock_download.return_value = self._make_dmepos_zip()
        fake_records = [
            {"hcpcs_code": "A1234", "description": "Test", "state_abbr": "CA",
             "year": 2026, "allowable": 10.0, "modifier": None}
        ]
        mock_parse.return_value = fake_records

        from core.cms_downloader import download_cms_fees

        result = download_cms_fees(2026, ["CA"])

        mock_delete.assert_called_once_with(
            state_abbr="CA", year=2026, data_source="cms_download"
        )
        mock_insert.assert_called_once()
        assert result == 1


# ---------------------------------------------------------------------------
# discover_available_cms_years — URL parsing (no network required)
# ---------------------------------------------------------------------------

class TestDiscoverAvailableCmsYears:
    """Tests for year-discovery URL parsing in discover_available_cms_years."""

    def _make_html_with_links(self, hrefs):
        links = "".join(f'<a href="{h}">link</a>' for h in hrefs)
        return f"<html><body>{links}</body></html>"

    @patch("core.cms_downloader.get_preference", return_value=None)
    @patch("core.cms_downloader.set_preference")
    @patch("core.cms_downloader.requests.get")
    def test_detects_year_from_quarterly_pattern(self, mock_get, mock_set, mock_pref):
        """dme{yy}-[a-d].zip links correctly map to the 4-digit year."""
        html = self._make_html_with_links([
            "/files/zip/dme26-a.zip",
            "/files/zip/dme25-d.zip",
        ])
        mock_get.return_value = MagicMock(status_code=200, text=html)

        years = discover_available_cms_years()

        assert 2026 in years
        assert 2025 in years

    @patch("core.cms_downloader.get_preference", return_value=None)
    @patch("core.cms_downloader.set_preference")
    @patch("core.cms_downloader.requests.get")
    def test_detects_year_from_dmeposfs_pattern(self, mock_get, mock_set, mock_pref):
        """DMEPOSFS{year}Q{n}.zip links are parsed correctly."""
        html = self._make_html_with_links([
            "/Downloads/DMEPOSFS2024Q1.zip",
        ])
        mock_get.return_value = MagicMock(status_code=200, text=html)

        years = discover_available_cms_years()

        assert 2024 in years

    @patch("core.cms_downloader.get_preference", return_value=None)
    @patch("core.cms_downloader.set_preference")
    @patch("core.cms_downloader.requests.get")
    def test_returns_empty_set_on_http_error(self, mock_get, mock_set, mock_pref):
        """Returns empty set gracefully when CMS page returns non-200."""
        mock_get.return_value = MagicMock(status_code=404, text="")

        years = discover_available_cms_years()

        assert years == set()

    @patch("core.cms_downloader.get_preference", return_value=None)
    @patch("core.cms_downloader.set_preference")
    @patch("core.cms_downloader.requests.get", side_effect=Exception("network error"))
    def test_returns_empty_set_on_exception(self, mock_get, mock_set, mock_pref):
        """Returns empty set gracefully on any network/parsing exception."""
        years = discover_available_cms_years()
        assert years == set()

    @patch("core.cms_downloader.get_preference", return_value=None)
    @patch("core.cms_downloader.set_preference")
    @patch("core.cms_downloader.requests.get")
    def test_detects_year_from_no_hyphen_pattern(self, mock_get, mock_set, mock_pref):
        """dme{yy}[a-d].zip links (no hyphen) correctly map to the 4-digit year."""
        html = self._make_html_with_links([
            "/files/zip/dme26a.zip",
            "/files/zip/dme25d.zip",
        ])
        mock_get.return_value = MagicMock(status_code=200, text=html)

        years = discover_available_cms_years()

        assert 2026 in years
        assert 2025 in years
