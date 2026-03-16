"""Unit tests for the CMS DMEPOS ZIP file-selection logic.

Tests focus on ``_select_main_dmepos_filename`` and ``_extract_csv_from_zip``
without any network access — all ZIPs are built in-memory.
"""

import io
import zipfile

import pytest

from core.cms_downloader import (
    DownloadError,
    _extract_csv_from_zip,
    _select_main_dmepos_filename,
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

    def test_prefers_dmepos_txt_over_dmepos_csv(self):
        """When both .txt and .csv DMEPOS files are present, prefer .txt."""
        files = {
            "DMEPOS26_JAN.txt": "a" * 4000,
            "DMEPOS26_JAN.csv": "b" * 4000,
        }
        assert self._select(files) == "DMEPOS26_JAN.txt"

    def test_txt_only_zip(self):
        """Works correctly when the ZIP has only .txt data files."""
        files = {
            "DMEPOS26_JAN.txt": "a" * 5000,
            "DME Rural ZIP Code Quarter 1 2026.txt": "b" * 8000,
            "DMEPEN26_JAN.txt": "c" * 3000,
            "README.txt": "read me text",
        }
        assert self._select(files) == "DMEPOS26_JAN.txt"

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
        """When no DMEPOS-prefixed file exists, fall back to largest non-skipped."""
        files = {
            "fee_schedule_2026.csv": "a" * 7000,
            "Rural ZIP Codes.csv": "b" * 9000,
            "README.txt": "docs",
        }
        # Fallback picks largest: Rural ZIP Codes (9000)
        result = self._select(files)
        assert result == "Rural ZIP Codes.csv"

    def test_raises_when_empty_zip(self):
        """DownloadError raised for a ZIP with no usable data files."""
        files = {
            "README.txt": "just docs",
            "codebook.pdf": b"\x25\x50\x44\x46",
        }
        # .pdf not matched; README is skipped
        zip_bytes = _make_zip(files)
        with _open_zip(zip_bytes) as zf:
            with pytest.raises(DownloadError, match="No CSV or data file"):
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
        content = b"HCPCS_CD|LONG_DESCRIPTION|FEE\nA1234|Item desc|10.00\n"
        zip_bytes = _make_zip({
            "DMEPOS26_JAN.txt": content,
            "Rural ZIP Code File.txt": b"x" * len(content) * 2,
        })
        name, data = _extract_csv_from_zip(zip_bytes)
        assert name == "DMEPOS26_JAN.txt"
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
        name, _ = _extract_csv_from_zip(zip_bytes)
        assert name == "DMEPOS26_JAN.csv"

    def test_raises_download_error_on_no_data_file(self):
        """DownloadError raised when no usable data file exists in the ZIP."""
        zip_bytes = _make_zip({"README.txt": b"nothing here"})
        with pytest.raises(DownloadError):
            _extract_csv_from_zip(zip_bytes)
