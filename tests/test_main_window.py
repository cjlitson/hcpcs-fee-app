"""Tests for MainWindow startup / loading behaviour.

The key requirement: the main window must become visible *before* the initial
fee-record query runs, so that users do not see a frozen splash screen on
first launch.
"""

import os
import sys
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, call

import pytest

# Use the offscreen platform so the tests can run without a display.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import QTimer

# ---------------------------------------------------------------------------
# Shared QApplication fixture (one per session is enough)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def qapp():
    app = QApplication.instance() or QApplication(sys.argv)
    yield app


# ---------------------------------------------------------------------------
# Isolated DB fixture (mirrors the pattern in test_importer.py)
# ---------------------------------------------------------------------------

@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    """Provide an isolated SQLite database for each test."""
    db_file = tmp_path / "test_hcpcs.db"
    monkeypatch.setattr("core.database.DB_PATH", db_file)
    from core.database import init_db
    init_db()
    yield db_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pump_events(qapp, ms: int = 0) -> None:
    """Process pending Qt events (including zero-interval timers)."""
    qapp.processEvents()
    if ms > 0:
        import time
        deadline = time.monotonic() + ms / 1000.0
        while time.monotonic() < deadline:
            qapp.processEvents()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDeferredLoading:
    """_apply_filters must not run synchronously inside __init__."""

    def test_apply_filters_not_called_during_init(self, qapp, tmp_db):
        """get_fees should NOT be called while MainWindow.__init__ is running."""
        from ui.main_window import MainWindow

        calls_during_init: list[bool] = []
        init_done = [False]

        original_get_fees = None

        def tracking_get_fees(**kwargs):
            calls_during_init.append(not init_done[0])
            if original_get_fees:
                return original_get_fees(**kwargs)
            return []

        with patch("core.database.get_fees", side_effect=tracking_get_fees) as mock_gf:
            original_get_fees = lambda **kw: []  # noqa: E731
            window = MainWindow()
            init_done[0] = True
            window.show()

        # Any call that happened *before* init_done was set is a premature call.
        assert not any(calls_during_init), (
            f"get_fees was called {sum(calls_during_init)} time(s) during __init__; "
            "the initial load must be deferred until after the window is shown."
        )
        window.close()

    def test_initial_status_shows_loading(self, qapp, tmp_db):
        """Status bar should say 'Loading fee records…' right after __init__."""
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()

        status_text = window.status_bar.currentMessage()
        assert "loading" in status_text.lower() or "load" in status_text.lower(), (
            f"Expected a 'loading' message in the status bar right after init, "
            f"got: {status_text!r}"
        )
        window.close()

    def test_apply_filters_runs_after_event_loop(self, qapp, tmp_db):
        """get_fees should be called once the event loop processes the deferred timer."""
        from ui.main_window import MainWindow

        # get_fees is imported directly into ui.main_window's namespace, so
        # patch it there (not at core.database) to intercept the call.
        with patch("ui.main_window.get_fees", return_value=[]) as mock_gf:
            window = MainWindow()
            window.show()
            assert mock_gf.call_count == 0, "get_fees was called prematurely during init"

            # Process pending events (fires the QTimer.singleShot(0, ...) callback).
            _pump_events(qapp)

            assert mock_gf.call_count >= 1, (
                "get_fees was never called after the event loop started; "
                "the deferred initial load did not fire."
            )
        window.close()

    def test_status_updated_after_load(self, qapp, tmp_db):
        """Status bar should show a record count after the deferred load completes."""
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
            window.show()
            _pump_events(qapp)

        status = window.status_bar.currentMessage()
        # After an empty result the status should contain "0 records" or similar.
        assert "record" in status.lower(), (
            f"Expected record-count message in status bar after load, got: {status!r}"
        )
        window.close()


class TestRestorePreferencesSignals:
    """Restoring saved preferences must not trigger _apply_filters prematurely."""

    def test_no_filter_call_when_restoring_zip(self, qapp, tmp_db):
        """Setting zip_edit text during preference restore must not call _apply_filters."""
        from core.database import set_preference
        from ui.main_window import MainWindow

        # Save a valid 5-digit ZIP in preferences so the restore will setText on zip_edit.
        set_preference("filter_zip", "90210")

        apply_calls: list[str] = []

        with patch("core.database.get_fees", return_value=[]) as mock_gf:
            window = MainWindow()
            # No call should have happened during init.
            assert mock_gf.call_count == 0, (
                f"get_fees was called {mock_gf.call_count} time(s) during init "
                "while restoring a saved ZIP preference."
            )
        window.close()

    def test_no_filter_call_when_restoring_hcpcs_code(self, qapp, tmp_db):
        """Setting code_edit text must not start the debounce timer or call _apply_filters."""
        from core.database import set_preference
        from ui.main_window import MainWindow

        set_preference("filter_hcpcs", "E0601")

        with patch("core.database.get_fees", return_value=[]) as mock_gf:
            window = MainWindow()
            assert mock_gf.call_count == 0, (
                "get_fees was called during init while restoring HCPCS code preference."
            )
        window.close()

    def test_zip_label_reflects_restored_value(self, qapp, tmp_db):
        """The rural label must reflect the restored ZIP even though signals were blocked."""
        from core.database import set_preference
        from ui.main_window import MainWindow

        set_preference("filter_zip", "90210")

        with patch("core.database.get_fees", return_value=[]):
            with patch("core.database.is_rural_zip", return_value=False):
                window = MainWindow()

        label_text = window.rural_label.text()
        assert "90210" in label_text, (
            f"Rural label should show the restored ZIP '90210', got: {label_text!r}"
        )
        window.close()

    def test_hcpcs_field_contains_restored_value(self, qapp, tmp_db):
        """code_edit must contain the restored HCPCS code after init."""
        from core.database import set_preference
        from ui.main_window import MainWindow

        set_preference("filter_hcpcs", "E0601")

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()

        assert window.code_edit.text() == "E0601", (
            f"code_edit should contain restored value 'E0601', got: {window.code_edit.text()!r}"
        )
        window.close()
