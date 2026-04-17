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
    from core.database import init_db, set_preference
    init_db()
    # Mark first-run as complete so the setup wizard never opens and blocks tests.
    set_preference("first_run_done", "1")
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

    def test_apply_filters_does_not_run_until_window_visible(self, qapp, tmp_db):
        """The initial deferred load must wait until the main window is shown."""
        from ui.main_window import MainWindow

        with patch("ui.main_window.get_fees", return_value=[]) as mock_gf:
            window = MainWindow()
            _pump_events(qapp, ms=200)
            assert mock_gf.call_count == 0, (
                "get_fees should not run before the main window is visible."
            )

            window.show()
            _pump_events(qapp, ms=200)
            assert mock_gf.call_count >= 1, (
                "get_fees should run after the main window is shown."
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


class TestUiAdjustments:
    def test_state_dropdown_is_wider_and_arrows_hidden_when_purchase_panel_hidden(self, qapp, tmp_db):
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
        window.show()
        qapp.processEvents()

        assert window.state_combo.minimumWidth() >= 280
        assert not window._add_btn.isVisible()
        assert not window._remove_btn.isVisible()

        window._set_purchase_list_panel_visible(True)
        qapp.processEvents()
        assert window._add_btn.isVisible()
        assert window._remove_btn.isVisible()
        window.close()

    def test_modernized_sections_exist_and_purchase_toggle_property_tracks_visibility(self, qapp, tmp_db):
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
        window.show()
        qapp.processEvents()

        assert window.centralWidget().objectName() == "appShell"
        assert window._update_bar_widget.objectName() == "updateBannerCard"
        assert window._toolbar_card.objectName() == "filterCard"
        assert window._purchase_btn.property("role") == "toggle"
        assert window._purchase_btn.property("active") == "false"

        window._set_purchase_list_panel_visible(True)
        qapp.processEvents()
        assert window._purchase_btn.property("active") == "true"
        window.close()

    def test_top_bar_uses_rural_pill_badge(self, qapp, tmp_db):
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            with patch("ui.main_window.is_rural_zip", return_value=True):
                window = MainWindow()
        window.show()
        window.zip_edit.setText("90210")
        qapp.processEvents()

        assert window.rural_label.objectName() == "ruralPill"
        assert window.rural_label.property("ruralState") in {"rural", "non_rural"}
        assert "90210" in window.rural_label.text()
        window.close()

    def test_purchase_panel_uses_bundles_menu_and_row_delete_column(self, qapp, tmp_db):
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
        panel = window._purchase_list_panel
        menu = panel._bundles_btn.menu()
        actions = [a.text() for a in menu.actions()]

        assert panel.table.columnCount() == 7
        assert actions == ["Save Current Bundle", "Load / Manage Bundles…"]
        window.close()


class TestPurchaseListPanelStartup:
    def test_refresh_does_not_run_before_required_widgets_exist(self, qapp, tmp_db, monkeypatch):
        from ui.purchase_list_panel import PurchaseListPanel

        calls_with_widget_state = []
        original_refresh = PurchaseListPanel.refresh_prices

        def _tracking_refresh(self):
            calls_with_widget_state.append(
                (hasattr(self, "table"), hasattr(self, "grand_total_label"))
            )
            return original_refresh(self)

        monkeypatch.setattr(PurchaseListPanel, "refresh_prices", _tracking_refresh)

        panel = PurchaseListPanel()
        qapp.processEvents()

        assert all(has_table and has_total for has_table, has_total in calls_with_widget_state)
        panel.close()

    def test_purchase_panel_breadcrumbs_are_logged_during_startup(self, qapp, tmp_db):
        from ui.main_window import MainWindow

        breadcrumbs = []

        def _capture_breadcrumb(self, message):
            breadcrumbs.append(message)

        with patch.object(MainWindow, "_write_startup_breadcrumb", new=_capture_breadcrumb):
            with patch("core.database.get_fees", return_value=[]):
                window = MainWindow()

        assert any("creating PurchaseListPanel" in msg for msg in breadcrumbs)
        assert any("created PurchaseListPanel" in msg for msg in breadcrumbs)
        window.close()

    def test_purchase_panel_failure_is_breadcrumbed_before_reraise(self, qapp, tmp_db):
        from ui.main_window import MainWindow

        breadcrumbs = []

        def _capture_breadcrumb(self, message):
            breadcrumbs.append(message)

        with patch.object(MainWindow, "_write_startup_breadcrumb", new=_capture_breadcrumb):
            with patch("ui.main_window.PurchaseListPanel", side_effect=RuntimeError("panel boom")):
                with pytest.raises(RuntimeError, match="Failed to initialize main window: panel boom"):
                    MainWindow()

        assert any("PurchaseListPanel creation failed: panel boom" in msg for msg in breadcrumbs)

    def test_quick_add_suggestions_prioritize_code_matches(self, qapp, tmp_db):
        from ui.purchase_list_panel import PurchaseListPanel

        panel = PurchaseListPanel()

        sample_records = [
            {"hcpcs_code": "A0001", "description": "Alpha code"},
            {"hcpcs_code": "L5301", "description": "Lower limb prosthesis"},
            {"hcpcs_code": "L1234", "description": "L-series example"},
        ]
        with patch("ui.purchase_list_panel.get_fees", return_value=sample_records):
            panel._update_quick_add_suggestions("L5")

        suggestions = panel._quick_add_model.stringList()
        assert suggestions
        assert suggestions[0].startswith("L5301")
        panel.close()


class TestPurchaseListContextRules:
    def test_purchase_list_requires_single_year_and_state(self, qapp, tmp_db):
        from core.database import insert_fees, save_selected_states
        from ui.main_window import MainWindow

        save_selected_states([("CA", "California")])
        insert_fees(
            [{
                "hcpcs_code": "L5301",
                "description": "BK prosthesis",
                "state_abbr": "CA",
                "year": 2026,
                "allowable": 100.0,
                "allowable_nr": 95.0,
                "allowable_r": 105.0,
                "modifier": None,
            }],
            data_source="import",
        )

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
        window.show()
        qapp.processEvents()

        panel = window._purchase_list_panel
        if window.year_combo.findData(2026) < 0:
            window.year_combo.addItem("2026", 2026)
        window.year_combo.setCurrentIndex(window.year_combo.findData(2026))
        panel.refresh_context_state()
        qapp.processEvents()
        assert "CMS 2026" in panel.pricing_context_label.text()
        assert not panel.is_context_ready()
        assert not panel.quick_add_edit.isEnabled()
        assert panel.selection_requirement_label.text()

        state_idx = window.state_combo.findData("CA")
        window.state_combo.setCurrentIndex(state_idx)
        qapp.processEvents()
        assert panel.is_context_ready()
        assert panel.quick_add_edit.isEnabled()
        assert not panel.selection_requirement_label.text()

        window.year_combo.setCurrentIndex(0)  # All Years
        qapp.processEvents()
        assert not panel.is_context_ready()
        assert "specific cms year" in panel.pricing_context_label.text().lower()
        assert not panel.quick_add_edit.isEnabled()
        window.close()

    def test_export_uses_main_export_dialog(self, qapp, tmp_db):
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()

        window._records = [{"hcpcs_code": "L5301"}]
        with patch("ui.main_window.MainExportDialog") as mock_dlg:
            instance = mock_dlg.return_value
            window._export()
        mock_dlg.assert_called_once()
        instance.exec.assert_called_once()
        window.close()
