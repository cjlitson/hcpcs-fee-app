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
        """Status bar should show contextual no-results feedback after an empty load."""
        from ui.main_window import MainWindow

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
            window.show()
            _pump_events(qapp)

        status = window.status_bar.currentMessage()
        assert status == "No results found for the current filters.", (
            f"Expected no-results contextual message in status bar after load, got: {status!r}"
        )
        window.close()

    def test_status_updated_message_for_non_empty_results(self, qapp, tmp_db):
        """Status bar should show a contextual updated message for non-empty results."""
        from ui.main_window import MainWindow

        with patch("ui.main_window.get_fees", return_value=[{"hcpcs_code": "E0601"}]):
            window = MainWindow()
            window.show()
            _pump_events(qapp)

        status = window.status_bar.currentMessage()
        assert status == "Results updated."
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
    def test_state_dropdown_sized_and_contextual_selection_bar_tracks_table_selection(self, qapp, tmp_db):
        from ui.main_window import MainWindow
        from PyQt6.QtWidgets import QTableWidgetItem

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
        window.show()
        qapp.processEvents()

        # State combo should be reasonably sized (not oversized at 280+)
        assert window.state_combo.minimumWidth() >= 100
        assert window.state_combo.maximumWidth() <= 220

        # Selection bar should be hidden until at least one row is selected
        assert window._add_selected_btn is not None
        assert window._clear_selection_btn is not None
        assert not window._selection_action_bar.isVisible()
        assert window._add_selected_btn.text() == "Add Selected"

        window.table.setRowCount(2)
        window.table.setItem(0, 0, QTableWidgetItem("L5301"))
        window.table.setItem(1, 0, QTableWidgetItem("E0601"))
        window.table.selectRow(0)
        qapp.processEvents()
        assert window._selection_action_bar.isVisible()
        assert window._add_selected_btn.isEnabled()
        assert window._selection_count_label.text() == "1 row selected"

        window.table.selectAll()
        qapp.processEvents()
        assert window._selection_count_label.text() == "2 rows selected"

        window._clear_selection_btn.click()
        qapp.processEvents()
        assert not window._selection_action_bar.isVisible()
        window.close()

    def test_contextual_add_selected_keeps_existing_add_behavior(self, qapp, tmp_db):
        from ui.main_window import MainWindow
        from PyQt6.QtWidgets import QTableWidgetItem

        with patch("core.database.get_fees", return_value=[]):
            window = MainWindow()
        window.show()
        qapp.processEvents()

        calls = []
        window._purchase_list_panel.is_context_ready = lambda: True
        window._purchase_list_panel.add_code = lambda code, desc="": calls.append((code, desc))

        window.table.setRowCount(1)
        window.table.setItem(0, 0, QTableWidgetItem("E0601"))
        window.table.setItem(0, 1, QTableWidgetItem("Oxygen concentrator"))
        window.table.selectRow(0)
        qapp.processEvents()

        assert window._selection_action_bar.isVisible()
        window._add_selected_btn.click()
        qapp.processEvents()

        assert calls == [("E0601", "Oxygen concentrator")]
        assert len(window.table.selectionModel().selectedRows()) == 0
        assert window._purchase_list_panel.isVisible()
        assert not window._selection_action_bar.isVisible()
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
        app_qss = window.styleSheet()
        assert "QFrame#appHeader" in app_qss
        assert "QFrame#filterCard { border-color:" in app_qss
        assert "QToolButton::menu-indicator" in app_qss
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

        assert panel.table.columnCount() == 6
        assert actions == ["Save Current Bundle", "Load / Manage Bundles…"]
        window.close()

    def test_bundle_picker_buttons_use_refreshed_roles(self, qapp, tmp_db):
        from PyQt6.QtWidgets import QPushButton, QToolButton
        from ui.purchase_list_dialog import BundlePickerDialog

        dlg = BundlePickerDialog()

        manage_btn = next((btn for btn in dlg.findChildren(QToolButton) if btn.text() == "Manage"), None)
        assert manage_btn is not None
        assert manage_btn.property("role") == "ghost"

        load_btn = next((btn for btn in dlg.findChildren(QPushButton) if btn.text() == "Load Bundle"), None)
        cancel_btn = next((btn for btn in dlg.findChildren(QPushButton) if btn.text() == "Cancel"), None)
        assert load_btn is not None and cancel_btn is not None
        assert load_btn.property("role") == "primary"
        assert cancel_btn.property("role") == "ghost"
        dlg.close()

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
        l5301_index = next((i for i, value in enumerate(suggestions) if value.startswith("L5301")), None)
        l1234_index = next((i for i, value in enumerate(suggestions) if value.startswith("L1234")), None)
        assert l5301_index is not None, suggestions
        assert l1234_index is not None, suggestions
        assert l5301_index < l1234_index
        panel.close()

    def test_quick_add_enter_uses_suggestion_when_partial_code_typed(self, qapp, tmp_db, monkeypatch):
        from ui.purchase_list_panel import PurchaseListPanel

        panel = PurchaseListPanel()
        monkeypatch.setattr(panel, "_is_ready_for_pricing", lambda: True)
        panel._quick_add_suggestions = {"L5301 — BK prosthesis": "L5301"}
        panel._quick_add_model.setStringList(["L5301 — BK prosthesis"])
        panel.quick_add_edit.setText("L53")

        with patch("ui.purchase_list_panel.get_fees", return_value=[{"hcpcs_code": "L5301", "description": "BK prosthesis"}]):
            with patch.object(panel, "add_code") as mock_add_code:
                panel._quick_add_from_input()

        mock_add_code.assert_called_once_with("L5301", "BK prosthesis")
        assert panel.quick_add_edit.text() == ""
        panel.close()

    def test_bundle_preview_hydrates_missing_descriptions(self, qapp, tmp_db):
        from ui.purchase_list_dialog import BundlePickerDialog

        bundle_payload = {
            "id": 42,
            "name": "Knee Bundle",
            "items": [{"hcpcs_code": "L5301", "description": "", "quantity": 2}],
        }

        with patch("ui.purchase_list_dialog.list_bundle_categories", return_value=[]):
            with patch("ui.purchase_list_dialog.list_bundles", return_value=[{"id": 42, "name": "Knee Bundle", "item_count": 1}]):
                with patch("ui.purchase_list_dialog.load_bundle", return_value=bundle_payload):
                    with patch("ui.purchase_list_dialog.get_fees", return_value=[{"hcpcs_code": "L5301", "description": "BK prosthesis"}]):
                        dlg = BundlePickerDialog()

        dlg._populate_preview(42)
        assert dlg.preview_table.item(0, 0).text() == "L5301"
        assert dlg.preview_table.item(0, 1).text() == "BK prosthesis"
        assert dlg.preview_table.item(0, 2).text() == "2"
        dlg.close()


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
