from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_main_window_uses_main_export_dialog_module():
    source = (REPO_ROOT / "ui" / "main_window.py").read_text(encoding="utf-8")
    assert "from ui.main_export_dialog import MainExportDialog" in source
    assert "ui.export_dialog" not in source


def test_spec_includes_main_export_dialog_hidden_import():
    source = (REPO_ROOT / "hcpcs_fee_app.spec").read_text(encoding="utf-8")
    assert '"ui.main_export_dialog"' in source
    assert '"ui.export_dialog"' not in source


def test_main_export_dialog_removes_print_preview():
    source = (REPO_ROOT / "ui" / "main_export_dialog.py").read_text(encoding="utf-8")
    assert "Print Preview" not in source
    assert "def _show_preview" not in source
    assert "QTextDocument" not in source


def test_main_export_dialog_has_visible_checked_selector_style():
    source = (REPO_ROOT / "ui" / "main_export_dialog.py").read_text(encoding="utf-8")
    assert "QRadioButton:checked" in source
    assert "background: #003366;" in source


def test_user_guide_footer_uses_dark_mode_aware_style_class():
    source = (REPO_ROOT / "ui" / "main_window.py").read_text(encoding="utf-8")
    assert ".footer-note" in source
    assert ".footer-note p { color: #E6E6E6; }" in source
    assert '<div class="section footer-note">' in source


def test_user_guide_shortcuts_table_uses_dark_mode_aware_style_class():
    source = (REPO_ROOT / "ui" / "main_window.py").read_text(encoding="utf-8")
    assert ".shortcut-table th { background-color: #2F3E53; color: #E6E6E6; }" in source
    assert '<table class="shortcut-table">' in source
    assert '<tr style="background-color: #EEF2F7;">' not in source


def test_clear_filters_preserves_year_and_state_selection():
    source = (REPO_ROOT / "ui" / "main_window.py").read_text(encoding="utf-8")
    section = source.split("def _clear_filters(self):", 1)[1].split(
        "\n    # ------------------------------------------------- Filter persistence ---",
        1,
    )[0]
    assert "self.year_combo.setCurrentIndex(0)" not in section
    assert "self.state_combo.setCurrentIndex(0)" not in section
    assert "self.group_combo.setCurrentIndex(0)" in section
    assert "self.code_edit.clear()" in section
    assert "self.keyword_edit.clear()" in section
    assert "self.zip_edit.clear()" in section
