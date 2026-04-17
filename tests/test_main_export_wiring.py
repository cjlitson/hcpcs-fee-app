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
