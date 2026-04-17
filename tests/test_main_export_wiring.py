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
