import zipfile

import pytest
from PyQt6.QtWidgets import QApplication

_has_docx = True
try:
    import docx  # noqa: F401
except ImportError:
    _has_docx = False


@pytest.mark.skipif(not _has_docx, reason="python-docx not installed")
def test_generate_purchase_document_docx(tmp_path):
    from core.document_generator import generate_purchase_document_docx

    out = tmp_path / "generated.docx"
    generate_purchase_document_docx(
        out,
        {
            "veteran_last_name": "Smith",
            "last4": "1234",
            "consult_date": "2026-04-16",
            "deliver_to": "Veteran",
            "vendor": "Acme Medical",
            "comments": "Call patient before delivery",
            "items": [
                {"hcpcs_code": "L5301", "quantity": 2, "cost": 100.0, "description": "BK prosthesis"},
            ],
        },
    )

    assert out.exists()
    with zipfile.ZipFile(out, "r") as zf:
        xml = zf.read("word/document.xml").decode("utf-8")
    assert "VA HCPCS Worksheet" in xml
    assert "Smith" in xml
    assert "Acme Medical" in xml
    assert "PO #" not in xml


def test_generate_document_dialog_adds_custom_item_and_can_save(monkeypatch):
    from ui.generate_document_dialog import GenerateDocumentDialog

    app = QApplication.instance() or QApplication([])
    saved = []
    monkeypatch.setattr(
        "ui.generate_document_dialog.save_custom_code",
        lambda code, description, price: saved.append((code, description, price)),
    )

    dlg = GenerateDocumentDialog(purchase_items=[])
    dlg.custom_code_edit.setText("SHIP01")
    dlg.custom_qty_edit.setText("2")
    dlg.custom_price_edit.setText("14.95")
    dlg.custom_description_edit.setText("Shipping")
    dlg.custom_save_checkbox.setChecked(True)
    dlg._add_custom_item()

    assert dlg.items_table.rowCount() == 1
    assert dlg.items_table.item(0, 0).text() == "SHIP01"
    assert dlg.items_table.item(0, 1).text() == "2"
    assert dlg.items_table.item(0, 2).text() == "14.95"
    assert dlg.items_table.item(0, 3).text() == "Shipping"
    assert saved == [("SHIP01", "Shipping", 14.95)]
    app.processEvents()
