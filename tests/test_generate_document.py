import zipfile

import pytest

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
    assert "HCPCS Worksheet" in xml
    assert "Smith" in xml
    assert "Acme Medical" in xml
    assert "PO #" not in xml
