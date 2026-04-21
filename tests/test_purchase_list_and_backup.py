import json
import sqlite3
import zipfile
from pathlib import Path

import pytest

_has_docx = True
try:
    import docx  # noqa: F401
except ImportError:
    _has_docx = False

_skip_no_docx = pytest.mark.skipif(not _has_docx, reason="python-docx not installed")


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    db_file = tmp_path / "test_hcpcs.db"
    monkeypatch.setattr("core.database.DB_PATH", db_file)
    from core.database import init_db
    init_db()
    return db_file


def test_bundle_save_list_load_rename_delete(tmp_db):
    from core.database import (
        delete_bundle,
        list_bundles,
        load_bundle,
        rename_bundle,
        save_bundle,
    )

    bundle_id = save_bundle(
        "Below Knee Prosthetic",
        [
            {"hcpcs_code": "L5301", "quantity": 1, "sort_order": 0},
            {"hcpcs_code": "L5620", "quantity": 2, "sort_order": 1},
        ],
    )
    bundles = list_bundles()
    assert len(bundles) == 1
    assert bundles[0]["item_count"] == 2
    loaded = load_bundle(bundle_id)
    assert loaded["name"] == "Below Knee Prosthetic"
    assert [i["hcpcs_code"] for i in loaded["items"]] == ["L5301", "L5620"]

    # Save with same name should replace items
    save_bundle("Below Knee Prosthetic", [{"hcpcs_code": "L5671", "quantity": 3, "sort_order": 0}])
    loaded2 = load_bundle(bundle_id)
    assert len(loaded2["items"]) == 1
    assert loaded2["items"][0]["hcpcs_code"] == "L5671"
    assert loaded2["items"][0]["quantity"] == 3

    rename_bundle(bundle_id, "BK Updated")
    assert load_bundle(bundle_id)["name"] == "BK Updated"
    delete_bundle(bundle_id)
    assert list_bundles() == []


def test_backup_create_inspect_restore(tmp_path, tmp_db, monkeypatch):
    from core.database import insert_fees, save_selected_states
    from core import backup

    insert_fees(
        [
            {
                "hcpcs_code": "L5301",
                "description": "BK prosthesis",
                "state_abbr": "CA",
                "year": 2026,
                "allowable": 100.0,
                "allowable_nr": 95.0,
                "allowable_r": 105.0,
                "modifier": None,
            }
        ],
        data_source="import",
    )
    save_selected_states([("CA", "California")])
    cfg_path = tmp_path / "hcpcs_app_config.json"
    cfg_path.write_text(json.dumps({"data_dir": str(tmp_path)}), encoding="utf-8")
    monkeypatch.setattr(backup, "list_saved_vendors", lambda: ["Acme Medical"])

    monkeypatch.setattr(backup, "_db_path", lambda: tmp_db)
    monkeypatch.setattr(backup, "_config_path", lambda: cfg_path)

    out_zip = tmp_path / "hcpcs_backup_test.zip"
    created = backup.create_backup(out_zip)
    assert Path(created).exists()

    manifest = backup.inspect_backup(created)
    assert manifest["record_counts"]["hcpcs_fees"] == 1
    assert "available_years" in manifest
    assert manifest["saved_vendor_count"] == 1

    with zipfile.ZipFile(created, "r") as zf:
        assert "hcpcs_fees.db" in zf.namelist()
        assert "manifest.json" in zf.namelist()

    # mutate DB then restore and verify original count is restored
    conn = sqlite3.connect(str(tmp_db))
    with conn:
        conn.execute("DELETE FROM hcpcs_fees")
    conn.close()
    restored = backup.restore_backup(created)
    assert restored["record_counts"]["hcpcs_fees"] == 1
    restored_db = Path(restored["database_path"])
    conn = sqlite3.connect(str(restored_db))
    count = conn.execute("SELECT COUNT(*) FROM hcpcs_fees").fetchone()[0]
    conn.close()
    assert count == 1


def test_purchase_list_csv_export(tmp_path):
    from core.exporter import export_purchase_list_to_csv

    out = tmp_path / "purchase.csv"
    export_purchase_list_to_csv(
        [
            {
                "hcpcs_code": "L5301",
                "description": "BK prosthesis",
                "quantity": 2,
                "unit_price": 100.0,
                "line_total": 200.0,
            },
            {
                "hcpcs_code": "L5620",
                "description": "Test liner",
                "quantity": 1,
                "unit_price": 50.0,
                "line_total": 50.0,
            },
        ],
        out,
        meta={"year": 2026, "state": "CA", "zip_code": "90210", "rural_status": "Non-Rural (NR)"},
    )
    txt = out.read_text(encoding="utf-8")
    assert "VA HCPCS Purchase List" in txt
    assert "Bundle" not in txt
    assert "Grand Total" in txt
    assert "200.00" in txt


@_skip_no_docx
def test_purchase_list_docx_export(tmp_path):
    from core.exporter import export_purchase_list_to_docx

    out = tmp_path / "purchase.docx"
    export_purchase_list_to_docx(
        [
            {
                "hcpcs_code": "L5301",
                "description": "BK prosthesis",
                "quantity": 2,
                "unit_price": 100.0,
                "line_total": 200.0,
            },
            {
                "hcpcs_code": "L5620",
                "description": "Test liner",
                "quantity": 1,
                "unit_price": 50.0,
                "line_total": 50.0,
            },
        ],
        out,
        meta={"year": 2026, "state": "CA", "zip_code": "90210", "rural_status": "Non-Rural (NR)"},
    )
    assert out.exists()
    with zipfile.ZipFile(out, "r") as zf:
        xml = zf.read("word/document.xml").decode("utf-8")
    assert "VA HCPCS Purchase List" in xml
    assert "PO #" not in xml
    assert "Quantity" in xml
    assert "Grand Total" in xml
    assert 'w:fill="003366"' in xml
    assert 'w:fill="EEF2F7"' in xml


def test_saved_vendor_persistence(monkeypatch):
    from core import vendor_store

    cfg = {}
    monkeypatch.setattr(vendor_store, "get_config_value", lambda key, default=None: cfg.get(key, default))
    monkeypatch.setattr(vendor_store, "set_config_value", lambda key, value: cfg.__setitem__(key, value))

    assert vendor_store.list_saved_vendors() == []
    vendor_store.save_vendor_name("Acme Medical")
    vendor_store.save_vendor_name("acme medical")
    vendor_store.save_vendor_name("  ")
    assert vendor_store.list_saved_vendors() == ["Acme Medical"]


def test_purchase_list_pdf_export(tmp_path):
    from core.exporter import export_purchase_list_to_pdf

    out = tmp_path / "purchase.pdf"
    export_purchase_list_to_pdf(
        [
            {
                "hcpcs_code": "L5301",
                "description": "BK prosthesis",
                "quantity": 2,
                "unit_price": 100.0,
                "line_total": 200.0,
            }
        ],
        out,
        meta={"year": 2026, "state": "CA", "zip_code": "90210", "rural_status": "Non-Rural (NR)"},
    )
    assert out.exists()
    assert out.read_bytes().startswith(b"%PDF")


def test_bundle_categories_crud_and_move(tmp_db):
    from core.database import (
        create_bundle_category,
        delete_bundle_category,
        list_bundle_categories,
        list_bundles,
        save_bundle,
        set_bundle_category,
    )

    cat_id = create_bundle_category("BK")
    assert any(c["id"] == cat_id and c["name"] == "BK" for c in list_bundle_categories())

    bundle_id = save_bundle("Test Bundle", [{"hcpcs_code": "L5301", "quantity": 1, "sort_order": 0}], category_id=cat_id)
    bundles = list_bundles()
    assert any(b["id"] == bundle_id and b["category_id"] == cat_id for b in bundles)

    set_bundle_category(bundle_id, None)
    bundles = list_bundles()
    assert any(b["id"] == bundle_id and b["category_id"] is None for b in bundles)

    delete_bundle_category(cat_id)
    assert all(c["id"] != cat_id for c in list_bundle_categories())


def test_custom_codes_are_saved_as_user_input_and_bundle_compatible(tmp_db):
    from core.database import (
        delete_custom_code,
        get_fees,
        list_custom_codes,
        load_bundle,
        save_bundle,
        save_custom_code,
    )

    save_custom_code("SHIP01", "Shipping", 19.99)
    save_custom_code("SHIP01", "Shipping and handling", 24.50)

    custom_rows = list_custom_codes()
    assert len(custom_rows) == 1
    assert custom_rows[0]["hcpcs_code"] == "SHIP01"
    assert custom_rows[0]["description"] == "Shipping and handling"
    assert custom_rows[0]["price"] == pytest.approx(24.50)
    assert custom_rows[0]["source"] == "UserInput"

    fee_rows = get_fees(hcpcs_code="SHIP01")
    exact = [r for r in fee_rows if (r.get("hcpcs_code") or "").upper() == "SHIP01"]
    assert len(exact) == 1
    assert exact[0]["data_source"] == "UserInput"
    assert exact[0]["allowable"] == pytest.approx(24.50)

    bundle_id = save_bundle("Custom Input Bundle", [{"hcpcs_code": "SHIP01", "quantity": 2, "sort_order": 0}])
    loaded = load_bundle(bundle_id)
    assert loaded is not None
    assert loaded["items"][0]["hcpcs_code"] == "SHIP01"
    assert loaded["items"][0]["quantity"] == 2

    assert delete_custom_code("SHIP01") == 1
    assert list_custom_codes() == []
