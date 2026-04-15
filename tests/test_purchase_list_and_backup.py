import json
import sqlite3
import zipfile
from pathlib import Path

import pytest


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

    monkeypatch.setattr(backup, "_db_path", lambda: tmp_db)
    monkeypatch.setattr(backup, "_config_path", lambda: cfg_path)

    out_zip = tmp_path / "hcpcs_backup_test.zip"
    created = backup.create_backup(out_zip)
    assert Path(created).exists()

    manifest = backup.inspect_backup(created)
    assert manifest["record_counts"]["hcpcs_fees"] == 1
    assert "available_years" in manifest

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
            }
        ],
        out,
        meta={"year": 2026, "state": "CA", "zip_code": "90210", "rural_status": "Non-Rural (NR)"},
    )
    txt = out.read_text(encoding="utf-8")
    assert "VA HCPCS Purchase List" in txt
    assert "Grand Total" in txt
    assert "200.00" in txt
