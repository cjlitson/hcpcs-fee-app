import csv
import os
from datetime import datetime

from core.database import insert_fees, add_import_log


def parse_visn_csv(path):
    """Parse a VISN-format CSV file and return a list of record dicts.

    Supports flexible column naming used across VISN 22 exports.
    """
    records = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize column names to lowercase with underscores
            norm = {
                k.strip().lower().replace(" ", "_").replace("-", "_"): (v or "").strip()
                for k, v in row.items()
            }

            hcpcs_code = (
                norm.get("hcpcs_code")
                or norm.get("hcpcs")
                or norm.get("code")
                or ""
            ).upper().strip()

            description = (
                norm.get("description")
                or norm.get("long_description")
                or norm.get("short_description")
                or norm.get("item_description")
                or norm.get("item_name")
                or ""
            )

            state_abbr = (
                norm.get("state_abbr")
                or norm.get("state")
                or norm.get("st")
                or ""
            ).upper().strip()

            year_raw = (
                norm.get("year")
                or norm.get("fy")
                or norm.get("calendar_year")
                or str(datetime.now().year)
            )
            try:
                year = int(str(year_raw).strip())
            except (ValueError, TypeError):
                year = datetime.now().year

            allowable_raw = (
                norm.get("allowable")
                or norm.get("fee")
                or norm.get("allowable_amount")
                or norm.get("fee_amount")
                or norm.get("purchase_fee_amt")
                or norm.get("rent_1_mo_amt")
                or ""
            )
            try:
                allowable = float(
                    str(allowable_raw).replace("$", "").replace(",", "").strip()
                )
            except (ValueError, TypeError):
                allowable = None

            modifier = (
                norm.get("modifier")
                or norm.get("modifier_code")
                or ""
            ).strip() or None

            if hcpcs_code:
                records.append(
                    {
                        "hcpcs_code": hcpcs_code,
                        "description": description,
                        "state_abbr": state_abbr,
                        "year": year,
                        "allowable": allowable,
                        "modifier": modifier,
                    }
                )
    return records


def parse_cms_csv(path, state_abbr, year):
    """Parse a CMS DMEPOS fee schedule CSV and return record dicts.

    CMS DMEPOS files contain national data identified by locality/state.
    """
    records = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            norm = {
                k.strip().lower().replace(" ", "_").replace("-", "_"): (v or "").strip()
                for k, v in row.items()
            }

            hcpcs_code = (
                norm.get("hcpcs_cd")
                or norm.get("hcpcs_code")
                or norm.get("hcpcs")
                or ""
            ).upper().strip()

            description = (
                norm.get("long_description")
                or norm.get("short_description")
                or norm.get("description")
                or norm.get("item_description")
                or ""
            )

            modifier = (
                norm.get("modifier")
                or norm.get("modifier_cd")
                or ""
            ).strip() or None

            # CMS DMEPOS fee columns (try common names)
            allowable_raw = (
                norm.get("fee_schedule_price")
                or norm.get("purchase_fee_amt")
                or norm.get("capped_rental_1_month")
                or norm.get("allowable")
                or norm.get("fee")
                or ""
            )
            try:
                allowable = float(
                    str(allowable_raw).replace("$", "").replace(",", "").strip()
                )
            except (ValueError, TypeError):
                allowable = None

            if hcpcs_code:
                records.append(
                    {
                        "hcpcs_code": hcpcs_code,
                        "description": description,
                        "state_abbr": state_abbr,
                        "year": year,
                        "allowable": allowable,
                        "modifier": modifier,
                    }
                )
    return records


def import_visn_csv(filepath, selected_states=None):
    """Parse and import a VISN-format CSV. Returns count of imported records."""
    records = parse_visn_csv(filepath)

    if selected_states:
        records = [r for r in records if r["state_abbr"] in selected_states]

    insert_fees(records, data_source="visn_csv")

    states_imported = sorted({r["state_abbr"] for r in records})
    add_import_log(
        file_name=os.path.basename(filepath),
        source="VISN CSV",
        record_count=len(records),
        states=", ".join(states_imported),
    )
    return len(records)


def import_cms_csv(filepath, state_abbr, year):
    """Parse and import a CMS DMEPOS CSV file. Returns count of imported records."""
    records = parse_cms_csv(filepath, state_abbr, year)
    insert_fees(records, data_source="cms_download")
    add_import_log(
        file_name=os.path.basename(filepath),
        source="CMS Download",
        record_count=len(records),
        states=state_abbr,
    )
    return len(records)
