import csv
import io
import os
from datetime import datetime

from core.database import insert_fees, add_import_log

def _detect_delimiter(path):
    """Detect whether a file is pipe-delimited or comma-delimited.

    Reads the first non-empty line and counts pipes vs commas.
    Returns '|' or ',' (defaults to ',' if unclear).
    """
    try:
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line:
                    pipes = line.count("|")
                    commas = line.count(",")
                    return "|" if pipes > commas else ","
    except Exception:
        pass
    return ","

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
    """Parse a CMS DMEPOS fee schedule file and return record dicts.

    Handles both:
    - Comma-delimited .csv (older CMS format)
    - Pipe-delimited .txt (current CMS format as of 2025, e.g. dme25-a.zip)

    CMS DMEPOS files contain national data; state_abbr and year are supplied
    by the caller since they are not always present in the file itself.
    """
    delimiter = _detect_delimiter(path)
    records = []
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            # Skip rows where all values are empty (blank lines in TXT files)
            if not any(v and v.strip() for v in row.values()):
                continue

            norm = {
                k.strip().lower().replace(" ", "_").replace("-", "_"): (v or "").strip()
                for k, v in row.items()
                if k is not None
            }

            hcpcs_code = (
                norm.get("hcpcs_cd")
                or norm.get("hcpcs_code")
                or norm.get("hcpcs")
                or norm.get("proc_cd")
                or norm.get("procedure_code")
                or ""
            ).upper().strip()

            description = (
                norm.get("long_description")
                or norm.get("short_description")
                or norm.get("description")
                or norm.get("item_description")
                or norm.get("long_desc")
                or norm.get("short_desc")
                or ""
            )

            modifier = (
                norm.get("modifier")
                or norm.get("modifier_cd")
                or ""
            ).strip() or None

            # CMS DMEPOS fee columns — try all known column name variants
            # The pipe-delimited 2025 format uses different column names than older CSV
            allowable_raw = (
                norm.get("fee_schedule_price"]
                or norm.get("purchase_fee_amt")
                or norm.get("pur_fee_amt")
                or norm.get("capped_rental_1_month")
                or norm.get("rent_1_mo_amt")
                or norm.get("allowable")
                or norm.get("fee")
                or norm.get("payment_amount")
                or norm.get("pay_amt")
                or norm.get("fsc_price")
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