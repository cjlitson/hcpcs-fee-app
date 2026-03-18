import csv
import os
import re as _re
from datetime import datetime

from core.database import insert_fees, add_import_log

# ---------------------------------------------------------------------------
# Delimiter detection
# ---------------------------------------------------------------------------

def _detect_delimiter(path):
    """Detect whether a file uses '~', '|', or ',' as delimiter.

    Scans up to the first 30 non-empty lines and returns the delimiter found
    on the first line that contains at least 3 occurrences of a candidate
    delimiter.  This skips short preamble/metadata lines (which often have
    zero or one delimiter) and reliably identifies data lines.

    Returns '~', '|', or ',' (defaults to ',' if unclear).
    """
    try:
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
            for i, line in enumerate(f):
                if i >= 30:
                    break
                line = line.strip()
                if not line:
                    continue
                tildes = line.count("~")
                pipes = line.count("|")
                commas = line.count(",")
                # Require at least 3 occurrences so we don't misidentify a stray
                # character in a short metadata / title line.
                if tildes >= 3 and tildes >= pipes and tildes >= commas:
                    return "~"
                if pipes >= 3 and pipes > commas:
                    return "|"
                if commas >= 3:
                    return ","
    except Exception:
        pass
    return ","


# ---------------------------------------------------------------------------
# CMS DMEPOS tilde-delimited TXT parser
# ---------------------------------------------------------------------------

# Expected column positions in the CMS tilde-delimited format (0-based).
# Format sample (from CMS documentation/actual files):
#   2025~A4216~  ~  ~J~OS~A~00~AZ     ~000000.35~000000.62~000000.53~000000.61~0~1~ ~Sterile water/saline, 10 ml
# Positions:
#  0  year
#  1  hcpcs_code
#  2  modifier_1
#  3  modifier_2
#  4  jurisdiction  (JURIS)
#  5  category      (CATG)
#  6  pricing_indicator
#  7  multiple_pricing_indicator
#  8  state_abbr    (may be padded with spaces, e.g. "AZ     ")
#  9  floor_fee     (non-rural / NR base-year fee, col A)
# 10  ceiling_fee   (non-rural / NR ceiling)
# 11  updated_fee   (non-rural / NR updated fee schedule — the "current" NR amount)
# 12  rural_fee     (rural updated fee schedule — R amount)
# 13  rural_indicator  ('1' = rural available, '0' = not rural)
# 14  facility_indicator
# 15  (reserved/blank)
# 16  description

_TXT_YEAR = 0
_TXT_HCPCS = 1
_TXT_MOD1 = 2
_TXT_MOD2 = 3
_TXT_STATE = 8
_TXT_NR_UPDATED = 11   # updated non-rural fee schedule amount
_TXT_R_UPDATED = 12    # rural updated fee schedule amount
_TXT_RURAL_IND = 13    # rural indicator: '1' means rural rate is provided
_TXT_DESC = 16


def _parse_amount(raw):
    """Parse a numeric amount string, returning float or None."""
    try:
        cleaned = str(raw).replace("$", "").replace(",", "").strip()
        val = float(cleaned)
        return val if val != 0.0 else None
    except (ValueError, TypeError):
        return None


def parse_dmepos_tilde_txt(path, state_abbr=None, year=None):
    """Parse a CMS DMEPOS tilde-delimited TXT file (no header row).

    Returns a list of record dicts suitable for ``insert_fees()``.

    Args:
        path: Path to the tilde-delimited file.
        state_abbr: If provided, only return records for this state.
        year: If provided, use as the year for all records (overrides col 0).
    """
    records = []
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n\r")
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("~")]
            if len(parts) < 12:
                continue

            # Extract year from the file unless overridden
            rec_year = year
            if rec_year is None:
                try:
                    rec_year = int(parts[_TXT_YEAR])
                except (ValueError, IndexError):
                    continue

            hcpcs = parts[_TXT_HCPCS].upper().strip() if len(parts) > _TXT_HCPCS else ""
            if not hcpcs:
                continue

            rec_state = parts[_TXT_STATE].upper().strip() if len(parts) > _TXT_STATE else ""
            if not rec_state:
                continue

            # Filter by requested state if given
            if state_abbr and rec_state.upper() != state_abbr.upper():
                continue

            mod1 = parts[_TXT_MOD1].strip() if len(parts) > _TXT_MOD1 else ""
            mod2 = parts[_TXT_MOD2].strip() if len(parts) > _TXT_MOD2 else ""
            modifier = None
            if mod1 and mod2:
                modifier = f"{mod1},{mod2}"
            elif mod1:
                modifier = mod1
            elif mod2:
                modifier = mod2

            nr_raw = parts[_TXT_NR_UPDATED] if len(parts) > _TXT_NR_UPDATED else ""
            r_raw = parts[_TXT_R_UPDATED] if len(parts) > _TXT_R_UPDATED else ""
            rural_ind = parts[_TXT_RURAL_IND].strip() if len(parts) > _TXT_RURAL_IND else "0"

            allowable_nr = _parse_amount(nr_raw)
            # Only populate allowable_r when the rural indicator is '1' and the amount is > 0
            allowable_r_raw = _parse_amount(r_raw)
            allowable_r = allowable_r_raw if rural_ind == "1" and allowable_r_raw else None

            description = parts[_TXT_DESC].strip() if len(parts) > _TXT_DESC else ""

            records.append(
                {
                    "hcpcs_code": hcpcs,
                    "description": description,
                    "state_abbr": rec_state,
                    "year": rec_year,
                    "allowable": allowable_nr,
                    "allowable_nr": allowable_nr,
                    "allowable_r": allowable_r,
                    "modifier": modifier,
                }
            )
    return records


# ---------------------------------------------------------------------------
# CMS DMEPOS CSV parser (grid format with preamble)
# ---------------------------------------------------------------------------

_HEADER_REQUIRED = {"hcpcs", "description"}
# Column header pattern for state-specific NR/R columns.
# Handles all observed CMS variants:
#   "AZ (NR)" "AZ (R)"   — standard parentheses with space
#   "AZ(NR)"  "AZ(R)"    — parentheses without space
#   "AZ NR"   "AZ R"     — space-separated, no parens
#   "AZ-NR"   "AZ-R"     — hyphen-separated
# Brackets must be balanced: if an opening paren/bracket is present the
# corresponding closing one is required; if absent, neither is required.
_STATE_COL_RE = _re.compile(
    r"^([A-Z]{2})\s*(?:\((?P<kind1>NR|R)\)|\[(?P<kind2>NR|R)\]|[-\s](?P<kind3>NR|R))$",
    _re.IGNORECASE,
)


def _find_csv_header_line(fp):
    """Scan *fp* line-by-line and return (line_index, header_text) for the real header.

    The real header is the first line whose lowercase tokens include both
    'hcpcs' and 'description'.  Returns (None, None) if not found.
    """
    for idx, line in enumerate(fp):
        lower = line.lower()
        if "hcpcs" in lower and "description" in lower:
            return idx, line
    return None, None


def parse_dmepos_grid_csv(path, state_abbr=None, year=None):
    """Parse a CMS DMEPOS grid-format CSV (one row per HCPCS, states as columns).

    Skips any preamble rows before the real header line.
    Expands each row into per-state records populated with allowable_nr and
    allowable_r from columns like ``AZ (NR)`` and ``AZ (R)``.

    Args:
        path: Path to the CSV file.
        state_abbr: If provided, only return records for this state.
        year: Override year for all records (otherwise not set).

    Returns list of record dicts.
    """
    records = []
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as fp:
        header_idx, header_line = _find_csv_header_line(fp)
        if header_line is None:
            return records

        # Re-open and skip to the header line
        fp.seek(0)
        for _ in range(header_idx):
            next(fp)

        reader = csv.DictReader(fp)
        # Normalize header keys
        for row in reader:
            if not any(v and v.strip() for v in row.values()):
                continue
            norm = {k.strip(): (v or "").strip() for k, v in row.items() if k is not None}
            norm_lower = {k.lower(): v for k, v in norm.items()}

            hcpcs = (
                norm_lower.get("hcpcs") or norm_lower.get("hcpcs_cd") or norm_lower.get("hcpcs code") or ""
            ).upper().strip()
            if not hcpcs:
                continue

            description = (
                norm_lower.get("description") or norm_lower.get("long_description") or ""
            )
            mod1_raw = (norm_lower.get("mod") or norm_lower.get("modifier") or "").strip()
            mod2_raw = (norm_lower.get("mod2") or "").strip()
            if mod1_raw and mod2_raw:
                modifier = f"{mod1_raw},{mod2_raw}"
            elif mod1_raw:
                modifier = mod1_raw
            elif mod2_raw:
                modifier = mod2_raw
            else:
                modifier = None

            # Collect NR/R amounts per state from column headers
            nr_by_state = {}
            r_by_state = {}
            for col_key, val in norm.items():
                m = _STATE_COL_RE.match(col_key.strip())
                if m:
                    st = m.group(1).upper()
                    # Extract whichever named group captured the NR/R indicator
                    kind = (m.group("kind1") or m.group("kind2") or m.group("kind3") or "").upper()
                    amount = _parse_amount(val)
                    if kind == "NR":
                        nr_by_state[st] = amount
                    else:
                        r_by_state[st] = amount

            states_to_emit = {state_abbr.upper()} if state_abbr else set(nr_by_state) | set(r_by_state)

            for st in states_to_emit:
                allowable_nr = nr_by_state.get(st)
                allowable_r = r_by_state.get(st)
                records.append(
                    {
                        "hcpcs_code": hcpcs,
                        "description": description,
                        "state_abbr": st,
                        "year": year or datetime.now().year,
                        "allowable": allowable_nr,
                        "allowable_nr": allowable_nr,
                        "allowable_r": allowable_r,
                        "modifier": modifier,
                    }
                )
    return records


# ---------------------------------------------------------------------------
# General-purpose CMS file parser (dispatches to tilde or CSV)
# ---------------------------------------------------------------------------

def parse_cms_csv(path, state_abbr=None, year=None):
    """Parse a CMS DMEPOS fee schedule CSV file and return record dicts.

    Handles comma/pipe-delimited .csv files in grid format (with preamble rows).
    The CMS sync always extracts CSV files; tilde-delimited .txt files are no
    longer used in the automated download path.

    state_abbr and year are optional filter / default values.
    """
    delimiter = _detect_delimiter(path)

    # CSV / pipe-delimited — try grid format first (has NR/R columns)
    grid_records = parse_dmepos_grid_csv(path, state_abbr=state_abbr, year=year)
    if grid_records:
        return grid_records

    # Fallback: generic column-based CSV (older VISN / legacy CMS format)
    records = []
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as fp:
        header_idx, header_line = _find_csv_header_line(fp)
        if header_line is None:
            fp.seek(0)
            header_idx = 0

        fp.seek(0)
        for _ in range(header_idx):
            next(fp)

        reader = csv.DictReader(fp, delimiter=delimiter)
        for row in reader:
            if not any(v and v.strip() for v in row.values()):
                continue
            norm = {
                k.strip().lower().replace(" ", "_").replace("-", "_"): (v or "").strip()
                for k, v in row.items()
                if k is not None
            }
            hcpcs_code = (
                norm.get("hcpcs_cd") or norm.get("hcpcs_code") or norm.get("hcpcs")
                or norm.get("proc_cd") or norm.get("procedure_code") or ""
            ).upper().strip()
            if not hcpcs_code:
                continue
            description = (
                norm.get("long_description") or norm.get("short_description")
                or norm.get("description") or norm.get("item_description") or ""
            )
            modifier = (norm.get("modifier") or norm.get("modifier_cd") or "").strip() or None
            allowable_raw = (
                norm.get("fee_schedule_price") or norm.get("purchase_fee_amt")
                or norm.get("pur_fee_amt") or norm.get("capped_rental_1_month")
                or norm.get("rent_1_mo_amt") or norm.get("allowable")
                or norm.get("fee") or norm.get("payment_amount") or ""
            )
            try:
                allowable = float(str(allowable_raw).replace("$", "").replace(",", "").strip())
            except (ValueError, TypeError):
                allowable = None
            records.append(
                {
                    "hcpcs_code": hcpcs_code,
                    "description": description,
                    "state_abbr": state_abbr,
                    "year": year,
                    "allowable": allowable,
                    "allowable_nr": allowable,
                    "allowable_r": None,
                    "modifier": modifier,
                }
            )
    return records


# ---------------------------------------------------------------------------
# VISN CSV parser (unchanged, for user-supplied files)
# ---------------------------------------------------------------------------

def parse_visn_csv(path):
    """Parse a VISN-format CSV file and return a list of record dicts."""
    records = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        # Try to skip preamble if this looks like a CMS grid file
        header_idx, header_line = _find_csv_header_line(f)
        if header_line is None:
            f.seek(0)
            header_idx = 0
        f.seek(0)
        for _ in range(header_idx):
            next(f)
        reader = csv.DictReader(f)
        for row in reader:
            norm = {
                k.strip().lower().replace(" ", "_").replace("-", "_"): (v or "").strip()
                for k, v in row.items()
            }
            hcpcs_code = (
                norm.get("hcpcs_code") or norm.get("hcpcs") or norm.get("code") or ""
            ).upper().strip()
            description = (
                norm.get("description") or norm.get("long_description")
                or norm.get("short_description") or norm.get("item_description")
                or norm.get("item_name") or ""
            )
            state_abbr = (
                norm.get("state_abbr") or norm.get("state") or norm.get("st") or ""
            ).upper().strip()
            year_raw = (
                norm.get("year") or norm.get("fy") or norm.get("calendar_year")
                or str(datetime.now().year)
            )
            try:
                year = int(str(year_raw).strip())
            except (ValueError, TypeError):
                year = datetime.now().year
            allowable_raw = (
                norm.get("allowable") or norm.get("fee") or norm.get("allowable_amount")
                or norm.get("fee_amount") or norm.get("purchase_fee_amt")
                or norm.get("rent_1_mo_amt") or ""
            )
            try:
                allowable = float(
                    str(allowable_raw).replace("$", "").replace(",", "").strip()
                )
            except (ValueError, TypeError):
                allowable = None
            modifier = (norm.get("modifier") or norm.get("modifier_code") or "").strip() or None
            if hcpcs_code:
                records.append(
                    {
                        "hcpcs_code": hcpcs_code,
                        "description": description,
                        "state_abbr": state_abbr,
                        "year": year,
                        "allowable": allowable,
                        "allowable_nr": allowable,
                        "allowable_r": None,
                        "modifier": modifier,
                    }
                )
    return records


# ---------------------------------------------------------------------------
# Rural ZIP file parser
# ---------------------------------------------------------------------------

def _find_zip_column(norm):
    """Find the ZIP code value from a normalized row dict.

    Checks well-known column names first, then falls back to any column
    whose normalized name contains 'zip' (e.g. 'dmepos_rural_zip_code').
    Returns the raw string value or ''.
    """
    # Try well-known column names first
    val = (
        norm.get("zip_code") or norm.get("zip5") or norm.get("zip")
        or norm.get("zipcode") or norm.get("postal_code")
        or norm.get("dmepos_rural_zip_code") or ""
    )
    if val:
        return val
    # Fallback: scan all columns for any key containing 'zip'
    for key, v in norm.items():
        if "zip" in key and v:
            return v
    return ""


def parse_rural_zip_file(path, year):
    """Parse a CMS DMERuralZIP file and return records for insert_rural_zips().

    The file may be CSV or tilde-delimited. Each record contains the ZIP code
    and optionally state abbreviation.  Returns list of dicts with keys:
    year, zip5, state_abbr (may be None).
    """
    delimiter = _detect_delimiter(path)
    records = []
    if delimiter == "~":
        # Tilde-delimited: columns vary; try to find ZIP in each line
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                parts = [p.strip() for p in line.split("~")]
                if not parts:
                    continue
                # Look for first field that looks like a 5-digit ZIP
                for part in parts:
                    z = part.strip().zfill(5)
                    if len(z) == 5 and z.isdigit():
                        # Try to find state abbreviation nearby
                        state = None
                        for p in parts:
                            if len(p.strip()) == 2 and p.strip().isalpha():
                                state = p.strip().upper()
                                break
                        records.append({"year": year, "zip5": z, "state_abbr": state})
                        break
        return records

    # CSV format: look for ZIP/State columns
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        # Try to skip preamble
        header_idx, header_line = _find_csv_header_line(f)
        if header_line is None:
            f.seek(0)
            header_idx = 0
        f.seek(0)
        for _ in range(header_idx):
            next(f)

        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            norm = {
                k.strip().lower().replace(" ", "_"): (v or "").strip()
                for k, v in row.items()
                if k is not None
            }
            # Find zip5 value using helper that handles CMS naming variants
            zip5_raw = _find_zip_column(norm)
            z = str(zip5_raw).strip()
            if not z:
                continue
            # Take first 5 digits
            digits = _re.sub(r"\D", "", z)[:5]
            if len(digits) < 5:
                continue
            state = (norm.get("state") or norm.get("state_abbr") or norm.get("st") or "").upper().strip() or None
            records.append({"year": year, "zip5": digits.zfill(5), "state_abbr": state})
    return records


# ---------------------------------------------------------------------------
# Import helpers
# ---------------------------------------------------------------------------

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
        states=",".join(states_imported),
    )
    return len(records)


def import_cms_csv(filepath, state_abbr=None, year=None):
    """Parse and import a CMS DMEPOS CSV/TXT file. Returns count of imported records.

    Uses replace semantics: existing ``cms_download`` rows for the same
    (state_abbr, year) are deleted before new rows are inserted, so
    re-importing the same file does not create duplicates.
    """
    from core.database import delete_fees_by_year_state_source
    records = parse_cms_csv(filepath, state_abbr=state_abbr, year=year)
    if records and state_abbr and year:
        delete_fees_by_year_state_source(
            state_abbr=state_abbr, year=year, data_source="cms_download"
        )
    insert_fees(records, data_source="cms_download")
    add_import_log(
        file_name=os.path.basename(filepath),
        source="CMS Download",
        record_count=len(records),
        states=state_abbr or ",".join(sorted({r.get("state_abbr", "") for r in records})),
    )
    return len(records)