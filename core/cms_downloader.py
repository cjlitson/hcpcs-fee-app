"""CMS DMEPOS fee schedule downloader.

Downloads the CMS DMEPOS (Durable Medical Equipment, Prosthetics, Orthotics,
and Supplies) fee schedule ZIP files for selected years, extracts the CSV data,
and imports it into the local database.

CMS DMEPOS fee schedule data source:
  https://www.cms.gov/medicare/payment/fee-schedules/dmepos
"""

import io
import os
import re
import zipfile
from datetime import datetime
from pathlib import Path

import requests

from core.database import add_import_log, insert_fees
from core.importer import parse_cms_csv

# CMS publishes quarterly DMEPOS fee schedule updates.
# URL templates — CMS sometimes changes the naming convention between years;
# multiple candidates are tried in order.
_CMS_URL_TEMPLATES = [
    # Newer naming pattern (2025+)
    "https://www.cms.gov/files/zip/{year}-dmepos-fee-schedule.zip",
    # Older naming pattern
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/DMEPOSFeeSched/Downloads/DMEPOSFS{year}Q1.zip",
    # Alternative
    "https://www.cms.gov/files/zip/dmepos-{year}-fee-schedule.zip",
]

# Mapping of US state abbreviations to state names
ALL_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}

SUPPORTED_YEARS = [2024, 2025, 2026]


class DownloadError(Exception):
    """Raised when the CMS download cannot be completed."""


def _try_download_zip(year, progress_callback=None):
    """Try each URL template for the given year and return the ZIP bytes.

    Raises DownloadError if none succeed.
    """
    last_error = None
    for template in _CMS_URL_TEMPLATES:
        url = template.format(year=year)
        try:
            if progress_callback:
                progress_callback(f"Trying {url} …")
            resp = requests.get(url, timeout=60, stream=True)
            if resp.status_code == 200:
                return resp.content
            last_error = f"HTTP {resp.status_code} from {url}"
        except requests.RequestException as exc:
            last_error = str(exc)
    raise DownloadError(
        f"Could not download CMS DMEPOS fee schedule for {year}.\n"
        f"Last error: {last_error}\n\n"
        "Please visit https://www.cms.gov/medicare/payment/fee-schedules/dmepos "
        "to download the file manually and use File → Import CSV."
    )


def _extract_csv_from_zip(zip_bytes):
    """Extract the first CSV file found inside a ZIP archive.

    Returns (filename, file_bytes) or raises DownloadError.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise DownloadError("No CSV file found inside the downloaded ZIP archive.")
        # Prefer the largest CSV (likely the main data file)
        csv_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        name = csv_names[0]
        return name, zf.read(name)


def download_cms_fees(year, selected_states, progress_callback=None):
    """Download CMS DMEPOS fee schedule for *year* and import for *selected_states*.

    Args:
        year: Integer year (e.g. 2024).
        selected_states: List of state abbreviations to import.
        progress_callback: Optional callable(str) for status messages.

    Returns:
        Total number of records imported.
    """
    if progress_callback:
        progress_callback(f"Downloading CMS DMEPOS fee schedule for {year}…")

    zip_bytes = _try_download_zip(year, progress_callback=progress_callback)

    if progress_callback:
        progress_callback("Extracting archive…")

    csv_name, csv_bytes = _extract_csv_from_zip(zip_bytes)

    # Write to a temp file so parse_cms_csv can read it
    tmp_dir = Path(__file__).parent.parent / "data"
    tmp_dir.mkdir(exist_ok=True)
    tmp_path = tmp_dir / f"_cms_tmp_{year}.csv"
    tmp_path.write_bytes(csv_bytes)

    total = 0
    try:
        for state_abbr in selected_states:
            if progress_callback:
                progress_callback(f"Importing {state_abbr} ({year})…")
            records = parse_cms_csv(str(tmp_path), state_abbr=state_abbr, year=year)
            insert_fees(records, data_source="cms_download")
            add_import_log(
                file_name=csv_name,
                source=f"CMS Download {year}",
                record_count=len(records),
                states=state_abbr,
            )
            total += len(records)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass

    return total
