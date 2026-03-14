"""CMS DMEPOS fee schedule downloader.

Downloads the CMS DMEPOS (Durable Medical Equipment, Prosthetics, Orthotics,
and Supplies) fee schedule ZIP files for selected years, extracts the CSV data,
and imports it into the local database.

CMS DMEPOS fee schedule data source:
  https://www.cms.gov/medicare/payment/fee-schedules/dmepos
"""

import html.parser
import io
import json
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests

from core.database import _get_app_dir, add_import_log, get_preference, insert_fees, set_preference
from core.importer import parse_cms_csv

# CMS DMEPOS page used to scrape current download links
CMS_DMEPOS_PAGE = "https://www.cms.gov/medicare/payment/fee-schedules/dmepos"
_URL_CACHE_KEY_PREFIX = "cms_url_cache_"
_URL_CACHE_TTL_HOURS = 24

# CMS publishes quarterly DMEPOS fee schedule updates.
# URL templates — CMS sometimes changes the naming convention between years;
# multiple candidates are tried in order.
# {year2d} = 2-digit year (e.g. "25" for 2025); {year} = 4-digit year.
_CMS_URL_TEMPLATES = [
    # Quarterly pattern — current CMS convention (most recent quarter first)
    "https://www.cms.gov/files/zip/dme{year2d}-d.zip",
    "https://www.cms.gov/files/zip/dme{year2d}-c.zip",
    "https://www.cms.gov/files/zip/dme{year2d}-b.zip",
    "https://www.cms.gov/files/zip/dme{year2d}-a.zip",
    # Legacy patterns
    "https://www.cms.gov/files/zip/{year}-dmepos-fee-schedule.zip",
    "https://www.cms.gov/files/zip/dmepos-{year}-fee-schedule.zip",
    "https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/DMEPOSFeeSched/Downloads/DMEPOSFS{year}Q1.zip",
]


class _LinkExtractor(html.parser.HTMLParser):
    """Extracts all href values from <a> tags that end with '.zip'."""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr, val in attrs:
                if attr == "href" and val and val.lower().endswith(".zip"):
                    self.links.append(val)


def _scrape_cms_urls(year):
    """Scrape the CMS DMEPOS page for ZIP download links relevant to *year*.

    Returns a deduplicated list of absolute URLs, or an empty list on any error.
    """
    year2d = str(year)[-2:]
    try:
        resp = requests.get(CMS_DMEPOS_PAGE, timeout=15)
        if resp.status_code != 200:
            return []
        extractor = _LinkExtractor()
        extractor.feed(resp.text)
        seen = set()
        urls = []
        for href in extractor.links:
            # Resolve relative URLs against the CMS base
            abs_url = urljoin("https://www.cms.gov", href)
            lower = abs_url.lower()
            # Keep links that match the quarterly pattern for this year (e.g. "dme25")
            # OR contain "dmepos" with the 4-digit year — avoids false positives from
            # unrelated numeric substrings (e.g. version numbers, other years).
            if f"dme{year2d}" in lower or ("dmepos" in lower and str(year) in lower):
                if abs_url not in seen:
                    seen.add(abs_url)
                    urls.append(abs_url)
        return urls
    except Exception:
        return []


def _get_cached_urls(year):
    """Return cached URL list for *year* if cache is still valid, else empty list."""
    key = f"{_URL_CACHE_KEY_PREFIX}{year}"
    raw = get_preference(key)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        cached_at = datetime.fromisoformat(data["cached_at"])
        # Ensure timezone-aware comparison even if stored timestamp lacks tzinfo
        if cached_at.tzinfo is None:
            cached_at = cached_at.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600
        if age_hours > _URL_CACHE_TTL_HOURS:
            return []
        return data["urls"]
    except Exception:
        return []


def _set_cached_urls(year, urls):
    """Cache URL list for *year* with the current timestamp."""
    key = f"{_URL_CACHE_KEY_PREFIX}{year}"
    data = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "urls": urls,
    }
    set_preference(key, json.dumps(data))


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

SUPPORTED_YEARS = list(range(2024, date.today().year + 2))


class DownloadError(Exception):
    """Raised when the CMS download cannot be completed."""


def _try_download_zip(year, progress_callback=None):
    """Try to download the CMS DMEPOS ZIP for the given year.

    Strategy:
    1. Check cache for previously discovered URLs for this year.
    2. If cache miss or expired, scrape the CMS DMEPOS page to discover URLs.
    3. Cache any discovered URLs.
    4. Try discovered URLs first, then fall back to hardcoded templates.
    5. Raise DownloadError if all attempts fail.
    """
    year2d = str(year)[-2:]

    # Build candidate URL list
    candidate_urls = []

    # Step 1: Check cache
    cached = _get_cached_urls(year)
    if cached:
        if progress_callback:
            progress_callback(f"Using {len(cached)} cached URL(s) for {year}…")
        candidate_urls.extend(cached)
    else:
        # Step 2: Scrape CMS page
        if progress_callback:
            progress_callback("Checking CMS website for current download links…")
        scraped = _scrape_cms_urls(year)
        if scraped:
            if progress_callback:
                progress_callback(f"Found {len(scraped)} download link(s) on CMS website.")
            _set_cached_urls(year, scraped)
            candidate_urls.extend(scraped)

    # Step 3: Add hardcoded fallback templates
    for template in _CMS_URL_TEMPLATES:
        url = template.format(year=year, year2d=year2d)
        if url not in candidate_urls:
            candidate_urls.append(url)

    # Step 4: Try each candidate
    last_error = None
    for url in candidate_urls:
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
    """Extract the main data file from a CMS DMEPOS ZIP archive.

    CMS ZIPs may contain:
    - .csv files (older format)
    - .txt files (pipe-delimited, current format as of 2025)
    - README / layout description .txt files (excluded)

    Returns (filename, file_bytes) for the largest data file found,
    or raises DownloadError if no suitable file is found.
    """
    # Keywords that identify non-data files to skip
    _SKIP_KEYWORDS = ("readme", "read_me", "read me", "layout", "record layout", "codebook")

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        all_names = zf.namelist()

        # First try: look for CSV files
        data_names = [
            n for n in all_names
            if n.lower().endswith(".csv")
            and not any(kw in n.lower() for kw in _SKIP_KEYWORDS)
        ]

        # Second try: pipe-delimited TXT files (CMS 2025 format)
        if not data_names:
            data_names = [
                n for n in all_names
                if n.lower().endswith(".txt")
                and not any(kw in n.lower() for kw in _SKIP_KEYWORDS)
            ]

        if not data_names:
            raise DownloadError(
                "No CSV or data file found inside the downloaded ZIP archive.\n"
                f"ZIP contents: {', '.join(all_names) or '(empty)'}"
            )

        # Prefer the largest file — most likely the main fee schedule data
        data_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        name = data_names[0]
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

    data_name, data_bytes = _extract_csv_from_zip(zip_bytes)

    # Write to a temp file so parse_cms_csv can read it
    tmp_dir = _get_app_dir() / "data"
    tmp_dir.mkdir(exist_ok=True)
    tmp_path = tmp_dir / f"_cms_tmp_{year}.txt"
    tmp_path.write_bytes(data_bytes)

    total = 0
    try:
        for state_abbr in selected_states:
            if progress_callback:
                progress_callback(f"Importing {state_abbr} ({year})…")
            records = parse_cms_csv(str(tmp_path), state_abbr=state_abbr, year=year)
            insert_fees(records, data_source="cms_download")
            add_import_log(
                file_name=data_name,
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