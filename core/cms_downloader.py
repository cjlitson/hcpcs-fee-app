"""CMS DMEPOS fee schedule downloader.

Downloads the CMS DMEPOS (Durable Medical Equipment, Prosthetics, Orthotics,
and Supplies) fee schedule ZIP files for selected years, extracts the CSV data,
and imports it into the local database.

CMS DMEPOS fee schedule data source:
  https://www.cms.gov/medicare/payment/fee-schedules/dmepos/dmepos-fee-schedule
"""

import html.parser
import io
import json
import os
import re as _re
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests

from core.database import (
    _get_app_dir,
    add_import_log,
    delete_fees_by_year_state_source,
    delete_rural_zips_by_year,
    get_preference,
    insert_fees,
    insert_rural_zips,
    set_preference,
)
from core.importer import parse_cms_csv, parse_rural_zip_file

# CMS DMEPOS *fee schedule* page — this is where quarterly fee schedule ZIPs are listed.
# Using the dedicated fee schedule page avoids picking up jurisdiction-list ZIPs
# that appear on the broader DMEPOS landing page.
CMS_DMEPOS_PAGE = "https://www.cms.gov/medicare/payment/fee-schedules/dmepos/dmepos-fee-schedule"
_URL_CACHE_KEY_PREFIX = "cms_url_cache_"
_URL_CACHE_TTL_HOURS = 24
_AVAILABLE_YEARS_CACHE_KEY = "cms_available_years_cache"

# CMS publishes quarterly DMEPOS fee schedule updates.
# URL templates — CMS sometimes changes the naming convention between years;
# multiple candidates are tried in order.
# {year2d} = 2-digit year (e.g. "25" for 2025); {year} = 4-digit year.
# NOTE: Only include patterns that are known to be valid / current.
# Legacy "DMEPOSFS{year}Q1.zip" is intentionally excluded — CMS retired that
# URL scheme; including it caused spurious 404 errors when all other patterns
# fail.
_CMS_URL_TEMPLATES = [
    # Quarterly pattern — current CMS convention (most recent quarter first)
    "https://www.cms.gov/files/zip/dme{year2d}-d.zip",
    "https://www.cms.gov/files/zip/dme{year2d}-c.zip",
    "https://www.cms.gov/files/zip/dme{year2d}-b.zip",
    "https://www.cms.gov/files/zip/dme{year2d}-a.zip",
    # No-hyphen variants (CMS has used both forms)
    "https://www.cms.gov/files/zip/dme{year2d}d.zip",
    "https://www.cms.gov/files/zip/dme{year2d}c.zip",
    "https://www.cms.gov/files/zip/dme{year2d}b.zip",
    "https://www.cms.gov/files/zip/dme{year2d}a.zip",
    # Legacy patterns (tried last; verified via HEAD before attempting download)
    "https://www.cms.gov/files/zip/{year}-dmepos-fee-schedule.zip",
    "https://www.cms.gov/files/zip/dmepos-{year}-fee-schedule.zip",
]

# Pattern for quarterly fee schedule ZIPs: dme{YY}[-][a-d].zip (case-insensitive)
# Hyphen is optional to handle both dme26-a.zip and dme26a.zip forms.
_QUARTERLY_ZIP_RE = _re.compile(r"dme\d{2}-?[a-d]\.zip$", _re.IGNORECASE)

# Sub-page link pattern: year-specific detail pages on the CMS site
# e.g. /dme26 or /dmepos-fee-schedule/dme26
_SUBPAGE_LINK_RE = _re.compile(r"/dme\d{2}(?:[^/]*)$", _re.IGNORECASE)


class _LinkExtractor(html.parser.HTMLParser):
    """Extracts href values from all <a> tags.

    Collects all hrefs so that both ZIP links and sub-page links are captured
    for two-level scraping.
    """

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for attr, val in attrs:
                if attr == "href" and val:
                    self.links.append(val)


def _scrape_cms_urls(year):
    """Scrape the CMS DMEPOS fee schedule page for quarterly ZIP links for *year*.

    Follows sub-page links matching year-specific detail pages (e.g. /dme26 or
    /dmepos-fee-schedule/dme26) and scrapes those for ZIP links as well.

    Accepts links matching ``dmeYY[-][a-d].zip`` (hyphen optional, case-insensitive)
    to handle both URL naming conventions CMS has used.

    Returns a deduplicated list of absolute URLs (most-recent quarter first),
    or an empty list on any error.
    """
    year2d = str(year)[-2:]

    def _collect_zip_urls(html_text, base_url, seen, urls):
        extractor = _LinkExtractor()
        extractor.feed(html_text)
        for href in extractor.links:
            abs_url = urljoin(base_url, href)
            filename = abs_url.split("/")[-1]
            if not _QUARTERLY_ZIP_RE.match(filename):
                continue
            # Normalize by removing hyphen so dme26a.zip and dme26-a.zip both match
            filename_nohyphen = filename.replace("-", "").lower()
            if not filename_nohyphen.startswith(f"dme{year2d}"):
                continue
            if abs_url not in seen:
                seen.add(abs_url)
                urls.append(abs_url)
        return extractor.links

    def _collect_subpage_links(html_text, base_url):
        extractor = _LinkExtractor()
        extractor.feed(html_text)
        subpages = []
        for href in extractor.links:
            abs_url = urljoin(base_url, href)
            path = abs_url.split("?")[0]
            if _SUBPAGE_LINK_RE.search(path):
                # Check that the sub-page matches the requested year
                m = _re.search(r"dme(\d{2})", path, _re.IGNORECASE)
                if m and m.group(1) == year2d:
                    subpages.append(abs_url)
        return subpages

    try:
        resp = requests.get(CMS_DMEPOS_PAGE, timeout=15)
        if resp.status_code != 200:
            return []

        seen = set()
        urls = []
        _collect_zip_urls(resp.text, CMS_DMEPOS_PAGE, seen, urls)

        # Follow year-specific sub-pages that may list the actual ZIP files
        subpages = _collect_subpage_links(resp.text, CMS_DMEPOS_PAGE)
        for subpage_url in subpages:
            try:
                sub_resp = requests.get(subpage_url, timeout=15)
                if sub_resp.status_code == 200:
                    _collect_zip_urls(sub_resp.text, subpage_url, seen, urls)
            except Exception:
                pass

        # Sort descending (D > C > B > A) so the latest quarter is tried first
        urls.sort(key=lambda u: u.lower(), reverse=True)
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


def discover_available_cms_years():
    """Scrape the CMS DMEPOS page and return a set of years currently available.

    Results are cached for 24 hours in user_preferences.  On any error (network
    failure, unexpected page format, etc.) returns an empty set so callers can
    fall back gracefully.

    Year patterns detected:
    - ``dme{yy}-a/b/c/d.zip``  → 2000 + yy
    - URLs containing ``dmepos`` and a 4-digit year
    - ``DMEPOSFS{year}Q{n}.zip``
    """
    # Check cache first
    raw = get_preference(_AVAILABLE_YEARS_CACHE_KEY)
    if raw:
        try:
            data = json.loads(raw)
            cached_at = datetime.fromisoformat(data["cached_at"])
            if cached_at.tzinfo is None:
                cached_at = cached_at.replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600
            if age_hours <= _URL_CACHE_TTL_HOURS:
                return set(data["years"])
        except Exception:
            pass

    def _extract_years_from_links(links, base_url="https://www.cms.gov"):
        years = set()
        for href in links:
            abs_url = urljoin(base_url, href)
            lower = abs_url.lower()
            # Pattern: dme{yy}-[a-d].zip or dme{yy}[a-d].zip (no hyphen)
            m = _re.search(r"dme(\d{2})-?[a-d]\.zip", lower)
            if m:
                years.add(2000 + int(m.group(1)))
                continue
            # Pattern: sub-page link e.g. /dme26 or /dmepos-fee-schedule/dme26
            m = _re.search(r"/dme(\d{2})(?:[^/\d]|$)", lower)
            if m:
                years.add(2000 + int(m.group(1)))
                continue
            # Pattern: dmepos + 4-digit year anywhere in URL
            m = _re.search(r"dmepos.*?(\b20\d{2}\b)", lower)
            if m:
                years.add(int(m.group(1)))
                continue
            # Pattern: DMEPOSFS{year}Q{n}.zip
            m = _re.search(r"dmeposfs(20\d{2})q\d", lower)
            if m:
                years.add(int(m.group(1)))
                continue
        return years

    try:
        resp = requests.get(CMS_DMEPOS_PAGE, timeout=15)
        if resp.status_code != 200:
            return set()
        extractor = _LinkExtractor()
        extractor.feed(resp.text)
        years = _extract_years_from_links(extractor.links)

        # Also follow sub-page links to detect years listed only on detail pages
        for href in extractor.links:
            abs_url = urljoin("https://www.cms.gov", href)
            path = abs_url.split("?")[0]
            if _SUBPAGE_LINK_RE.search(path):
                try:
                    sub_resp = requests.get(abs_url, timeout=15)
                    if sub_resp.status_code == 200:
                        sub_extractor = _LinkExtractor()
                        sub_extractor.feed(sub_resp.text)
                        years |= _extract_years_from_links(sub_extractor.links, abs_url)
                except Exception:
                    pass

        # Cache the result
        cache_data = {
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "years": sorted(years),
        }
        set_preference(_AVAILABLE_YEARS_CACHE_KEY, json.dumps(cache_data))
        return years
    except Exception:
        return set()


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
    "PR": "Puerto Rico", "VI": "Virgin Islands",
}

SUPPORTED_YEARS = list(range(2024, date.today().year + 1))


class DownloadError(Exception):
    """Raised when the CMS download cannot be completed."""


def _clear_url_cache(year):
    """Remove the cached URL list for *year* so the next attempt re-scrapes."""
    key = f"{_URL_CACHE_KEY_PREFIX}{year}"
    set_preference(key, "")


def _try_download_zip(year, progress_callback=None):
    """Try to download the CMS DMEPOS ZIP for the given year.

    Strategy:
    1. Check cache for previously discovered URLs for this year.
    2. If cache miss or expired, scrape the CMS DMEPOS page to discover URLs.
    3. Cache any discovered URLs.
    4. Try discovered URLs first, then fall back to hardcoded templates.
       Quarterly template URLs (dmeYY-[a-d].zip) are treated as trusted so they
       skip the HEAD pre-check — these are the definitive CMS quarterly patterns
       and some CMS servers respond to HEAD differently than GET.
    5. Raise DownloadError if all attempts fail (and clear the URL cache so stale
       entries don't block future attempts).
    """
    year2d = str(year)[-2:]

    # Build candidate URL list
    candidate_urls = []
    # Track which URLs were obtained via live scrape or cache (trusted sources).
    # Template-only URLs get a HEAD check first to skip retired patterns quickly,
    # EXCEPT for the quarterly dmeYY-[a-d].zip patterns which are always trusted.
    trusted_urls = set()

    # Quarterly templates are always trusted — skip HEAD check for these
    quarterly_trusted = {
        f"https://www.cms.gov/files/zip/dme{year2d}-d.zip",
        f"https://www.cms.gov/files/zip/dme{year2d}-c.zip",
        f"https://www.cms.gov/files/zip/dme{year2d}-b.zip",
        f"https://www.cms.gov/files/zip/dme{year2d}-a.zip",
        f"https://www.cms.gov/files/zip/dme{year2d}d.zip",
        f"https://www.cms.gov/files/zip/dme{year2d}c.zip",
        f"https://www.cms.gov/files/zip/dme{year2d}b.zip",
        f"https://www.cms.gov/files/zip/dme{year2d}a.zip",
    }
    trusted_urls.update(quarterly_trusted)

    # Step 1: Check cache
    cached = _get_cached_urls(year)
    if cached:
        if progress_callback:
            progress_callback(f"Using {len(cached)} cached URL(s) for {year}…")
        candidate_urls.extend(cached)
        trusted_urls.update(cached)
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
            trusted_urls.update(scraped)

    # Step 3: Add hardcoded fallback templates
    for template in _CMS_URL_TEMPLATES:
        url = template.format(year=year, year2d=year2d)
        if url not in candidate_urls:
            candidate_urls.append(url)

    # Step 4: Try each candidate
    # For trusted URLs (discovered via live scrape, cache, or quarterly pattern)
    # go straight to GET.  For legacy template URLs, first do a cheap HEAD request
    # to skip known-missing URLs quickly and avoid long timeouts.
    last_error = None
    for url in candidate_urls:
        try:
            if progress_callback:
                progress_callback(f"Trying {url} …")
            if url not in trusted_urls:
                # HEAD check to avoid spending time on 404 legacy template URLs
                try:
                    head = requests.head(url, timeout=10, allow_redirects=True)
                    if head.status_code not in (200, 301, 302):
                        last_error = f"HTTP {head.status_code} (HEAD) from {url}"
                        continue
                except requests.RequestException:
                    pass  # Proceed to GET anyway if HEAD itself fails
            resp = requests.get(url, timeout=60, stream=True)
            if resp.status_code == 200:
                return resp.content
            last_error = f"HTTP {resp.status_code} from {url}"
        except requests.RequestException as exc:
            last_error = str(exc)

    # All attempts failed — clear the URL cache so stale entries don't block
    # future sync attempts for this year.
    _clear_url_cache(year)

    raise DownloadError(
        f"Could not download CMS DMEPOS fee schedule for {year}.\n"
        f"Last error: {last_error}\n\n"
        "Please visit https://www.cms.gov/medicare/payment/fee-schedules/dmepos "
        "to download the file manually and use File → Import CSV."
    )


# Keywords that identify documentation / non-data files to always skip.
_SKIP_KEYWORDS = ("readme", "read_me", "read me", "layout", "record layout", "codebook")

# Additional exclusion keywords for the *main* fee schedule selection.
# Files containing any of these words are considered auxiliary datasets.
_MAIN_EXCLUSION_KEYWORDS = (
    "jurisdiction", "list", "rural", "zip code", "cba", "dmepen",
    "back", "fad", "former", "schedule file", "chng", "pen",
)

# Keywords that strongly indicate auxiliary (non-fee-schedule) datasets inside a
# CMS DMEPOS ZIP.  Files whose lowercased name contains any of these are
# deprioritised in the selection process.
_AUXILIARY_KEYWORDS = _MAIN_EXCLUSION_KEYWORDS

# Pattern for the rural ZIP code mapping file inside a CMS ZIP
_RURAL_ZIP_FILE_RE = _re.compile(r"^dmerural", _re.IGNORECASE)


def _select_rural_zip_filename(all_names):
    """Return the name of the rural ZIP code mapping file from *all_names*, or None."""
    for name in all_names:
        basename = os.path.basename(name).lower()
        if _RURAL_ZIP_FILE_RE.match(basename) and (
            name.lower().endswith(".csv") or name.lower().endswith(".txt")
        ):
            return name
    return None


def _select_main_dmepos_filename(all_names, zf):
    """Choose the main DMEPOS fee-schedule file from *all_names* (a ZIP name list).

    Only CSV files are considered.  Selection priority:
    1. Files whose basename starts with "dmepos" (case-insensitive), ends
       with .csv, and does NOT contain any exclusion keyword.
    2. Files that contain "dmepos" (anywhere), no auxiliary keyword, not skipped,
       ending .csv.
    3. Fallback: any non-skipped .csv file, sorted largest-first.

    .txt files are never selected — the CMS sync uses the grid-format CSV
    exclusively, which has self-describing column headers.

    Returns the chosen filename string, or raises ``DownloadError`` when the
    ZIP contains no usable CSV data file.
    """
    def basename_lower(name):
        return os.path.basename(name).lower()

    def is_skipped(name):
        return any(kw in name.lower() for kw in _SKIP_KEYWORDS)

    def has_exclusion(name):
        return any(kw in name.lower() for kw in _MAIN_EXCLUSION_KEYWORDS)

    # --- Tier 1: starts-with "dmepos", .csv, not skipped, no exclusion keywords ---
    dmepos_prefix_csv = [
        n for n in all_names
        if basename_lower(n).startswith("dmepos")
        and n.lower().endswith(".csv")
        and not is_skipped(n)
        and not has_exclusion(n)
    ]

    if len(dmepos_prefix_csv) == 1:
        return dmepos_prefix_csv[0]
    if len(dmepos_prefix_csv) > 1:
        # Among multiple matches take the largest
        dmepos_prefix_csv.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        return dmepos_prefix_csv[0]

    # --- Tier 2: contains "dmepos", .csv, no auxiliary keyword, not skipped -------
    dmepos_any_csv = [
        n for n in all_names
        if "dmepos" in n.lower()
        and n.lower().endswith(".csv")
        and not is_skipped(n)
        and not any(kw in n.lower() for kw in _AUXILIARY_KEYWORDS)
    ]

    if dmepos_any_csv:
        dmepos_any_csv.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
        return dmepos_any_csv[0]

    # --- Tier 3: fallback — any non-skipped .csv file, largest first ---------------
    fallback = [
        n for n in all_names
        if n.lower().endswith(".csv")
        and not is_skipped(n)
    ]
    if not fallback:
        raise DownloadError(
            "No CSV data file found inside the downloaded ZIP archive.\n"
            f"ZIP contents: {', '.join(all_names) or '(empty)'}"
        )

    fallback.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
    return fallback[0]


def _extract_csv_from_zip(zip_bytes, progress_callback=None):
    """Extract the main DMEPOS fee-schedule file (and optionally the rural ZIP file)
    from a CMS ZIP archive.

    Returns ``(filename, file_bytes, rural_filename_or_None, rural_bytes_or_None)``.
    Raises ``DownloadError`` if no suitable main data file is found.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        all_names = zf.namelist()
        name = _select_main_dmepos_filename(all_names, zf)
        if progress_callback:
            progress_callback(f"Selected ZIP entry: {name}")
        main_bytes = zf.read(name)

        # Also extract rural ZIP code mapping file if present
        rural_name = _select_rural_zip_filename(all_names)
        rural_bytes = None
        if rural_name:
            if progress_callback:
                progress_callback(f"Found rural ZIP mapping file: {rural_name}")
            rural_bytes = zf.read(rural_name)

        return name, main_bytes, rural_name, rural_bytes


def download_cms_fees(year, selected_states, progress_callback=None):
    """Download CMS DMEPOS fee schedule for *year* and import for *selected_states*.

    Also extracts and stores the rural ZIP code mapping for the year when present.

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

    data_name, data_bytes, rural_name, rural_bytes = _extract_csv_from_zip(
        zip_bytes, progress_callback=progress_callback
    )

    # Write to temp files
    tmp_dir = _get_app_dir() / "data"
    tmp_dir.mkdir(exist_ok=True)
    tmp_path = tmp_dir / f"_cms_tmp_{year}.csv"
    tmp_path.write_bytes(data_bytes)

    tmp_rural_path = None
    if rural_bytes:
        tmp_rural_path = tmp_dir / f"_cms_rural_tmp_{year}.csv"
        tmp_rural_path.write_bytes(rural_bytes)

    total = 0
    try:
        for state_abbr in selected_states:
            if progress_callback:
                progress_callback(f"Importing {state_abbr} ({year})…")
            records = parse_cms_csv(str(tmp_path), state_abbr=state_abbr, year=year)
            if len(records) == 0:
                raise DownloadError(
                    f"Parsed 0 records from {data_name} for {state_abbr} ({year}). "
                    "Aborting update to avoid wiping existing data. "
                    "Please verify the downloaded file or use File → Import CSV."
                )
            if progress_callback:
                progress_callback(f"Parsed {len(records)} records from {data_name}")
            # Replace: delete existing cms_download rows for this year/state, then insert
            delete_fees_by_year_state_source(
                state_abbr=state_abbr, year=year, data_source="cms_download"
            )
            insert_fees(records, data_source="cms_download")
            add_import_log(
                file_name=data_name,
                source=f"CMS Download {year}",
                record_count=len(records),
                states=state_abbr,
            )
            total += len(records)

        # Import rural ZIP codes for the year (replace-semantics: delete then insert)
        if tmp_rural_path:
            if progress_callback:
                progress_callback(f"Importing rural ZIP codes for {year}…")
            try:
                rural_records = parse_rural_zip_file(str(tmp_rural_path), year=year)
                if rural_records:
                    delete_rural_zips_by_year(year)
                    insert_rural_zips(rural_records)
                    if progress_callback:
                        progress_callback(
                            f"Stored {len(rural_records):,} rural ZIP codes for {year}."
                        )
            except Exception as exc:
                # Rural ZIP failure is non-fatal — log and continue
                if progress_callback:
                    progress_callback(f"Warning: rural ZIP import failed: {exc}")

    finally:
        for p in (tmp_path, tmp_rural_path):
            if p:
                try:
                    p.unlink()
                except OSError:
                    pass

    return total