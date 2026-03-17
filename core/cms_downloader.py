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
import xml.etree.ElementTree as _ET
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
    # No-quarter variant — CMS publishes this for the initial/only release of a year
    "https://www.cms.gov/files/zip/dme{year2d}.zip",
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

# Pattern for DMEPOS fee schedule ZIPs: dme{YY}[-][a-d].zip or dme{YY}.zip
# Quarter letter is optional to handle dme26.zip (CMS publishes this for initial releases).
_QUARTERLY_ZIP_RE = _re.compile(r"dme\d{2}(?:-?[a-d])?\.zip$", _re.IGNORECASE)

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


# CMS RSS feed URL for the DMEPOS fee schedule page
_CMS_RSS_URL = "https://www.cms.gov/rss/30881"

# Preference key for the pattern tracker
_PATTERN_TRACKER_KEY = "cms_successful_patterns"


def _scrape_rss_urls(year):
    """Fetch the CMS DMEPOS RSS feed and return ZIP URLs for *year*.

    Parses ``<link>`` and ``<guid>`` elements for sub-page URLs matching the
    ``dme{yy}`` pattern, follows those sub-pages to find ZIP links (reusing
    ``_collect_zip_urls`` logic), and returns discovered ZIP URLs.

    Returns an empty list on any failure — RSS is a best-effort discovery layer.
    """
    year2d = str(year)[-2:]
    try:
        resp = requests.get(_CMS_RSS_URL, timeout=15)
        if resp.status_code != 200:
            return []

        root = _ET.fromstring(resp.text)

        # Collect candidate sub-page URLs from <link> and <guid> elements
        subpage_urls = []
        seen_subpages = set()
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for elem in root.iter():
            tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
            if tag in ("link", "guid") and elem.text:
                url = elem.text.strip()
                # Check if this looks like a year-specific sub-page for the requested year
                path = url.split("?")[0]
                m = _re.search(r"/dme(\d{2})(?:[^/\d]|$)", path, _re.IGNORECASE)
                if m and m.group(1) == year2d and url not in seen_subpages:
                    seen_subpages.add(url)
                    subpage_urls.append(url)

        seen_zips = set()
        zip_urls = []
        for subpage_url in subpage_urls:
            try:
                sub_resp = requests.get(subpage_url, timeout=15)
                if sub_resp.status_code != 200:
                    continue
                extractor = _LinkExtractor()
                extractor.feed(sub_resp.text)
                for href in extractor.links:
                    abs_url = urljoin(subpage_url, href)
                    filename = abs_url.split("/")[-1]
                    if not _QUARTERLY_ZIP_RE.match(filename):
                        continue
                    filename_nohyphen = filename.replace("-", "").lower()
                    if not filename_nohyphen.startswith(f"dme{year2d}"):
                        continue
                    if abs_url not in seen_zips:
                        seen_zips.add(abs_url)
                        zip_urls.append(abs_url)
            except Exception:
                pass

        zip_urls.sort(key=lambda u: u.lower(), reverse=True)
        return zip_urls
    except Exception:
        return []


def _record_successful_pattern(year, url, discovered_via):
    """Record a successful download URL pattern for *year* in the pattern tracker.

    Extracts the URL template (replacing 2-digit year with ``{yy}`` and quarter
    letter with ``{q}``) and saves to user preferences so future syncs can use
    the same pattern for newer years.
    """
    year2d = str(year)[-2:]
    try:
        # Extract the pattern template from the URL
        filename = url.split("/")[-1]
        # Replace year digits with {yy} and quarter letter with {q}
        pattern = _re.sub(
            r"(?i)(dme)" + year2d + r"(-?)([a-d])(\.zip)$",
            r"\1{yy}\2{q}\4",
            filename,
        )
        if pattern == filename:
            # No quarter letter — plain dme{yy}.zip pattern
            pattern = _re.sub(
                r"(?i)(dme)" + year2d + r"(\.zip)$",
                r"\1{yy}\2",
                filename,
            )

        raw = get_preference(_PATTERN_TRACKER_KEY)
        data = {}
        if raw:
            try:
                data = json.loads(raw)
            except Exception:
                data = {}
        patterns = data.get("patterns", {})
        patterns[str(year)] = {
            "url": url,
            "pattern": pattern,
            "discovered_via": discovered_via,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        data["patterns"] = patterns
        set_preference(_PATTERN_TRACKER_KEY, json.dumps(data))
    except Exception:
        pass  # Pattern recording is non-critical


def _generate_pattern_candidates(year):
    """Generate candidate URLs for *year* from previously successful patterns.

    Loads stored patterns from user preferences, generates candidate URLs for
    the requested year by substituting ``{yy}`` with the 2-digit year, and
    ranks them by recency (most recent prior year's pattern first).

    Returns a list of candidate URL strings (may be empty).
    """
    year2d = str(year)[-2:]
    try:
        raw = get_preference(_PATTERN_TRACKER_KEY)
        if not raw:
            return []
        data = json.loads(raw)
        patterns = data.get("patterns", {})

        # Sort prior years by recency (descending) — exclude the requested year itself
        prior = [
            (int(y), info)
            for y, info in patterns.items()
            if int(y) != year
        ]
        prior.sort(key=lambda x: x[0], reverse=True)

        candidates = []
        seen_patterns = set()
        for _yr, info in prior:
            tmpl = info.get("pattern", "")
            if not tmpl or tmpl in seen_patterns:
                continue
            seen_patterns.add(tmpl)
            # Generate URL: replace {yy} with 2-digit year; drop {q} variants separately
            if "{q}" in tmpl:
                # Generate all quarter variants (d first — most recent)
                for q in ("d", "c", "b", "a"):
                    filename = tmpl.replace("{yy}", year2d).replace("{q}", q)
                    url = f"https://www.cms.gov/files/zip/{filename}"
                    if url not in candidates:
                        candidates.append(url)
            else:
                filename = tmpl.replace("{yy}", year2d)
                url = f"https://www.cms.gov/files/zip/{filename}"
                if url not in candidates:
                    candidates.append(url)
        return candidates
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
    """Scrape the CMS DMEPOS page and RSS feed; return a set of available years.

    Results are cached for 24 hours in user_preferences.  On any error (network
    failure, unexpected page format, etc.) returns an empty set so callers can
    fall back gracefully.

    Year patterns detected:
    - ``dme{yy}[-][a-d].zip`` or ``dme{yy}.zip`` → 2000 + yy
    - URLs containing ``dmepos`` and a 4-digit year
    - ``DMEPOSFS{year}Q{n}.zip``
    - RSS feed ``<link>``/``<guid>`` elements matching ``/dme{yy}``
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
            # Pattern: dme{yy}[-][a-d].zip, dme{yy}.zip, or dme{yy}[a-d].zip (no hyphen)
            m = _re.search(r"dme(\d{2})(?:-?[a-d])?\.zip", lower)
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

    years = set()

    # Attempt RSS feed discovery first (lightweight and structured)
    try:
        rss_resp = requests.get(_CMS_RSS_URL, timeout=15)
        if rss_resp.status_code == 200:
            root = _ET.fromstring(rss_resp.text)
            rss_links = []
            for elem in root.iter():
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                if tag in ("link", "guid") and elem.text:
                    rss_links.append(elem.text.strip())
            years |= _extract_years_from_links(rss_links)
    except Exception:
        pass

    try:
        resp = requests.get(CMS_DMEPOS_PAGE, timeout=15)
        if resp.status_code != 200:
            if not years:
                return set()
        else:
            extractor = _LinkExtractor()
            extractor.feed(resp.text)
            years |= _extract_years_from_links(extractor.links)

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
    except Exception:
        if not years:
            return set()

    if not years:
        return set()

    # Cache the result
    cache_data = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "years": sorted(years),
    }
    set_preference(_AVAILABLE_YEARS_CACHE_KEY, json.dumps(cache_data))
    return years


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

    Discovery strategy (in order):
    1. Check cache for previously discovered URLs for this year.
    2. Check CMS RSS feed for sub-page links → follow to find ZIP URLs.
    3. Scrape the CMS DMEPOS HTML page + sub-pages.
    4. Generate candidates from pattern tracker (prior year successes).
    5. Fall back to hardcoded URL templates.

    On success, records the successful URL pattern to the pattern tracker and
    caches the URL for future use.  On failure, clears the URL cache so stale
    entries don't block future sync attempts.
    """
    year2d = str(year)[-2:]

    # Build candidate URL list with discovery-method tags for the pattern tracker
    # Each entry: (url, discovery_method)
    candidate_entries = []

    # Track which URLs are trusted (no HEAD pre-check needed).
    # Quarterly templates and scraped/cached URLs skip HEAD for speed.
    trusted_urls = set()

    # Quarterly and no-letter templates are always trusted
    quarterly_trusted = {
        f"https://www.cms.gov/files/zip/dme{year2d}.zip",
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

    seen_urls = set()

    def _add_candidates(urls, method):
        for url in urls:
            if url not in seen_urls:
                seen_urls.add(url)
                candidate_entries.append((url, method))

    # Step 1: Check cache
    cached = _get_cached_urls(year)
    if cached:
        if progress_callback:
            progress_callback(f"Using {len(cached)} cached URL(s) for {year}…")
        _add_candidates(cached, "cache")
        trusted_urls.update(cached)
    else:
        # Step 2: RSS feed discovery
        if progress_callback:
            progress_callback("Checking CMS RSS feed for download links…")
        rss_urls = _scrape_rss_urls(year)
        if rss_urls:
            if progress_callback:
                progress_callback(f"Found {len(rss_urls)} link(s) via RSS feed.")
            _add_candidates(rss_urls, "rss")
            trusted_urls.update(rss_urls)

        # Step 3: Scrape CMS HTML page
        if progress_callback:
            progress_callback("Checking CMS website for current download links…")
        scraped = _scrape_cms_urls(year)
        if scraped:
            if progress_callback:
                progress_callback(f"Found {len(scraped)} download link(s) on CMS website.")
            _add_candidates(scraped, "scrape")
            trusted_urls.update(scraped)

        # Cache all discovered URLs (RSS + scrape combined)
        all_discovered = [url for url, _ in candidate_entries]
        if all_discovered:
            _set_cached_urls(year, all_discovered)

    # Step 4: Pattern tracker candidates
    pattern_candidates = _generate_pattern_candidates(year)
    _add_candidates(pattern_candidates, "pattern")
    trusted_urls.update(pattern_candidates)

    # Step 5: Hardcoded template fallback
    for template in _CMS_URL_TEMPLATES:
        url = template.format(year=year, year2d=year2d)
        method = "template"
        if url not in seen_urls:
            seen_urls.add(url)
            candidate_entries.append((url, method))

    # Try each candidate
    last_error = None
    for url, discovery_method in candidate_entries:
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
                _record_successful_pattern(year, url, discovery_method)
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
_RURAL_ZIP_FILE_RE = _re.compile(r"^dme\s*rural", _re.IGNORECASE)


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