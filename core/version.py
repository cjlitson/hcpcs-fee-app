"""App version and GitHub release update checker."""

import re
import requests

# ---- Bump this on every release ----
APP_VERSION = "1.1.0"

GITHUB_REPO = "cjlitson/hcpcs-fee-app"
RELEASES_URL = f"https://github.com/{GITHUB_REPO}/releases/latest"
API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"


def _parse_version(tag: str):
    """Parse a 'vX.Y.Z' or 'X.Y.Z' tag into a (major, minor, patch) tuple.

    Returns None if the tag cannot be parsed.
    """
    m = re.match(r"v?(\d+)\.(\d+)\.(\d+)", tag.strip())
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def check_for_update():
    """Check GitHub for a newer release.

    Returns (latest_version_str, download_url) if an update is available,
    or None if we're up to date or the check fails.

    This function must be safe to call from a background thread.
    It never raises — all errors are caught and return None.
    """
    try:
        resp = requests.get(
            API_URL,
            timeout=5,
            verify=True,
            headers={"Accept": "application/vnd.github+json"},
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        tag = data.get("tag_name", "")
        latest = _parse_version(tag)
        current = _parse_version(APP_VERSION)
        if latest is None or current is None:
            return None
        if latest > current:
            # Use the html_url from the release, fall back to our constant
            html_url = data.get("html_url", RELEASES_URL)
            return (tag.lstrip("v"), html_url)
        return None
    except Exception:
        return None
