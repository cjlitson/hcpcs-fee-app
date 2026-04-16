from __future__ import annotations

from core.config import get_config_value, set_config_value

_VENDOR_KEY = "saved_vendors"


def list_saved_vendors() -> list[str]:
    raw = get_config_value(_VENDOR_KEY, [])
    if not isinstance(raw, list):
        return []
    out = []
    seen = set()
    for item in raw:
        name = str(item or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def save_vendor_name(name: str) -> list[str]:
    cleaned = (name or "").strip()
    vendors = list_saved_vendors()
    if not cleaned:
        return vendors
    if cleaned.lower() not in {v.lower() for v in vendors}:
        vendors.append(cleaned)
        set_config_value(_VENDOR_KEY, vendors)
    return vendors
