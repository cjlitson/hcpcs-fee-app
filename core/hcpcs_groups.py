"""HCPCS Level II code group definitions.

CMS organises HCPCS codes by their prefix letter. This module provides
the canonical mapping used throughout the app for grouping and browsing.
"""

# Ordered dict of prefix letter → (short_name, description)
# Only includes prefixes relevant to DMEPOS fee schedules, plus common
# adjacent categories that may appear in CMS data files.
HCPCS_GROUPS = {
    "A": ("Medical & Surgical Supplies", "Transportation services, medical/surgical supplies, and miscellaneous items (A0000–A9999)"),
    "B": ("Enteral/Parenteral Therapy", "Enteral and parenteral therapy supplies and equipment (B0000–B9999)"),
    "C": ("Hospital Outpatient PPS", "Temporary hospital outpatient prospective payment system codes (C0000–C9999)"),
    "D": ("Dental Procedures", "Dental procedures and services (D0000–D9999)"),
    "E": ("Durable Medical Equipment", "Durable medical equipment — wheelchairs, hospital beds, oxygen, CPAP, etc. (E0000–E9999)"),
    "G": ("Procedures & Services", "Temporary procedures and professional services not in CPT (G0000–G9999)"),
    "H": ("Rehabilitative Services", "Alcohol/drug treatment and rehabilitative services (H0000–H9999)"),
    "J": ("Drugs (Non-Oral)", "Drugs administered other than oral method, chemotherapy drugs (J0000–J9999)"),
    "K": ("Temporary DME Codes", "Temporary codes assigned by DME regional carriers (K0000–K9999)"),
    "L": ("Orthotics & Prosthetics", "Orthotic and prosthetic procedures and devices (L0000–L9999)"),
    "M": ("Medical Services", "Medical services (M0000–M9999)"),
    "P": ("Pathology & Lab", "Pathology and laboratory services (P0000–P9999)"),
    "Q": ("Temporary Codes", "Temporary codes for supplies, services, and drugs (Q0000–Q9999)"),
    "R": ("Diagnostic Radiology", "Diagnostic radiology services (R0000–R9999)"),
    "S": ("Private Sector Codes", "Temporary national codes — privately developed, not for Medicare (S0000–S9999)"),
    "T": ("State Medicaid Codes", "State Medicaid agency codes (T0000–T9999)"),
    "V": ("Vision & Hearing", "Vision and hearing devices and services (V0000–V9999)"),
}


def get_group_for_code(hcpcs_code):
    """Return (prefix, short_name, description) for a given HCPCS code, or None."""
    if not hcpcs_code:
        return None
    prefix = hcpcs_code[0].upper()
    if prefix in HCPCS_GROUPS:
        short, desc = HCPCS_GROUPS[prefix]
        return (prefix, short, desc)
    return None


def get_group_choices(only_prefixes=None):
    """Return a list of (prefix, display_label) tuples for use in combo boxes.

    If only_prefixes is provided (a set of prefix letters), only groups
    with data in the database are included.

    Example: [("A", "A — Medical & Surgical Supplies"), ("B", "B — Enteral/Parenteral Therapy"), ...]
    """
    choices = []
    for prefix, (short, _) in HCPCS_GROUPS.items():
        if only_prefixes is not None and prefix not in only_prefixes:
            continue
        choices.append((prefix, f"{prefix} — {short}"))
    return choices
