"""IANA timezone per utility.

TOU clock windows are local wall-clock time at the service address. A
consumer that knows the address's own zone (a Mysa device does) should use
that. ``utility_timezone`` is the fallback: an explicit
``utilities.timezone`` override, else the zone of a state/province whose
whole territory observes a single IANA zone. Multi-zone jurisdictions
(TX, FL, KY, TN, IN, MI, KS, NE, ND, SD, ID, OR, NV, AK; ON, QC, BC, NL,
SK, NU) return None until an operator sets the override.
"""
from __future__ import annotations

from typing import Any

US_SINGLE_ZONE = {
    **dict.fromkeys(
        ("CT", "DE", "DC", "GA", "ME", "MD", "MA", "NH", "NJ", "NY", "NC", "OH",
         "PA", "RI", "SC", "VT", "VA", "WV"),
        "America/New_York",
    ),
    **dict.fromkeys(
        ("AL", "AR", "IL", "IA", "LA", "MN", "MS", "MO", "OK", "WI"),
        "America/Chicago",
    ),
    **dict.fromkeys(("CO", "MT", "NM", "UT", "WY"), "America/Denver"),
    "CA": "America/Los_Angeles",
    "WA": "America/Los_Angeles",
    "HI": "Pacific/Honolulu",
    # Arizona does not observe DST, except the Navajo Nation (see below).
    "AZ": "America/Phoenix",
    "PR": "America/Puerto_Rico",
}

CA_SINGLE_ZONE = {
    "NS": "America/Halifax",
    "NB": "America/Moncton",
    "PE": "America/Halifax",
    "MB": "America/Winnipeg",
    "AB": "America/Edmonton",
    "YT": "America/Whitehorse",
    "NT": "America/Edmonton",
}


def _val(v: Any) -> str:
    if v is None:
        return ""
    if hasattr(v, "value"):
        v = v.value
    return str(v).strip().upper()


def utility_timezone(utility: Any) -> tuple[str | None, str | None]:
    """(iana_zone, source) where source is 'utility' | 'state_default' | None."""
    override = getattr(utility, "timezone", None)
    if override:
        return override, "utility"
    country = _val(getattr(utility, "country", None))
    state = _val(getattr(utility, "state_province", None))
    table = US_SINGLE_ZONE if country == "US" else CA_SINGLE_ZONE if country == "CA" else {}
    zone = table.get(state)
    if zone == "America/Phoenix" and "navajo" in (getattr(utility, "name", "") or "").lower():
        return None, None
    return (zone, "state_default") if zone else (None, None)


def utility_currency(utility: Any) -> str | None:
    return {"US": "USD", "CA": "CAD"}.get(_val(getattr(utility, "country", None)))
