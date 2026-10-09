"""R21 fix 7: a tariff document for a different state / province.

``url_jurisdictions`` reads state/province markers from a source URL's path:
full names ("south-dakota", "Minnesota") and two-letter codes only in strict
forms (a path segment "/MN/", or a filename prefix "sd-electric-tariffs",
"ia_rates"). ``wrong_jurisdiction`` is True when the URL names one or more
jurisdictions and none is the utility's own. City names that contain a state
name ("Kansas City", "Virginia Beach", "Oklahoma City") are ignored.
"""
from __future__ import annotations

import re
from urllib.parse import unquote, urlparse

NAMES = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas", "CA": "california",
    "CO": "colorado", "CT": "connecticut", "DE": "delaware", "FL": "florida", "GA": "georgia",
    "HI": "hawaii", "ID": "idaho", "IL": "illinois", "IN": "indiana", "IA": "iowa",
    "KS": "kansas", "KY": "kentucky", "LA": "louisiana", "ME": "maine", "MD": "maryland",
    "MA": "massachusetts", "MI": "michigan", "MN": "minnesota", "MS": "mississippi",
    "MO": "missouri", "MT": "montana", "NE": "nebraska", "NV": "nevada", "NH": "new hampshire",
    "NJ": "new jersey", "NM": "new mexico", "NY": "new york", "NC": "north carolina",
    "ND": "north dakota", "OH": "ohio", "OK": "oklahoma", "OR": "oregon", "PA": "pennsylvania",
    "RI": "rhode island", "SC": "south carolina", "SD": "south dakota", "TN": "tennessee",
    "TX": "texas", "UT": "utah", "VT": "vermont", "VA": "virginia", "WA": "washington",
    "WV": "west virginia", "WI": "wisconsin", "WY": "wyoming", "DC": "district of columbia",
    "AB": "alberta", "BC": "british columbia", "MB": "manitoba", "NB": "new brunswick",
    "NL": "newfoundland", "NS": "nova scotia", "ON": "ontario", "PE": "prince edward island",
    "QC": "quebec", "SK": "saskatchewan",
}
_CITY_RE = re.compile(r"\b(?:kansas|oklahoma|carson|nevada|iowa|virginia|indiana|michigan|missouri|texas|delaware)\s+(?:city|beach|falls|valley)\b|\bwashington\s+(?:dc|d\s*c)\b")
# 2-letter codes that collide with common words: only as an UPPERCASE path segment.
_AMBIG = {"IN", "OR", "ME", "OH", "OK", "HI", "ID", "LA", "AL", "DE", "PA", "MA", "CO", "ON", "NE", "MS", "SC", "NB", "PE", "AR"}
_PREFIX_WORDS = r"(?:electric|elec|tariffs?|rates?|rate[-_ ]?book|schedules?|price)"


def url_jurisdictions(url: str) -> set[str]:
    path = unquote(urlparse(str(url or "")).path or "")
    words = re.sub(r"[-_./%+]+", " ", path).lower()
    words = _CITY_RE.sub(" ", words)
    found: set[str] = set()
    for code, name in sorted(NAMES.items(), key=lambda kv: -len(kv[1])):
        if re.search(rf"\b{name}\b", words):
            found.add(code)
            words = re.sub(rf"\b{name}\b", " ", words)
    for seg in [s for s in path.split("/") if s]:
        if len(seg) == 2 and seg.upper() in NAMES and (seg.upper() not in _AMBIG or seg.isupper()):
            found.add(seg.upper())
    fname = path.rsplit("/", 1)[-1]
    m = re.match(rf"^([a-z]{{2}})[-_]{_PREFIX_WORDS}", fname, re.I)
    if m and m.group(1).upper() in NAMES and m.group(1).upper() not in _AMBIG:
        found.add(m.group(1).upper())
    return found


def wrong_jurisdiction(url: str, utility_state: str | None) -> bool:
    st = str(utility_state or "").strip().upper()
    if st not in NAMES:
        return False
    found = url_jurisdictions(url)
    return bool(found) and st not in found
