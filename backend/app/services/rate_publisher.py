"""R23: utilities whose residential rates are set and published by another
official body (a publisher of record) — accepted, and flagged as such.

Hydro-Sherbrooke is a municipal distributor. Its residential rates are
identical to Hydro-Québec's (Régie de l'énergie, dossier R-4183-2021,
B-0033: "Les tarifs domestiques et généraux d'Hydro-Sherbrooke sont
identiques à ceux d'Hydro-Québec"), and its own tariff regulation
(Règlement no 425) is published by the regulator, the Régie de l'énergie.
So pages from hydroquebec.com (publisher of record) or regie-energie.qc.ca
(regulator) are official sources for it even though they never name
Hydro-Sherbrooke. Add an entry only with such documentary evidence.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

PUBLISHERS_OF_RECORD: list[dict] = [
    {
        "utility_re": re.compile(r"^\s*hydro[\s-]*sherbrooke\b", re.I),
        "state": "QC",
        "publisher": "Hydro-Québec",
        "publisher_domains": ("hydroquebec.com",),
        "regulator": "Régie de l'énergie du Québec",
        "regulator_domains": ("regie-energie.qc.ca",),
        "evidence": ("Régie de l'énergie R-4183-2021-B-0033: residential and general rates of Hydro-Sherbrooke "
                     "are identical to Hydro-Québec's; Règlement no 425 published by the Régie"),
        "evidence_url": ("https://www.regie-energie.qc.ca/fr/participants/dossiers/R-4183-2021/doc/"
                         "R-4183-2021-B-0033-Demande-Piece-2022_04_21.pdf"),
    },
]


def publisher_of_record(utility_name: str, state: str = "") -> dict | None:
    for e in PUBLISHERS_OF_RECORD:
        if e["utility_re"].search(utility_name or "") and (not state or state.upper() == e["state"]):
            return e
    return None


def _in(host: str, domains) -> bool:
    host = (host or "").lower().removeprefix("www.")
    return any(host == d or host.endswith("." + d) for d in domains)


def source_kind(url: str, entry: dict) -> str | None:
    """'publisher_of_record' / 'regulator' when ``url`` is one of the entry's
    official sources, else None."""
    h = urlparse(url or "").netloc
    if _in(h, entry["publisher_domains"]):
        return "publisher_of_record"
    if _in(h, entry["regulator_domains"]):
        return "regulator"
    return None


def identity_override(pages, utility_name: str, state: str) -> dict | None:
    """When every fetched page comes from the utility's publisher of record or
    its regulator, return the entry (caller keeps the tariffs and flags them)."""
    e = publisher_of_record(utility_name, state)
    urls = [getattr(p, "url", "") for p in pages or [] if getattr(p, "url", "")]
    if not e or not urls:
        return None
    return e if all(source_kind(u, e) for u in urls) else None


def flag_tariffs(tariffs, entry: dict) -> None:
    """Mark tariffs as coming from the publisher of record / regulator."""
    for t in tariffs or []:
        kind = source_kind(getattr(t, "source_url", "") or "", entry) or "publisher_of_record"
        notes = dict(getattr(t, "confidence_notes", None) or {})
        notes["rate_source"] = kind
        notes["rate_publisher"] = entry["regulator"] if kind == "regulator" else entry["publisher"]
        notes["rate_publisher_evidence"] = entry["evidence"]
        notes["rate_publisher_evidence_url"] = entry["evidence_url"]
        t.confidence_notes = notes
