"""Closed-world rider census (PR B / G5).

A plan cannot be Mysa-complete or computable until every live rider on its
utility's inventory has a cited disposition (applies / not_applicable /
optional / location_fee_or_tax / event_day).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

VALID_DISPOSITIONS = frozenset({
    "applies",
    "not_applicable",
    "not_found",  # searched the retained docs; value absent (R28)
    "optional",
    "location_fee_or_tax",
    "event_day",
})

# Dispositions that close a census row without feeding the price compiler.
UNPRICED_DISPOSITIONS = frozenset({
    "not_applicable",
    "not_found",
    "optional",
    "location_fee_or_tax",
    "event_day",
})


@dataclass(frozen=True)
class InventoryRider:
    code: str
    name: str
    kind: str = "rider_per_kwh"


@dataclass(frozen=True)
class DispositionInput:
    rider_code: str
    disposition: str
    disposition_page: str | None = None
    disposition_quote: str | None = None


@dataclass
class CensusResult:
    complete: bool
    missing_codes: list[str] = field(default_factory=list)
    invalid: list[str] = field(default_factory=list)  # bad disposition / no cite
    reasons: list[str] = field(default_factory=list)

    @property
    def mysa_complete_eligible(self) -> bool:
        """Census must be closed before Mysa-complete / computable."""
        return self.complete


def evaluate_rider_census(
    inventory: Iterable[InventoryRider],
    dispositions: Iterable[DispositionInput],
    *,
    require_citation: bool = True,
) -> CensusResult:
    """Return whether every inventory rider has a valid cited disposition."""
    inv = list(inventory)
    by_code = {d.rider_code: d for d in dispositions}
    missing: list[str] = []
    invalid: list[str] = []
    reasons: list[str] = []

    for entry in inv:
        d = by_code.get(entry.code)
        if d is None:
            missing.append(entry.code)
            reasons.append(f"census_gap:{entry.code}")
            continue
        if d.disposition not in VALID_DISPOSITIONS:
            invalid.append(entry.code)
            reasons.append(f"invalid_disposition:{entry.code}:{d.disposition}")
            continue
        # not_found / not_applicable may lack a value quote; still need a
        # page tag (or the synthetic "not_found" marker) so the census is
        # auditable. Other dispositions keep the full citation rule.
        if require_citation:
            if d.disposition in {"not_found", "not_applicable"}:
                if not (d.disposition_page and str(d.disposition_page).strip()):
                    invalid.append(entry.code)
                    reasons.append(f"missing_disposition_page:{entry.code}")
                    continue
            else:
                if not (d.disposition_quote and str(d.disposition_quote).strip()):
                    invalid.append(entry.code)
                    reasons.append(f"missing_disposition_quote:{entry.code}")
                    continue
                if not (d.disposition_page and str(d.disposition_page).strip()):
                    invalid.append(entry.code)
                    reasons.append(f"missing_disposition_page:{entry.code}")
                    continue

    # Dispositions for unknown inventory codes are non-blocking warnings;
    # they do not open the census, but they do not close a gap either.
    inv_codes = {e.code for e in inv}
    for code, d in by_code.items():
        if code not in inv_codes:
            reasons.append(f"disposition_without_inventory:{code}")

    complete = not missing and not invalid
    return CensusResult(
        complete=complete,
        missing_codes=missing,
        invalid=invalid,
        reasons=reasons,
    )


def is_live_inventory_row(
    *,
    superseded_by_entry_id: int | None,
    supersede_reason: str | None,
) -> bool:
    return superseded_by_entry_id is None and supersede_reason is None
