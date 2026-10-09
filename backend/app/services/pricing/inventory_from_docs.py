"""Populate closed-world rider inventory + typical-bill oracles from a
document set (PR R27-2).

G5 (rider census) and G6 (typical-bill cross-check) only fire when these
structures are non-empty. R27 found both tables empty — so G5/G6 never ran
and wrong accepted prices (FPL marketing PDF, NL old edition) slipped through.

No network I/O. Callers pass document-set members plus optional retained
text. Inventory entries are derived from:
  1. ``rider_sheet`` members (code from URL / title),
  2. "subject to Rider X" / exhibit listings inside tariff text,
  3. optional seed codes from a plan's known rider components (golden).

Typical-bill / published all-in tables are extracted as **oracles only**
(never stored as plan prices) for G6.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable

from app.services.pricing.document_set import DocumentMember, ROLE_RIDER_SHEET
from app.services.pricing.rider_census import InventoryRider
from app.services.pricing.types import money

# Rider kinds that belong on the closed census (not calendars / base energy).
_CENSUS_KINDS = frozenset({
    "rider_per_kwh",
    "rider_percent",
    "credit",
    "event_day",
    "excluded_item",
})

_SUBJECT_TO_RE = re.compile(
    r"(?:subject\s+to|plus|including)\s+(?:the\s+)?"
    r"(?:riders?|adjustments?|clauses?)\s+"
    r"([A-Za-z0-9][A-Za-z0-9\-/,\s&]{1,80}?)(?:\.|;|\n|$)",
    re.I,
)
_RIDER_TOKEN_RE = re.compile(
    r"\b(?:Rider|Schedule|Clause)\s+([A-Z][A-Z0-9\-]{0,12})\b"
)
_FILENAME_RIDER_RE = re.compile(
    r"(?:^|/)(?:rider[-_])?([a-z0-9][a-z0-9\-_]{1,20})"
    r"(?:[-_]rider)?(?:\.pdf)?(?:\?.*)?$",
    re.I,
)

# Typical / published all-in figures (oracle only).
_TYPICAL_BILL_RE = re.compile(
    r"(?:typical\s+bill|average\s+monthly\s+bill|sample\s+bill|"
    r"all[- ]?in(?:\s+price)?|total\s+(?:price|rate)\s+per\s+kwh)"
    r"[^\n]{0,120}?",
    re.I,
)
_CENTS_IN_LINE_RE = re.compile(
    r"(\d+\.\d{2,5})\s*(?:¢|cents?)\s*(?:per\s*)?(?:/)?\s*kwh",
    re.I,
)
_DOLLARS_IN_LINE_RE = re.compile(
    r"\$\s*(\d+\.\d{2,5})\s*(?:per\s*)?(?:/)?\s*kwh",
    re.I,
)
_KWH_RE = re.compile(r"(\d{3,5})\s*kwh", re.I)


@dataclass(frozen=True)
class TypicalBillOracle:
    """Published typical-bill / all-in figure used only as a G6 check."""

    cents_per_kwh: Decimal
    kwh: int | None = None
    source_quote: str = ""
    source_url: str | None = None
    label: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cents_per_kwh": str(self.cents_per_kwh),
            "kwh": self.kwh,
            "source_quote": self.source_quote,
            "source_url": self.source_url,
            "label": self.label,
        }


@dataclass
class InventoryBuildResult:
    inventory: list[InventoryRider] = field(default_factory=list)
    typical_bills: list[TypicalBillOracle] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "inventory": [
                {"code": r.code, "name": r.name, "kind": r.kind}
                for r in self.inventory
            ],
            "typical_bills": [t.to_dict() for t in self.typical_bills],
            "notes": list(self.notes),
        }


def _norm_code(raw: str) -> str:
    code = re.sub(r"[^A-Za-z0-9]+", "_", (raw or "").strip()).strip("_")
    return code.lower()


def rider_code_from_url(url: str, title: str | None = None) -> str | None:
    """Derive a census code from a rider-sheet URL or title."""
    path = (url or "").rstrip("/").split("/")[-1]
    path = re.sub(r"%20", "-", path)
    file_code = None
    if not re.search(r"tariff-book|rate-book|overview|index|rates\.html?", path, re.I):
        m = _FILENAME_RIDER_RE.search(path)
        if m:
            file_code = _norm_code(m.group(1))
            if file_code in {
                "tariff", "rates", "rate", "schedule", "residential", "electric",
                "pdf", "index", "shared", "section", "exhibit", "applicable",
                "riders", "entire", "filed", "about", "docs", "documents",
            }:
                file_code = None
            # Multi-word noise stems from exhibit PDFs.
            if file_code and file_code.startswith((
                "exhibit_", "entire_", "pro_rata_", "about_",
            )):
                file_code = None

    hay = (title or "").strip()
    title_code = None
    if hay:
        m = re.search(
            r"(?:rider|schedule|clause)\s+([A-Za-z0-9][A-Za-z0-9\-]{0,12})",
            hay,
            re.I,
        )
        if m:
            title_code = _norm_code(m.group(1))
        else:
            # First token is usually the rider code ("ECCR", "FCR fuel", "DSM-R").
            token = re.match(r"^([A-Za-z0-9][A-Za-z0-9\-]{0,12})\b", hay)
            if token:
                title_code = _norm_code(token.group(1))
            # Descriptive titles with no code token ("Fuel deferral") — use
            # the whole phrase so the filename can still win if richer.
            if title_code and " " in hay and len(title_code) <= 4:
                pass  # keep short code token
            elif " " in hay and len(hay) <= 40 and not title_code:
                title_code = _norm_code(hay)

    # Prefer the longer of title vs filename (fuel_deferral > fuel).
    candidates = [c for c in (file_code, title_code) if c]
    if not candidates:
        return None
    return max(candidates, key=len)


def riders_from_document_members(
    members: Iterable[DocumentMember] | Iterable[dict[str, Any]],
) -> list[InventoryRider]:
    """Build inventory rows from selected ``rider_sheet`` members."""
    out: list[InventoryRider] = []
    seen: set[str] = set()
    for raw in members:
        if isinstance(raw, dict):
            role = raw.get("role")
            url = raw.get("url") or ""
            title = raw.get("title")
            selected = raw.get("is_selected", True)
            rejected = raw.get("reject_reason")
        else:
            role = raw.role
            url = raw.url
            title = raw.title
            selected = raw.is_selected
            rejected = raw.reject_reason
        if role != ROLE_RIDER_SHEET or not selected or rejected:
            continue
        code = rider_code_from_url(url, title)
        if not code or code in seen:
            continue
        seen.add(code)
        kind = "rider_percent" if "percent" in (title or "").lower() or "pct" in code else "rider_per_kwh"
        out.append(InventoryRider(code=code, name=title or code, kind=kind))
    return out


def riders_from_text(document_text: str) -> list[InventoryRider]:
    """Pull rider codes from 'subject to Rider …' / Rider TOKEN mentions."""
    if not document_text:
        return []
    seen: set[str] = set()
    out: list[InventoryRider] = []

    def _add(code: str, name: str) -> None:
        c = _norm_code(code)
        if not c or c in seen or len(c) < 2:
            return
        seen.add(c)
        out.append(InventoryRider(code=c, name=name.strip() or c, kind="rider_per_kwh"))

    for m in _SUBJECT_TO_RE.finditer(document_text):
        chunk = m.group(1)
        # Split on commas / and / &
        parts = re.split(r",|\band\b|&", chunk)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            tok = re.match(
                r"(?:Rider|Schedule|Clause)?\s*([A-Za-z][A-Za-z0-9\-]{1,12})",
                part,
                re.I,
            )
            if tok:
                _add(tok.group(1), part)

    for m in _RIDER_TOKEN_RE.finditer(document_text):
        _add(m.group(1), m.group(0))

    return out


def riders_from_plan_components(
    components: Iterable[dict[str, Any]],
) -> list[InventoryRider]:
    """Seed inventory from golden / extracted components of census kinds."""
    out: list[InventoryRider] = []
    seen: set[str] = set()
    for c in components:
        kind = str(c.get("kind") or "")
        if kind not in _CENSUS_KINDS:
            continue
        code = _norm_code(str(c.get("code") or ""))
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(InventoryRider(
            code=code,
            name=str(c.get("name") or code),
            kind=kind,
        ))
    return out


def merge_inventory(*groups: Iterable[InventoryRider]) -> list[InventoryRider]:
    """Union by code; first occurrence wins (keeps richer name/kind)."""
    out: list[InventoryRider] = []
    seen: set[str] = set()
    for group in groups:
        for r in group:
            if r.code in seen:
                continue
            seen.add(r.code)
            out.append(r)
    return out


def extract_typical_bill_oracles(
    document_text: str,
    *,
    source_url: str | None = None,
) -> list[TypicalBillOracle]:
    """Find published typical-bill / all-in ¢/kWh figures for G6."""
    if not document_text:
        return []
    oracles: list[TypicalBillOracle] = []
    lines = document_text.splitlines()
    for i, line in enumerate(lines):
        if not _TYPICAL_BILL_RE.search(line):
            # Also accept a nearby header: check window of ±1 line.
            window = " ".join(lines[max(0, i - 1): i + 2])
            if not _TYPICAL_BILL_RE.search(window):
                continue
        hay = " ".join(lines[max(0, i - 1): i + 2])
        cents = None
        m = _CENTS_IN_LINE_RE.search(hay)
        if m:
            cents = money(m.group(1))
        else:
            m = _DOLLARS_IN_LINE_RE.search(hay)
            if m:
                cents = money(m.group(1)) * Decimal("100")
        if cents is None:
            continue
        kwh_m = _KWH_RE.search(hay)
        kwh = int(kwh_m.group(1)) if kwh_m else None
        quote = line.strip()[:200]
        oracles.append(TypicalBillOracle(
            cents_per_kwh=cents,
            kwh=kwh,
            source_quote=quote,
            source_url=source_url,
            label="typical_bill",
        ))
    # Dedupe by (cents, kwh)
    uniq: list[TypicalBillOracle] = []
    seen: set[tuple] = set()
    for o in oracles:
        key = (str(o.cents_per_kwh), o.kwh)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(o)
    return uniq


def build_inventory_from_document_set(
    *,
    members: Iterable[DocumentMember] | Iterable[dict[str, Any]],
    document_texts: Iterable[tuple[str | None, str]] | None = None,
    plan_components: Iterable[dict[str, Any]] | None = None,
) -> InventoryBuildResult:
    """Assemble inventory + typical-bill oracles for G5/G6.

    ``document_texts`` is an iterable of ``(source_url, text)``.
    """
    from_sheets = riders_from_document_members(members)
    from_text: list[InventoryRider] = []
    typical: list[TypicalBillOracle] = []
    notes: list[str] = []

    for url, text in (document_texts or []):
        found = riders_from_text(text or "")
        from_text.extend(found)
        typical.extend(extract_typical_bill_oracles(text or "", source_url=url))

    from_plan = riders_from_plan_components(plan_components or [])
    inventory = merge_inventory(from_sheets, from_text, from_plan)
    if from_sheets:
        notes.append(f"from_rider_sheets:{len(from_sheets)}")
    if from_text:
        notes.append(f"from_text_refs:{len(from_text)}")
    if from_plan:
        notes.append(f"from_plan_components:{len(from_plan)}")
    if typical:
        notes.append(f"typical_bills:{len(typical)}")
    return InventoryBuildResult(
        inventory=inventory,
        typical_bills=typical,
        notes=notes,
    )


def dispositions_from_plan_components(
    components: Iterable[dict[str, Any]],
) -> list[dict[str, str]]:
    """Map golden component dispositions onto census DispositionInput dicts."""
    out = []
    for c in components:
        kind = str(c.get("kind") or "")
        if kind not in _CENSUS_KINDS:
            continue
        code = _norm_code(str(c.get("code") or ""))
        if not code:
            continue
        out.append({
            "rider_code": code,
            "disposition": str(c.get("disposition") or "applies"),
            "disposition_page": str(c.get("source_page") or "p.golden"),
            "disposition_quote": str(
                c.get("source_quote") or c.get("name") or code
            ),
        })
    return out


__all__ = [
    "InventoryBuildResult",
    "TypicalBillOracle",
    "build_inventory_from_document_set",
    "dispositions_from_plan_components",
    "extract_typical_bill_oracles",
    "merge_inventory",
    "rider_code_from_url",
    "riders_from_document_members",
    "riders_from_plan_components",
    "riders_from_text",
]
