"""Per-utility pricing document-set builder (PR R27-1).

Assembles the official current tariff, referenced rider sheets, default
supply (deregulated), provincial commodity (ON/AB), and delivery charges.
Chooses the current edition (effective date ≤ as-of), records dates/URLs,
and demotes marketing PDFs when a rates PDF exists for the same role.

No network I/O — callers supply candidate documents (URL + optional text
snippet / dates). Fetching stays in the harness / pipeline.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

GOLDEN_DIR = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "pricing_golden"

# Role strings — keep in sync with ``DocumentRole`` in app.models.pricing.
ROLE_TARIFF = "tariff"
ROLE_RIDER_SHEET = "rider_sheet"
ROLE_DEFAULT_SUPPLY = "default_supply"
ROLE_PROVINCIAL_COMMODITY = "provincial_commodity"
ROLE_DELIVERY = "delivery"
ROLE_TYPICAL_BILL = "typical_bill"

# Roles required before a document set is "complete" for a recipe.
_REQUIRED_ROLES: dict[str, frozenset[str]] = {
    "bundled": frozenset({ROLE_TARIFF}),
    "deregulated": frozenset({ROLE_DELIVERY, ROLE_DEFAULT_SUPPLY}),
    "texas_tdu": frozenset({ROLE_DELIVERY}),
    "provincial_ontario": frozenset({ROLE_PROVINCIAL_COMMODITY, ROLE_DELIVERY}),
    "provincial_alberta": frozenset({ROLE_DEFAULT_SUPPLY, ROLE_DELIVERY}),
}

# URL / title tokens that mark marketing overviews (not rate sheets).
_MARKETING_MARKERS = (
    "new-customer-overview",
    "customer-overview",
    "residential-explanation",
    "marketing",
    "brochure",
    "faq",
)

# Tokens that mark a real rates / tariff document.
_RATES_MARKERS = (
    "tariff",
    "rate-book",
    "rates-",
    "rate_",
    "schedule",
    "sched_",
    "section_",
    "rider",
    "fuel",
    "rolr",
    "rpp",
    "billdata",
)

_MONTHS = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6,
    "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "sept": 9,
    "september": 9, "oct": 10, "october": 10, "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


@dataclass
class DocumentCandidate:
    """One candidate URL considered for a document set."""

    url: str
    role: str | None = None  # when known a priori (golden / curated)
    title: str | None = None
    edition_label: str | None = None
    effective_date: date | None = None
    text_snippet: str | None = None  # first ~2k chars for classification
    publisher_host: str | None = None
    content_hash: str | None = None
    retrieved_at: datetime | None = None


@dataclass
class DocumentMember:
    role: str
    url: str
    title: str | None = None
    edition_label: str | None = None
    effective_date: date | None = None
    publisher_host: str | None = None
    is_selected: bool = True
    reject_reason: str | None = None
    content_hash: str | None = None
    retrieved_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "url": self.url,
            "title": self.title,
            "edition_label": self.edition_label,
            "effective_date": (
                self.effective_date.isoformat() if self.effective_date else None
            ),
            "publisher_host": self.publisher_host,
            "is_selected": self.is_selected,
            "reject_reason": self.reject_reason,
            "content_hash": self.content_hash,
        }


@dataclass
class DocumentSetResult:
    utility_name: str | None
    recipe_code: str
    as_of_date: date
    members: list[DocumentMember] = field(default_factory=list)
    missing_roles: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        return not self.missing_roles

    def selected(self) -> list[DocumentMember]:
        return [m for m in self.members if m.is_selected and not m.reject_reason]

    def selected_urls(self) -> list[str]:
        return [m.url for m in self.selected()]

    def to_dict(self) -> dict[str, Any]:
        return {
            "utility_name": self.utility_name,
            "recipe_code": self.recipe_code,
            "as_of_date": self.as_of_date.isoformat(),
            "complete": self.complete,
            "missing_roles": list(self.missing_roles),
            "notes": list(self.notes),
            "documents": [m.to_dict() for m in self.members],
        }


def required_roles(recipe_code: str) -> frozenset[str]:
    return _REQUIRED_ROLES.get(recipe_code, frozenset({ROLE_TARIFF}))


def publisher_host(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return ""
    return host[4:] if host.startswith("www.") else host


def is_marketing_url(url: str, title: str | None = None) -> bool:
    hay = f"{url} {title or ''}".lower()
    return any(m in hay for m in _MARKETING_MARKERS)


def looks_like_rates_url(url: str, title: str | None = None) -> bool:
    hay = f"{url} {title or ''}".lower()
    return any(m in hay for m in _RATES_MARKERS)


def parse_effective_date(
    *,
    url: str = "",
    title: str | None = None,
    edition_label: str | None = None,
    text_snippet: str | None = None,
) -> date | None:
    """Best-effort date from URL / label / snippet. Never invents."""
    hay = " ".join(
        x for x in (url, title or "", edition_label or "", (text_snippet or "")[:1500])
        if x
    )
    # ISO-ish in URL: 2026-05-01, 2026_07_01, 08-01-2026, 8-1-26
    patterns = [
        re.compile(r"(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})"),
        re.compile(r"(?<!\d)(\d{1,2})[-_/](\d{1,2})[-_/](20\d{2})(?!\d)"),
        re.compile(r"(?<!\d)(\d{1,2})[-_/](\d{1,2})[-_/](\d{2})(?!\d)"),
    ]
    for pat in patterns:
        m = pat.search(hay)
        if not m:
            continue
        a, b, c = m.groups()
        try:
            if len(a) == 4:  # Y-M-D
                y, mo, d = int(a), int(b), int(c)
            elif len(c) == 4:  # M-D-Y
                mo, d, y = int(a), int(b), int(c)
            else:  # M-D-YY
                mo, d, y = int(a), int(b), 2000 + int(c)
            if 1 <= mo <= 12 and 1 <= d <= 31 and 2000 <= y <= 2100:
                return date(y, mo, d)
        except ValueError:
            continue

    # Month name + year: Jul_2026, October 2026, May 2026 tariff
    m = re.search(
        r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
        r"nov(?:ember)?|dec(?:ember)?)[_\s\-]+(20\d{2})",
        hay,
        re.I,
    )
    if m:
        key = m.group(1).lower()
        mo = _MONTHS.get(key) or _MONTHS.get(key[:3])
        if mo:
            return date(int(m.group(2)), mo, 1)

    # Compact: oct2026, jul2026
    m = re.search(
        r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)(20\d{2})",
        hay,
        re.I,
    )
    if m:
        return date(int(m.group(2)), _MONTHS[m.group(1).lower()], 1)
    return None


def classify_role(
    candidate: DocumentCandidate,
    *,
    recipe_code: str,
) -> str:
    """Assign a document role from URL/title/snippet when not already set."""
    if candidate.role:
        return candidate.role
    url = (candidate.url or "").lower()
    title = (candidate.title or "").lower()
    snippet = (candidate.text_snippet or "")[:2000].lower()
    hay = f"{url} {title} {snippet}"

    if "billdata" in hay or "bill calculator" in hay:
        return ROLE_DELIVERY
    if "oeb.ca" in hay and ("electricity-rates" in hay or "rpp" in hay):
        return ROLE_PROVINCIAL_COMMODITY
    if any(t in hay for t in (
        "rate-of-last-resort", "rate of last resort", "rolr", "rro",
        "default supply", "price to compare", "standard offer",
        "basic generation service", "provider of last resort",
    )):
        if recipe_code == "provincial_ontario":
            return ROLE_PROVINCIAL_COMMODITY
        return ROLE_DEFAULT_SUPPLY
    if any(t in hay for t in ("rider", "fuel-deferral", "exhibit-of-applicable",
                              "eccr", "fcr", "dsm-r", "fam", "pca")):
        # Rider sheets named explicitly; tariff books also contain "rider"
        # but prefer rider_sheet when the path looks sheet-like.
        if re.search(r"rider[-_]|/rider|/fuel|/eccr|/fcr|/dsm", url):
            return ROLE_RIDER_SHEET
    if recipe_code == "texas_tdu":
        return ROLE_DELIVERY
    if recipe_code in {"provincial_ontario", "provincial_alberta", "deregulated"}:
        # Non-commodity utility-hosted docs are delivery for those recipes.
        if "oeb.ca" not in hay and "auc.ab.ca" not in hay:
            if recipe_code == "provincial_alberta" and "rolr" not in hay:
                return ROLE_DELIVERY
            if recipe_code != "provincial_alberta":
                return ROLE_DELIVERY
    return ROLE_TARIFF


def choose_current_edition(
    candidates: list[DocumentMember],
    *,
    as_of: date,
) -> list[DocumentMember]:
    """Mark the newest effective_date ≤ as_of as selected; demote older."""
    dated = [c for c in candidates if c.effective_date and c.effective_date <= as_of]
    if not dated:
        # No parseable dates — keep all selected (caller may still demote marketing).
        return candidates
    best = max(c.effective_date for c in dated)  # type: ignore[type-var]
    out: list[DocumentMember] = []
    for c in candidates:
        if c.reject_reason:
            out.append(c)
            continue
        if c.effective_date is None:
            out.append(c)
            continue
        if c.effective_date > as_of:
            out.append(DocumentMember(
                **{**c.__dict__, "is_selected": False,
                   "reject_reason": "future_edition"},
            ))
        elif c.effective_date < best:
            out.append(DocumentMember(
                **{**c.__dict__, "is_selected": False,
                   "reject_reason": "superseded_edition"},
            ))
        else:
            out.append(DocumentMember(**{**c.__dict__, "is_selected": True}))
    return out


def demote_marketing(members: list[DocumentMember]) -> list[DocumentMember]:
    """When a rates PDF exists for a role, reject marketing overviews."""
    by_role: dict[str, list[DocumentMember]] = {}
    for m in members:
        by_role.setdefault(m.role, []).append(m)
    out: list[DocumentMember] = []
    for role, group in by_role.items():
        has_rates = any(
            looks_like_rates_url(m.url, m.title) and not is_marketing_url(m.url, m.title)
            for m in group
            if not m.reject_reason
        )
        for m in group:
            if (
                has_rates
                and is_marketing_url(m.url, m.title)
                and not m.reject_reason
            ):
                out.append(DocumentMember(
                    **{**m.__dict__, "is_selected": False,
                       "reject_reason": "marketing_pdf"},
                ))
            else:
                out.append(m)
    return out


def build_document_set(
    candidates: Iterable[DocumentCandidate],
    *,
    recipe_code: str,
    as_of: date | None = None,
    utility_name: str | None = None,
) -> DocumentSetResult:
    """Classify, date, select current editions, and report missing roles."""
    as_of = as_of or date.today()
    members: list[DocumentMember] = []
    for cand in candidates:
        role = classify_role(cand, recipe_code=recipe_code)
        eff = cand.effective_date or parse_effective_date(
            url=cand.url,
            title=cand.title,
            edition_label=cand.edition_label,
            text_snippet=cand.text_snippet,
        )
        host = cand.publisher_host or publisher_host(cand.url)
        members.append(DocumentMember(
            role=role,
            url=cand.url,
            title=cand.title,
            edition_label=cand.edition_label,
            effective_date=eff,
            publisher_host=host,
            content_hash=cand.content_hash,
            retrieved_at=cand.retrieved_at,
        ))

    # Per-role edition selection (rider sheets keep all selected unless dated supersession).
    final: list[DocumentMember] = []
    by_role: dict[str, list[DocumentMember]] = {}
    for m in members:
        by_role.setdefault(m.role, []).append(m)
    for role, group in by_role.items():
        if role == ROLE_RIDER_SHEET:
            # Keep every rider sheet; still demote future editions.
            for m in group:
                if m.effective_date and m.effective_date > as_of:
                    final.append(DocumentMember(
                        **{**m.__dict__, "is_selected": False,
                           "reject_reason": "future_edition"},
                    ))
                else:
                    final.append(m)
        else:
            final.extend(choose_current_edition(group, as_of=as_of))

    final = demote_marketing(final)

    selected_roles = {
        m.role for m in final if m.is_selected and not m.reject_reason
    }
    missing = sorted(required_roles(recipe_code) - selected_roles)
    notes: list[str] = []
    if any(m.reject_reason == "marketing_pdf" for m in final):
        notes.append("demoted_marketing_pdf")
    if any(m.reject_reason == "superseded_edition" for m in final):
        notes.append("chose_current_edition")

    return DocumentSetResult(
        utility_name=utility_name,
        recipe_code=recipe_code,
        as_of_date=as_of,
        members=final,
        missing_roles=missing,
        notes=notes,
    )


def load_curated_document_sets(
    path: Path | None = None,
) -> dict[str, list[DocumentCandidate]]:
    """Load ``document_sets.json`` keyed by utility_name."""
    import json

    root = path or GOLDEN_DIR
    data = json.loads((root / "document_sets.json").read_text())
    out: dict[str, list[DocumentCandidate]] = {}
    for util, docs in (data.get("utilities") or {}).items():
        out[util] = [
            DocumentCandidate(
                url=d["url"],
                role=d.get("role"),
                title=d.get("title"),
                edition_label=d.get("edition_label"),
                effective_date=(
                    date.fromisoformat(d["effective_date"])
                    if d.get("effective_date") else None
                ),
            )
            for d in docs
        ]
    return out


def build_golden_document_set(
    utility_name: str,
    recipe_code: str,
    *,
    as_of: date | None = None,
    curated: dict[str, list[DocumentCandidate]] | None = None,
) -> DocumentSetResult:
    curated = curated or load_curated_document_sets()
    cands = curated.get(utility_name) or []
    return build_document_set(
        cands,
        recipe_code=recipe_code,
        as_of=as_of or date(2026, 10, 9),
        utility_name=utility_name,
    )


def r27_coverage_report(
    *,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Offline check: which R27 fetch-gap utilities now have complete sets."""
    import json

    as_of = as_of or date(2026, 10, 9)
    plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
    curated = load_curated_document_sets()
    # One set per utility (use first plan's recipe).
    by_util: dict[str, str] = {}
    for p in plans:
        by_util.setdefault(p["utility_name"], p["recipe_code"])

    rows = []
    for util, recipe in sorted(by_util.items()):
        result = build_golden_document_set(
            util, recipe, as_of=as_of, curated=curated
        )
        rows.append({
            "utility_name": util,
            "recipe_code": recipe,
            "complete": result.complete,
            "missing_roles": result.missing_roles,
            "selected_urls": result.selected_urls(),
            "n_selected": len(result.selected()),
            "notes": result.notes,
        })
    complete = sum(1 for r in rows if r["complete"])
    return {
        "as_of": as_of.isoformat(),
        "utilities": len(rows),
        "complete": complete,
        "incomplete": len(rows) - complete,
        "rows": rows,
    }


__all__ = [
    "DocumentCandidate",
    "DocumentMember",
    "DocumentSetResult",
    "GOLDEN_DIR",
    "build_document_set",
    "build_golden_document_set",
    "choose_current_edition",
    "classify_role",
    "demote_marketing",
    "is_marketing_url",
    "load_curated_document_sets",
    "looks_like_rates_url",
    "parse_effective_date",
    "publisher_host",
    "r27_coverage_report",
    "required_roles",
]
