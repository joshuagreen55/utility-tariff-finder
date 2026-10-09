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


_MONTH_ALT = (
    r"jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|"
    r"july?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|"
    r"nov(?:ember)?|dec(?:ember)?"
)
_YMD_RE = re.compile(r"(?<!\d)(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})(?!\d)")
_MDY_RE = re.compile(r"(?<!\d)(\d{1,2})[-_/](\d{1,2})[-_/](20\d{2})(?!\d)")
_MDYY_RE = re.compile(r"(?<!\d)(\d{1,2})[-_/](\d{1,2})[-_/](\d{2})(?!\d)")
_MONTH_DAY_YEAR_RE = re.compile(
    rf"(?<![a-z])({_MONTH_ALT})\.?[_\s\-]+(\d{{1,2}})(?:st|nd|rd|th)?,?[_\s\-]+(20\d{{2}})(?!\d)",
    re.I,
)
_MONTH_YEAR_RE = re.compile(rf"(?<![a-z])({_MONTH_ALT})[_\s\-]*(20\d{{2}})(?!\d)", re.I)
# A date in document text counts only when the text says it is the effective
# date; tariff text is full of filed / issued / advice-letter / decision dates.
_EFFECTIVE_ANCHOR_RE = re.compile(
    r"(?:\beffective\b|\beff\.|\bin effect\b|\ben vigueur\b)[^.\n]{0,40}$", re.I,
)


def _month_num(token: str) -> int | None:
    key = token.lower().rstrip(".")
    return _MONTHS.get(key) or _MONTHS.get(key[:3])


def _numeric_md(a: int, b: int) -> tuple[int, int] | None:
    """(month, day) from ``a/b``; None when month-first vs day-first is ambiguous."""
    if a != b and a <= 12 and b <= 12:
        return None
    if a <= 12:
        return a, b
    if b <= 12:
        return b, a
    return None


def _dates_in(text: str) -> list[tuple[int, date]]:
    """Every full date in ``text`` with its start offset, plus month-year as day 1."""
    out: list[tuple[int, date]] = []
    spans: list[tuple[int, int]] = []

    def add(pos: int, end: int, y: int, mo: int, d: int) -> None:
        if any(s <= pos < e for s, e in spans):
            return
        if 1 <= mo <= 12 and 2000 <= y <= 2100:
            try:
                out.append((pos, date(y, mo, d)))
                spans.append((pos, end))
            except ValueError:
                pass

    for m in _YMD_RE.finditer(text):
        add(m.start(), m.end(), int(m.group(1)), int(m.group(2)), int(m.group(3)))
    for m in _MONTH_DAY_YEAR_RE.finditer(text):
        mo = _month_num(m.group(1))
        if mo:
            add(m.start(), m.end(), int(m.group(3)), mo, int(m.group(2)))
    for pat, century in ((_MDY_RE, 0), (_MDYY_RE, 2000)):
        for m in pat.finditer(text):
            md = _numeric_md(int(m.group(1)), int(m.group(2)))
            if md:
                add(m.start(), m.end(), century + int(m.group(3)), md[0], md[1])
    for m in _MONTH_YEAR_RE.finditer(text):
        mo = _month_num(m.group(1))
        if mo:
            add(m.start(), m.end(), int(m.group(2)), mo, 1)
    return sorted(out, key=lambda t: t[0])


def parse_effective_date(
    *,
    url: str = "",
    title: str | None = None,
    edition_label: str | None = None,
    text_snippet: str | None = None,
) -> date | None:
    """Best-effort date from URL / label / snippet. Never invents.

    URL, title and edition label name the edition, so any full date there
    counts. In document text only a date introduced by "effective" counts.
    Numeric dates whose month/day order is ambiguous (``06/07/2026``) are
    skipped rather than guessed.
    """
    for field_text in (edition_label or "", title or "", url or ""):
        found = _dates_in(field_text)
        if found:
            return found[0][1]
    snippet = (text_snippet or "")[:1500]
    for pos, d in _dates_in(snippet):
        if _EFFECTIVE_ANCHOR_RE.search(snippet[max(0, pos - 60):pos]):
            return d
    return None


_SUPPLY_RECIPES = frozenset({"deregulated", "provincial_ontario", "provincial_alberta"})
_DEFAULT_SUPPLY_RE = re.compile(
    r"\brate[\s-]of[\s-]last[\s-]resort\b|\brolr\b|\brro\b|\bdefault[\s-]supply\b"
    r"|\bprice[\s-]to[\s-]compare\b|\bstandard[\s-]offer\b"
    r"|\bbasic[\s-]generation[\s-]service\b"
    r"|\bbasic[\s-]service\b(?![\s-]*(?:charge|fee|customer charge))"
    r"|\bprovider[\s-]of[\s-]last[\s-]resort\b"
)


def _host_is(host: str, domain: str) -> bool:
    return host == domain or host.endswith("." + domain)


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
    host = publisher_host(candidate.url or "")
    on_board_host = _host_is(host, "oeb.ca") or _host_is(host, "auc.ab.ca")

    if "billdata" in hay or "bill calculator" in hay:
        return ROLE_DELIVERY
    if _host_is(host, "oeb.ca") and ("electricity-rates" in hay or re.search(r"\brpp\b", hay)):
        return ROLE_PROVINCIAL_COMMODITY
    # Bundled / Texas TDU sets have no supply document; a bundled tariff that
    # prints "Basic Service Charge" or "standard offer" is still the tariff.
    if recipe_code in _SUPPLY_RECIPES and _DEFAULT_SUPPLY_RE.search(hay):
        if recipe_code == "provincial_ontario":
            return ROLE_PROVINCIAL_COMMODITY
        return ROLE_DEFAULT_SUPPLY
    if re.search(r"rider[-_]|/rider|/fuel|/eccr|/fcr|/dsm", url):
        return ROLE_RIDER_SHEET
    if recipe_code == "texas_tdu":
        return ROLE_DELIVERY
    if recipe_code in _SUPPLY_RECIPES and not on_board_host:
        # Non-commodity utility-hosted docs are delivery for those recipes.
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
