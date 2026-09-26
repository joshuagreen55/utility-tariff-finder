"""Repair: Hydro-Québec residential tariffs sourced from callmepower.ca → HQ's own documents.

Issue #28. Live Rate D / DT / Flex D / DPC cite ``callmepower.ca`` (a rate
blog) while Rate DM / DP already cite HQ's rate book. This script moves the
third-party rows onto the official documents:

  Rate book (PDF): https://www.hydroquebec.com/data/documents-donnees/pdf/electricity-rates.pdf
  Rates page:      https://www.hydroquebec.com/residential/customer-space/rates/

No rate numbers are typed in here. For each live residential row whose
``source_url`` is on ``--source-host`` (default ``callmepower.ca``):

1. **verify-and-carry** (default): every component's ``rate_value`` (as
   dollars or as cents, "." or "," decimals) and every tier bound must be
   printed in the official PDF (preferred) or rates page. If all are, a new
   row carries the same components forward with the official
   ``source_url`` + document hash, and the old row is soft-superseded
   (``supersede_reason='source_repair'``, change event with the evidence).
2. **re-extract** (``--reextract``, spends LLM $): rows that fail step 1 are
   re-extracted from the official PDF with the production pipeline
   (Phase 3 + Phase 4). The extracted tariff with the matching name
   replaces the old row the same way; if nothing matches, or the new rows
   would lose computability, the old row is left live and reported.

Rows that verify in neither way stay live and are listed — never deleted.
Protected rows (approved / repair / manual) are skipped. The official PDF is
added as a monitoring source so future CHANGED signals point at it. Future
scrapes cannot re-attach callmepower: it is on the pipeline's
``THIRD_PARTY_DOMAINS`` hard-block (search, crawl, Phase 6) and
``store_tariffs`` holds a third-party extraction that would replace an
official row (``source_downgrade``).

Dry-run is the default; pass ``--apply`` to write. Idempotent: once no live
row cites the source host there is nothing to do.

Usage (inside the api container; long runs via ./deploy/run-on-vm.sh):
  python -m scripts.repair_hq_official_source                 # dry run, report
  python -m scripts.repair_hq_official_source --apply         # carry verified rows
  python -m scripts.repair_hq_official_source --reextract     # dry run incl. LLM re-extract
  python -m scripts.repair_hq_official_source --reextract --apply
  python -m scripts.repair_hq_official_source --pdf-file /tmp/electricity-rates.pdf
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.session import get_sync_engine
from app.models import Tariff, Utility
from app.models.tariff import CustomerClass, RateComponent, RateType
from app.services.monitor import stable_text_hash
from app.services.source_type import normalize_host
from app.services.tariff_history import is_protected, serialize_components, supersede_tariff

SCRIPT = "repair_hq_official_source"
HQ_UTILITY_ID = 1737
HQ_RATES_PDF_URL = "https://www.hydroquebec.com/data/documents-donnees/pdf/electricity-rates.pdf"
HQ_RATES_PAGE_URL = "https://www.hydroquebec.com/residential/customer-space/rates/"
DEFAULT_SOURCE_HOST = "callmepower.ca"
SUPERSEDE_REASON = "source_repair"

_COMPONENT_COPY_FIELDS = (
    "component_type", "unit", "rate_value", "tier_min_kwh", "tier_max_kwh",
    "tier_label", "period_index", "period_label", "period_start_time",
    "period_end_time", "day_type", "season", "season_start_month",
    "season_start_day", "season_end_month", "season_end_day", "adjustment",
    "included_in_energy",
)
_TARIFF_COPY_FIELDS = (
    "name", "code", "customer_class", "rate_type", "is_default", "description",
    "effective_date", "end_date", "energy_schedule_weekday",
    "energy_schedule_weekend", "demand_schedule_weekday", "demand_schedule_weekend",
)
_NUMBER_RE = re.compile(r"(?<![\d.,])\d+(?:[.,]\d+)?")


# ---------------------------------------------------------------------------
# Verification (pure)
# ---------------------------------------------------------------------------

def document_numbers(text: str) -> set[Decimal]:
    """Every number printed in a document, normalized ("6,905" == "6.905")."""
    out: set[Decimal] = set()
    for tok in _NUMBER_RE.findall(text or ""):
        try:
            out.add(Decimal(tok.replace(",", ".")).normalize())
        except InvalidOperation:
            continue
    return out


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value)).normalize()
    except InvalidOperation:
        return None


def value_printed(value: Any, numbers: set[Decimal]) -> bool:
    """A stored $ amount is printed as dollars or as cents."""
    d = _dec(value)
    if d is None:
        return False
    return d in numbers or (d * 100).normalize() in numbers


def _ctype(comp: Any) -> str:
    ct = comp.get("component_type") if isinstance(comp, dict) else getattr(comp, "component_type", "")
    return str(getattr(ct, "value", ct) or "")


def _attr(comp: Any, key: str) -> Any:
    return comp.get(key) if isinstance(comp, dict) else getattr(comp, key, None)


def missing_values(components: Iterable[Any], text: str) -> list[str]:
    """Component values (rate, tier bounds) NOT printed in ``text``; [] = all verified."""
    numbers = document_numbers(text)
    missing: list[str] = []
    comps = list(components or [])
    if not comps:
        return ["no components"]
    for c in comps:
        rv = _attr(c, "rate_value")
        if not value_printed(rv, numbers):
            missing.append(f"{_ctype(c)} rate {rv} {_attr(c, 'unit') or ''}".strip())
        for key in ("tier_min_kwh", "tier_max_kwh"):
            bound = _dec(_attr(c, key))
            if bound is not None and bound != 0 and bound not in numbers:
                missing.append(f"{_ctype(c)} {key} {_attr(c, key)}")
    return missing


def pick_official_document(components: Iterable[Any], documents: dict[str, str]) -> tuple[str | None, dict[str, list[str]]]:
    """First document (in the given order) that prints every value, plus misses per document."""
    comps = list(components or [])
    misses: dict[str, list[str]] = {}
    for url, text in documents.items():
        if not text:
            misses[url] = ["document unavailable"]
            continue
        miss = missing_values(comps, text)
        if not miss:
            return url, misses
        misses[url] = miss
    return None, misses


# ---------------------------------------------------------------------------
# DB work
# ---------------------------------------------------------------------------

@dataclass
class RepairReport:
    candidates: list[int] = field(default_factory=list)
    carried: list[tuple[int, int | None, str]] = field(default_factory=list)
    reextracted: list[tuple[int, int | None, str]] = field(default_factory=list)
    skipped_protected: list[int] = field(default_factory=list)
    unresolved: dict[int, dict[str, list[str]]] = field(default_factory=dict)


def third_party_residential(session: Session, utility_id: int, source_host: str) -> list[Tariff]:
    rows = session.execute(
        select(Tariff)
        .options(selectinload(Tariff.rate_components))
        .where(
            Tariff.utility_id == utility_id,
            Tariff.customer_class == CustomerClass.RESIDENTIAL,
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
        .order_by(Tariff.id)
    ).scalars().all()
    host = source_host.lower()
    return [
        t for t in rows
        if (h := normalize_host(t.source_url)) and (h == host or h.endswith(f".{host}"))
    ]


def _copy_tariff(old: Tariff, components: Iterable[Any], *, source_url: str, doc_hash: str | None,
                 evidence: dict, overrides: dict | None = None) -> Tariff:
    new = Tariff(**{**{f: getattr(old, f) for f in _TARIFF_COPY_FIELDS}, **(overrides or {})})
    new.utility_id = old.utility_id
    new.source_url = source_url
    new.source_document_hash = doc_hash
    new.last_verified_at = datetime.now(timezone.utc)
    new.approved = False
    new.confidence_score = old.confidence_score
    new.confidence_factors = {**(old.confidence_factors or {}), "source_repair": evidence}
    for c in components:
        new.rate_components.append(RateComponent(**{f: getattr(c, f) for f in _COMPONENT_COPY_FIELDS}))
    return new


def _replace(session: Session, old: Tariff, new: Tariff, *, evidence: dict) -> int:
    session.add(new)
    session.flush()
    supersede_tariff(
        session, old,
        successor=new,
        reason=SUPERSEDE_REASON,
        actor_type="script",
        actor_id=SCRIPT,
        source_url=new.source_url,
        source_document_hash=new.source_document_hash,
        payload={"evidence": evidence, "prior_source_url": old.source_url},
    )
    return new.id


def repair(
    session: Session,
    utility: Utility,
    documents: dict[str, str],
    *,
    apply: bool,
    source_host: str = DEFAULT_SOURCE_HOST,
    reextracted: dict[str, Any] | None = None,
) -> RepairReport:
    """Carry verified rows (and optionally re-extracted ones) onto official docs.

    ``documents``: official url → text, in preference order.
    ``reextracted``: official-doc ExtractedTariffs by name (``--reextract``).
    """
    from scripts.tariff_pipeline import (
        _STORE_TYPE_MAP_RAW,
        _build_rate_components,
        _computable_regression,
        tariffs_likely_same,
    )

    report = RepairReport()
    hashes = {url: stable_text_hash(text) for url, text in documents.items() if text}
    rows = third_party_residential(session, utility.id, source_host)
    report.candidates = [t.id for t in rows]
    print(f"  [{utility.name} id={utility.id}] {len(rows)} live residential row(s) on {source_host}")

    for t in rows:
        comps = list(t.rate_components or [])
        print(f"\n  id={t.id} '{t.name}' code={t.code!r} rate_type={t.rate_type.value} "
              f"eff={t.effective_date} source={t.source_url}")
        if is_protected(t):
            print("    SKIP: protected row (approved / repair / manual)")
            report.skipped_protected.append(t.id)
            continue

        url, misses = pick_official_document(comps, documents)
        if url:
            evidence = {
                "script": SCRIPT, "mode": "verify_carry", "document": url,
                "document_hash": hashes.get(url), "verified_values": len(comps),
                "from_source_url": t.source_url,
                "at": datetime.now(timezone.utc).isoformat(),
            }
            print(f"    VERIFIED: all {len(comps)} component value(s) printed in {url}")
            new_id = None
            if apply:
                new_id = _replace(session, t, _copy_tariff(
                    t, comps, source_url=url, doc_hash=hashes.get(url), evidence=evidence,
                ), evidence=evidence)
                print(f"    CARRIED → new id={new_id}; id={t.id} soft-superseded ({SUPERSEDE_REASON})")
            else:
                print("    would carry components forward onto the official source")
            report.carried.append((t.id, new_id, url))
            continue

        for doc, miss in misses.items():
            print(f"    not verified in {doc}: {', '.join(miss[:6])}{' …' if len(miss) > 6 else ''}")

        match = None
        if reextracted:
            match = next(
                (et for name, et in reextracted.items() if name == t.name or tariffs_likely_same(t.name, name)),
                None,
            )
        if match is None:
            print("    UNRESOLVED: left live" + ("" if reextracted is not None else " (try --reextract)"))
            report.unresolved[t.id] = misses
            continue

        new_components = _build_rate_components(match)
        overrides: dict[str, Any] = {}
        if _STORE_TYPE_MAP_RAW.get(match.rate_type):
            overrides["rate_type"] = RateType(_STORE_TYPE_MAP_RAW[match.rate_type])
        try:
            if match.effective_date:
                overrides["effective_date"] = date.fromisoformat(match.effective_date)
        except ValueError:
            pass
        regression = _computable_regression(t, overrides.get("rate_type", t.rate_type), new_components)
        if not new_components or regression:
            print(f"    UNRESOLVED: re-extracted '{match.name}' unusable "
                  f"({'no components' if not new_components else ', '.join(regression[:4])})")
            report.unresolved[t.id] = misses
            continue
        doc = match.source_url or HQ_RATES_PDF_URL
        evidence = {
            "script": SCRIPT, "mode": "reextract", "document": doc,
            "document_hash": hashes.get(doc), "extracted_name": match.name,
            "extraction_tier": getattr(match, "extraction_tier", None),
            "from_source_url": t.source_url, "prior_components": serialize_components(comps),
            "at": datetime.now(timezone.utc).isoformat(),
        }
        print(f"    RE-EXTRACTED from {doc} as '{match.name}':")
        for c in serialize_components(new_components):
            print(f"      {c['component_type']:<10} {c['rate_value']:>10} {c['unit'] or ''} "
                  f"{c.get('period_label') or ''} {c.get('season') or ''} {c.get('tier_label') or ''}".rstrip())
        new_id = None
        if apply:
            new_id = _replace(session, t, _copy_tariff(
                t, new_components, source_url=doc, doc_hash=hashes.get(doc), evidence=evidence,
                overrides=overrides,
            ), evidence=evidence)
            print(f"    REPLACED → new id={new_id}; id={t.id} soft-superseded ({SUPERSEDE_REASON})")
        report.reextracted.append((t.id, new_id, doc))

    if apply and (report.carried or report.reextracted):
        from app.services.pins import ensure_monitoring_source

        ensure_monitoring_source(session, utility.id, HQ_RATES_PDF_URL)
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _load_documents(pdf_file: str | None) -> dict[str, str]:
    from scripts import tariff_pipeline as tp

    if pdf_file:
        with open(pdf_file, "rb") as fh:
            pdf_text = tp._extract_pdf_pdfplumber(fh.read())
    else:
        pdf_text = tp.fetch_pdf_text(HQ_RATES_PDF_URL)
    page = tp._fetch_and_parse(HQ_RATES_PAGE_URL)
    docs = {HQ_RATES_PDF_URL: pdf_text or "", HQ_RATES_PAGE_URL: (page.content if page else "")}
    for url, text in docs.items():
        print(f"  official document {url}: {len(text):,} chars" + ("" if text else " (UNAVAILABLE)"))
    return docs


def _reextract(utility: Utility, pdf_text: str) -> dict[str, Any]:
    import hashlib

    from scripts import tariff_pipeline as tp

    page = tp.RatePage(
        url=HQ_RATES_PDF_URL, title="electricity-rates.pdf", page_type="pdf",
        content=pdf_text, content_hash=hashlib.sha256(pdf_text.encode()).hexdigest(),
    )
    extracted = tp.phase3_extract_tariffs([page], utility.name, state=utility.state_province)
    _, valid = tp.phase4_validate(extracted, utility.name, utility.state_province)
    out = {et.name: et for et in valid if et.customer_class == "residential"}
    print(f"  re-extracted {len(out)} residential tariff(s) from the official PDF: {sorted(out)}")
    return out


def main(argv: list[str] | None = None) -> RepairReport:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--utility-id", type=int, default=HQ_UTILITY_ID)
    parser.add_argument("--source-host", default=DEFAULT_SOURCE_HOST,
                        help=f"Third-party host to move off (default {DEFAULT_SOURCE_HOST})")
    parser.add_argument("--pdf-file", help="Local copy of the official rate book PDF")
    parser.add_argument("--reextract", action="store_true",
                        help="Re-extract unverified rows from the official PDF (LLM spend)")
    parser.add_argument("--apply", action="store_true", help="Write (default: dry run)")
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    print(f"{'APPLY' if args.apply else 'DRY RUN'}: {SCRIPT}")
    with Session(get_sync_engine()) as session:
        utility = session.get(Utility, args.utility_id)
        if utility is None:
            raise SystemExit(f"utility {args.utility_id} not found")
        if (utility.state_province or "").upper() != "QC":
            raise SystemExit(f"utility {utility.id} '{utility.name}' is not in QC — refusing")
        if not third_party_residential(session, utility.id, args.source_host):
            print(f"  nothing to do: no live residential row cites {args.source_host}")
            return RepairReport()

        documents = _load_documents(args.pdf_file)
        if not any(documents.values()):
            raise SystemExit("could not read any official HQ document — nothing verified, nothing written")
        reextracted = None
        if args.reextract:
            if not documents[HQ_RATES_PDF_URL]:
                raise SystemExit("--reextract needs the official PDF text")
            reextracted = _reextract(utility, documents[HQ_RATES_PDF_URL])

        report = repair(
            session, utility, documents,
            apply=args.apply, source_host=args.source_host, reextracted=reextracted,
        )
        if args.apply:
            session.commit()
        else:
            session.rollback()

    print(
        f"\n  summary: {len(report.candidates)} candidate(s), {len(report.carried)} carried, "
        f"{len(report.reextracted)} re-extracted, {len(report.skipped_protected)} protected, "
        f"{len(report.unresolved)} unresolved (left live)"
    )
    if not args.apply:
        print("  dry run — nothing written; re-run with --apply")
    return report


if __name__ == "__main__":
    main()
