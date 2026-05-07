"""One-shot data quality sweep for known systematic problems.

Manual quality checks (Hydro-Quebec, etc.) surfaced three classes of bad
data that aren't caught by the existing aggregator blocklist or the
attribution validator:

    A. PHANTOM tariffs extracted from utility *comparison/marketing PDFs*
       — e.g. `comparison-electricity-prices-2019.pdf`. These docs print a
       single "average residential rate" number per region; the LLM
       (correctly, given the prompt) parses that number as a FLAT tariff.
       The result is a fictional rate that doesn't appear in any real
       tariff schedule.

    B. NON-ELECTRIC tariffs in the electric tariff database. A handful of
       gas-distribution tariffs sneak in when a utility's combined
       gas+electric portal is crawled.

    C. STALE OpenEI URDB imports superseded by fresh extractions. The
       initial seed loaded ~10k tariffs from OpenEI's URDB (most from
       2017–2019). The new pipeline has been running for two months and
       has freshly re-extracted thousands of these. When a fresh
       extraction with a similar name exists for the same
       (utility, customer_class), the OpenEI row is a stale duplicate —
       the user sees both, gets confused which is current.

Each category is purged independently and is reversible only by a DB
restore — so dry-run is the default. Use `--apply` to actually delete,
and `--only A|B|C` to scope a single category.

Usage:

    python -m scripts.quality_cleanup                 # dry-run all
    python -m scripts.quality_cleanup --only A        # dry-run phantoms
    python -m scripts.quality_cleanup --apply         # delete all
    python -m scripts.quality_cleanup --apply --only C  # only stale dedup
"""
import argparse
import re
import sys
from collections import defaultdict
from typing import Iterable

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Tariff, Utility
from scripts.tariff_pipeline import tariffs_likely_same


# ----------------------------------------------------------------------
# Issue A — comparison/marketing PDF phantoms
# ----------------------------------------------------------------------

# Substrings in a source_url that mark the doc as a marketing/comparison
# brochure rather than an authoritative tariff schedule. These docs
# print *representative* prices, not real tariffs.
COMPARISON_URL_PATTERNS = [
    "comparison-electricity-prices",
    "comparison-of-electricity",
    "comparing-electricity",
    "rate-comparison",
    "rates-comparison",
    "average-electricity-rates",
    "electricity-prices-2019",
    "electricity-prices-2020",
    "electricity-prices-2021",
    "electricity-prices-2022",
    "electricity-prices-2023",
    "electricity-prices-2024",
    "electricity-prices-2025",
    "electricity-prices-2026",
]


def find_comparison_phantoms(s: Session) -> list[tuple[int, str, str, str]]:
    """Return (id, utility_name, tariff_name, source_url) for phantom rows."""
    pattern = "|".join(re.escape(p) for p in COMPARISON_URL_PATTERNS)
    rows = s.execute(
        text(
            """
            SELECT t.id, u.name, t.name, t.source_url
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            WHERE t.source_url ~* :pat
            """
        ),
        {"pat": pattern},
    ).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


# ----------------------------------------------------------------------
# Issue B — gas-only tariffs in an electric DB
# ----------------------------------------------------------------------

# Match clearly gas-only tariff names. We're conservative: must mention
# "gas" in a non-incidental way AND not also reference electric service.
GAS_NAME_REGEX = re.compile(
    r"\b(natural\s+gas|gas\s+distribution|gas\s+delivery|gas\s+supply|gas\s+service|gas\s+transmission|gds[\s\-]\d)\b",
    re.IGNORECASE,
)
ELECTRIC_HINT_REGEX = re.compile(r"\belectric|\bgas\s+turbine\b", re.IGNORECASE)


def find_gas_tariffs(s: Session) -> list[tuple[int, str, str]]:
    rows = s.execute(
        text(
            """
            SELECT t.id, u.name, t.name
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            WHERE u.is_active = true
            """
        )
    ).fetchall()
    out: list[tuple[int, str, str]] = []
    for tid, uname, tname in rows:
        if not tname:
            continue
        if GAS_NAME_REGEX.search(tname) and not ELECTRIC_HINT_REGEX.search(tname):
            out.append((tid, uname, tname))
    return out


# ----------------------------------------------------------------------
# Issue C — stale OpenEI imports superseded by fresh extractions
# ----------------------------------------------------------------------

# Name-matching delegated to scripts.tariff_pipeline.tariffs_likely_same
# so the live extractor (Phase 4 supersede) and this offline cleanup
# share one matcher.


# Tokens that distinguish otherwise-similar tariffs. If both names contain
# a discriminator and the sets differ, they're not the same product.
def _name_matches(openei_name: str, fresh_name: str) -> bool:
    """Thin wrapper kept for naming clarity in this script."""
    return tariffs_likely_same(openei_name, fresh_name)


def find_stale_openei_dups(s: Session) -> list[tuple[int, str, str, str]]:
    """Return (id, utility_name, openei_tariff_name, matched_fresh_name) for
    OpenEI imports that have a fresh sibling matching by name."""
    rows = s.execute(
        text(
            """
            SELECT t.id, t.utility_id, u.name AS utility_name,
                   t.name, t.customer_class, (t.openei_id IS NOT NULL) AS is_openei,
                   t.last_verified_at
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            ORDER BY t.utility_id, t.customer_class
            """
        )
    ).fetchall()

    by_group: dict[tuple[int, str], list] = defaultdict(list)
    for r in rows:
        by_group[(r[1], r[4])].append(r)

    candidates: list[tuple[int, str, str, str]] = []
    for (_uid, _cclass), members in by_group.items():
        openei = [r for r in members if r[5] and r[6] is None]
        fresh = [r for r in members if not r[5] or r[6] is not None]
        if not openei or not fresh:
            continue
        for o in openei:
            for f in fresh:
                if _name_matches(o[3], f[3]):
                    candidates.append((o[0], o[2], o[3], f[3]))
                    break
    return candidates


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------

def _delete_ids(s: Session, ids: Iterable[int], label: str) -> int:
    ids = list(ids)
    if not ids:
        return 0
    BATCH = 500
    deleted = 0
    for i in range(0, len(ids), BATCH):
        chunk = ids[i : i + BATCH]
        res = s.execute(
            text("DELETE FROM tariffs WHERE id = ANY(:ids)"),
            {"ids": chunk},
        )
        deleted += res.rowcount or 0
    s.commit()
    print(f"  deleted {deleted:,d} {label}")
    return deleted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually delete (default is dry-run)")
    parser.add_argument("--only", choices=["A", "B", "C"], help="Limit to one category")
    args = parser.parse_args()

    eng = get_sync_engine()
    with Session(eng) as s:
        before_total = s.execute(text("SELECT COUNT(*) FROM tariffs")).scalar()

        cats: dict[str, list] = {}

        if args.only in (None, "A"):
            phantoms = find_comparison_phantoms(s)
            cats["A"] = phantoms
            print()
            print("=" * 78)
            print(f"A. PHANTOM tariffs from comparison/marketing PDFs: {len(phantoms)}")
            print("=" * 78)
            by_util: dict[str, int] = defaultdict(int)
            for tid, uname, tname, src in phantoms:
                by_util[uname] += 1
            for uname, n in sorted(by_util.items(), key=lambda kv: -kv[1])[:15]:
                print(f"  {uname[:55]:<55s}  {n:>4d}")
            if phantoms[:5]:
                print("\n  Sample rows:")
                for tid, uname, tname, src in phantoms[:5]:
                    print(f"    id={tid:>5d}  {uname[:30]:<30s}  {tname[:50]}")

        if args.only in (None, "B"):
            gas = find_gas_tariffs(s)
            cats["B"] = gas
            print()
            print("=" * 78)
            print(f"B. GAS-only tariffs in electric DB: {len(gas)}")
            print("=" * 78)
            for tid, uname, tname in gas[:30]:
                print(f"  id={tid:>5d}  {uname[:30]:<30s}  {tname[:50]}")
            if len(gas) > 30:
                print(f"  ... and {len(gas) - 30} more")

        if args.only in (None, "C"):
            stale = find_stale_openei_dups(s)
            cats["C"] = stale
            print()
            print("=" * 78)
            print(f"C. STALE OpenEI imports superseded by fresh extractions: {len(stale)}")
            print("=" * 78)
            by_util: dict[str, int] = defaultdict(int)
            for tid, uname, on, fn in stale:
                by_util[uname] += 1
            top = sorted(by_util.items(), key=lambda kv: -kv[1])[:15]
            for uname, n in top:
                print(f"  {uname[:55]:<55s}  {n:>4d}")
            if stale[:8]:
                print("\n  Sample matches (OpenEI -> fresh):")
                for tid, uname, on, fn in stale[:8]:
                    print(f"    id={tid:<5d}  {uname[:25]:<25s}")
                    print(f"           OpenEI: {on[:65]}")
                    print(f"            Fresh: {fn[:65]}")

        total_to_delete = sum(len(v) for v in cats.values())
        print()
        print("=" * 78)
        print(f"SUMMARY  ({'APPLY MODE' if args.apply else 'DRY-RUN'})")
        print("=" * 78)
        print(f"  Tariffs in DB now:              {before_total:>6,d}")
        for k, v in cats.items():
            print(f"  Cat {k} candidates:              {len(v):>6,d}")
        print(f"  Total to delete:                {total_to_delete:>6,d}")
        print(f"  Tariffs after cleanup (est.):   {before_total - total_to_delete:>6,d}")

        if not args.apply:
            print()
            print("DRY-RUN: no changes made. Re-run with --apply to delete.")
            return 0

        # Apply
        print()
        print("APPLYING...")
        deleted_total = 0
        if "A" in cats:
            deleted_total += _delete_ids(s, [r[0] for r in cats["A"]], "phantom (cat A)")
        if "B" in cats:
            deleted_total += _delete_ids(s, [r[0] for r in cats["B"]], "gas (cat B)")
        if "C" in cats:
            deleted_total += _delete_ids(s, [r[0] for r in cats["C"]], "stale OpenEI (cat C)")

        after_total = s.execute(text("SELECT COUNT(*) FROM tariffs")).scalar()
        active_with_tariffs = s.execute(
            text(
                """
                SELECT COUNT(DISTINCT t.utility_id) FROM tariffs t
                JOIN utilities u ON u.id = t.utility_id
                WHERE u.is_active = true
                """
            )
        ).scalar()
        active_total = s.execute(text("SELECT COUNT(*) FROM utilities WHERE is_active = true")).scalar()
        print()
        print("=" * 78)
        print("DONE")
        print("=" * 78)
        print(f"  Deleted total:           {deleted_total:>6,d}")
        print(f"  Tariffs before:          {before_total:>6,d}")
        print(f"  Tariffs after:           {after_total:>6,d}")
        print(f"  Active-utility coverage: {active_with_tariffs} / {active_total} = "
              f"{100*active_with_tariffs/active_total:.2f}%")
        return 0


if __name__ == "__main__":
    sys.exit(main())
