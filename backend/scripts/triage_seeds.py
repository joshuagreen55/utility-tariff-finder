"""Triage stale OpenEI seed tariffs into actionable buckets.

The DB carries thousands of 2017 OpenEI "seed" tariffs that were never
re-verified (`openei_id IS NOT NULL AND last_verified_at IS NULL AND
superseded_by_tariff_id IS NULL`). Before spending LLM tokens trying to
re-extract or reconcile them, we need to know *why* each one is stale.
This script labels every stale seed into one of four buckets using
fast, transparent rules (no LLM):

  out_of_scope          The seed is not a consumer retail tariff we model:
                        wholesale / resale / transmission-service rates, or
                        the utility is a federal Power Marketing Admin
                        (BPA/SEPA/WAPA/SWPA) that only sells wholesale.
                        -> retire (supersede_reason='out_of_scope').

  dup_exact             The utility HAS fresh data and this seed's
                        normalized name exactly matches a fresh tariff of
                        the same customer class. A safe, deterministic
                        1:1 absorption the conservative matcher missed.
                        -> supersede onto the fresh row.

  has_fresh_review      The utility HAS fresh data but no exact normalized
                        match. These are OpenEI zone/region/voltage
                        explosions or discontinued variants -> Track B
                        (LLM 1:N) territory (scripts.supersede_via_llm).

  no_fresh_extract      The utility has ZERO fresh tariffs. A genuine
                        extraction gap -> needs a successful pipeline run
                        or source discovery.

Usage:
  python -m scripts.triage_seeds                 # dry-run, print bucket sizes
  python -m scripts.triage_seeds --samples 8     # also print sample names/bucket
  python -m scripts.triage_seeds --apply-oos     # retire out_of_scope seeds
  python -m scripts.triage_seeds --apply-dups    # supersede dup_exact seeds
"""
import argparse
import os
import re
import sys
from collections import Counter, defaultdict

from sqlalchemy import create_engine, text, update as sa_update
from sqlalchemy.orm import Session

from app.models.tariff import Tariff

# --- out-of-scope name signals (high precision; consumer retail stays) ---
# Deliberately excluded as too ambiguous (they match legitimate retail
# rates): 'transmission service' (large retail customers take service at
# transmission voltage) and 'buy-back' (net-metering export rates are
# arguably in scope for solar customers).
OOS_NAME_PATTERNS = [
    r"\bresale\b",
    r"\bwholesale\b",
    r"\bfor\s+resale\b",
    r"\binterdepartmental\b",
    r"telecommunications\s+network",
    r"\bbulk\s+power\b",
    r"\bfull\s+requirements\b",
    r"\bpartial\s+requirements\b",
]
OOS_RE = re.compile("|".join(OOS_NAME_PATTERNS), re.IGNORECASE)

# Utility types whose published "tariffs" are wholesale-only by nature.
OOS_UTILITY_TYPES = {"FEDERAL"}
OOS_NAME_HINTS_UTIL = re.compile(
    r"power\s+(administration|marketing|authority\s+wholesale)", re.IGNORECASE
)


def normalize_name(name: str) -> str:
    """Collapse OpenEI zone/region/tier/season permutations and boilerplate
    so that seed names can be compared to fresh marketing names.

    Product-discriminating words ('optional', 'standard', 'tou', 'demand',
    ...) are deliberately KEPT: stripping them made 'Optional Residential
    Service' normalize identically to 'Residential Service' — two different
    products — and dup_exact then retired the wrong seed.
    """
    s = name.lower()
    s = re.sub(r"\[[^\]]*\]", " ", s)                       # [NYC Zone J]
    s = re.sub(r"\b(region|zone|district|area|territory|baseline)\s*[-#:]?\s*\w+", " ", s)
    s = re.sub(r"\b(tier|step|block)\s*\d+\b", " ", s)
    s = re.sub(r"\b(summer|winter|spring|fall|autumn)\b", " ", s)
    s = re.sub(r"\b(schedule|rate|service|services)\b", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def classify(seed_name: str, util_type: str, util_name: str,
             has_fresh: bool, fresh_norm_by_class: dict) -> tuple[str, int | None]:
    """Return (bucket, matched_fresh_id_or_None)."""
    # out_of_scope by utility type / federal PMA
    if util_type in OOS_UTILITY_TYPES or OOS_NAME_HINTS_UTIL.search(util_name or ""):
        return "out_of_scope", None
    if OOS_RE.search(seed_name):
        return "out_of_scope", None

    if not has_fresh:
        return "no_fresh_extract", None

    return "has_fresh_review", None  # dup_exact resolved by caller w/ class map


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", type=int, default=0,
                    help="print N sample seed names per bucket")
    ap.add_argument("--apply-oos", action="store_true",
                    help="write supersede_reason='out_of_scope' for that bucket")
    ap.add_argument("--apply-dups", action="store_true",
                    help="supersede dup_exact seeds onto their fresh match")
    ap.add_argument("--utility", type=int, help="restrict to one utility_id")
    args = ap.parse_args()

    url = os.environ.get("SYNC_DATABASE_URL") or os.environ["DATABASE_URL"].replace("+asyncpg", "")
    engine = create_engine(url)

    print(f"\nSEED TRIAGE  {'(APPLY)' if (args.apply_oos or args.apply_dups) else '(dry-run)'}")
    print("=" * 78)

    util_filter = "AND u.id = :uid" if args.utility else ""
    params = {"uid": args.utility} if args.utility else {}

    with Session(engine) as session:
        # All active utilities that have at least one stale seed.
        utils = session.execute(text(f"""
            SELECT u.id, u.name, u.utility_type::text AS utype,
                   COUNT(*) FILTER (WHERE t.last_verified_at IS NOT NULL
                       AND t.superseded_by_tariff_id IS NULL
                       AND t.supersede_reason IS NULL) AS fresh,
                   COUNT(*) FILTER (WHERE t.last_verified_at IS NULL
                       AND t.openei_id IS NOT NULL
                       AND t.superseded_by_tariff_id IS NULL
                       AND t.supersede_reason IS NULL) AS stale
            FROM utilities u JOIN tariffs t ON t.utility_id = u.id
            WHERE u.is_active {util_filter}
            GROUP BY u.id, u.name, u.utility_type
            HAVING COUNT(*) FILTER (WHERE t.last_verified_at IS NULL
                       AND t.openei_id IS NOT NULL
                       AND t.superseded_by_tariff_id IS NULL
                       AND t.supersede_reason IS NULL) > 0
        """), params).all()

        bucket_counts = Counter()
        bucket_by_type = defaultdict(Counter)
        samples = defaultdict(list)
        oos_ids, dup_pairs = [], []

        for u in utils:
            has_fresh = u.fresh > 0
            # fresh normalized names by class for dup detection
            fresh_norm = defaultdict(dict)  # class -> {norm_name: id}
            collided: set[tuple[str, str]] = set()
            if has_fresh:
                for fr in session.execute(text("""
                    SELECT id, name, customer_class::text AS cc FROM tariffs
                    WHERE utility_id = :uid AND last_verified_at IS NOT NULL
                      AND superseded_by_tariff_id IS NULL AND supersede_reason IS NULL
                """), {"uid": u.id}):
                    key = normalize_name(fr.name)
                    if key in fresh_norm[fr.cc] and fresh_norm[fr.cc][key] != fr.id:
                        # Two distinct fresh tariffs collapse to the same
                        # normalized name -> dup_exact would be ambiguous.
                        collided.add((fr.cc, key))
                    else:
                        fresh_norm[fr.cc].setdefault(key, fr.id)

            seeds = session.execute(text("""
                SELECT id, name, customer_class::text AS cc FROM tariffs
                WHERE utility_id = :uid AND last_verified_at IS NULL
                  AND openei_id IS NOT NULL AND superseded_by_tariff_id IS NULL
                  AND supersede_reason IS NULL
            """), {"uid": u.id}).all()

            for s in seeds:
                bucket, _ = classify(s.name, u.utype, u.name, has_fresh, fresh_norm)
                if bucket == "has_fresh_review":
                    seed_key = normalize_name(s.name)
                    match = fresh_norm.get(s.cc, {}).get(seed_key)
                    if match and (s.cc, seed_key) not in collided:
                        bucket = "dup_exact"
                        dup_pairs.append((s.id, match))
                if bucket == "out_of_scope":
                    oos_ids.append(s.id)
                bucket_counts[bucket] += 1
                bucket_by_type[u.utype][bucket] += 1
                if args.samples and len(samples[bucket]) < args.samples:
                    samples[bucket].append(f"[{u.utype[:4]}] {u.name[:24]} :: {s.name[:46]}")

        total = sum(bucket_counts.values())
        print(f"Utilities with stale seeds: {len(utils)}    Total stale seeds: {total}\n")
        order = ["out_of_scope", "dup_exact", "has_fresh_review", "no_fresh_extract"]
        for b in order:
            n = bucket_counts.get(b, 0)
            pct = 100 * n / total if total else 0
            print(f"  {b:<20} {n:>6}  ({pct:4.1f}%)")
        print()

        print("By utility type:")
        print(f"  {'type':<22}{'oos':>7}{'dup':>7}{'review':>8}{'extract':>9}")
        for ut, c in sorted(bucket_by_type.items(), key=lambda kv: -sum(kv[1].values())):
            print(f"  {ut:<22}{c.get('out_of_scope',0):>7}{c.get('dup_exact',0):>7}"
                  f"{c.get('has_fresh_review',0):>8}{c.get('no_fresh_extract',0):>9}")
        print()

        if args.samples:
            for b in order:
                if samples[b]:
                    print(f"--- {b} samples ---")
                    for line in samples[b]:
                        print(f"    {line}")
                    print()

        if args.apply_oos and oos_ids:
            session.execute(
                sa_update(Tariff).where(Tariff.id.in_(oos_ids))
                .values(supersede_reason="out_of_scope")
            )
            session.commit()
            print(f"APPLIED out_of_scope retirement to {len(oos_ids)} seeds.")

        if args.apply_dups and dup_pairs:
            for sid, fid in dup_pairs:
                session.execute(
                    sa_update(Tariff).where(Tariff.id == sid)
                    .values(superseded_by_tariff_id=fid, supersede_reason="dup_normalized")
                )
            session.commit()
            print(f"APPLIED dup_normalized supersede to {len(dup_pairs)} seeds.")


if __name__ == "__main__":
    sys.exit(main())
