"""Database health score for utility tariff coverage & quality.

Produces a single 0-100 Health Score from two lenses, each of which is
also reported on its own so you can see WHERE the score comes from:

  LENS 1 — COVERAGE (breadth): of the active utilities we are supposed to
           serve, what fraction have what the product actually needs — a
           live, verified RESIDENTIAL tariff with an energy component?
           A looser "any verified tariff with components" count is also
           reported for context. Reported unweighted AND weighted by
           utility type (an IOU serving millions matters more than a
           500-meter coop).

  LENS 2 — QUALITY (depth): for the tariffs we actually serve to users
           (everything not superseded — the API returns these), how fresh,
           complete, and well-sourced are they?

Composite Health Score = weighted blend:
    40%  Coverage (type-weighted)
    30%  Freshness
    20%  Completeness (has energy + fixed charge + effective date)
    10%  Provenance (source quality: official / unknown / third-party)

PROVENANCE BEHAVIOUR CHANGE (2026-09, issue #28 — "provenance_method":
"source_type_v2" in the JSON). Provenance used to be "served tariff has a
non-null source_url", which sat at ~100 while live rows cited rate blogs.
It now scores each served tariff by ``tariffs.source_type``
(app/services/source_type.py):

    official     1.0   utility's own site / documents (or the board that
                       publishes the rate, e.g. OEB for Ontario)
    unknown      0.4   no URL, generic file host, government docket copy,
                       or no official host on file to compare against
    third_party  0.2   aggregator / rate blog / a host that differs from
                       every official host for the utility

Expect a one-time Provenance drop (at most -8 composite points, if every
row were third-party) on the first run after the deploy; it is a change of yardstick,
not a data regression. Snapshots before the change have no
"provenance_method" key. The composite weights are unchanged, and the
old "has_source_url" count is still reported. Unknown is 0.4 rather than
0.0 so a missing/unclassifiable URL is scored above a known blog copy.

Also reported, but NOT in the composite (so the score stays comparable with
its history): the COMPUTABLE lens — live, verified residential tariffs that
satisfy the computable contract (app/services/computable.py), and how many
active utilities have at least one. That is the bar for Mysa cost/TOU use.
And the OFFICIAL-SOURCE lens — how many active utilities' best live, verified
residential tariff cites an official source.

The score is deterministic and re-runnable. Pass --snapshot to append a
JSON line to logs/health_score_history.jsonl so you can chart the trend
after each refresh/cleanup run.

Usage:
  python /app/scripts/health_score.py
  python /app/scripts/health_score.py --snapshot
  python /app/scripts/health_score.py --json      # machine-readable only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine

# --- Tunable scoring weights -------------------------------------------------

COMPOSITE_WEIGHTS = {
    "coverage": 0.40,
    "freshness": 0.30,
    "completeness": 0.20,
    "provenance": 0.10,
}

# Provenance credit per served tariff by tariffs.source_type.
PROVENANCE_WEIGHTS = {
    "official": 1.0,
    "unknown": 0.4,
    "third_party": 0.2,
}
PROVENANCE_METHOD = "source_type_v2"


def provenance_score(counts: dict[str, int]) -> float:
    """0-100 Provenance from source_type counts; unrecognized types score as unknown."""
    total = sum(counts.values())
    if not total:
        return 0.0
    points = sum(
        PROVENANCE_WEIGHTS.get(st, PROVENANCE_WEIGHTS["unknown"]) * n
        for st, n in counts.items()
    )
    return 100.0 * points / total

# Relative importance of a utility by type (proxy for customers served, since
# we don't store customer counts). Used for the type-weighted coverage score.
# Keyed on the enum NAMES as stored in Postgres (uppercase), with the lowercase
# value forms also accepted so the lookup is robust to either storage style.
TYPE_WEIGHTS = {
    "IOU": 10,
    "INVESTOR_OWNED": 10,
    "COMMUNITY_CHOICE": 6,
    "STATE": 4,
    "FEDERAL": 4,
    "POLITICAL_SUBDIVISION": 4,
    "MUNICIPAL": 3,
    "COOPERATIVE": 2,
    "RETAIL_MARKETER": 1,
    "BEHIND_METER": 1,
    "OTHER": 1,
}


def type_weight(utype: str) -> int:
    """Look up a utility-type weight, tolerant of name/value & case."""
    if not utype:
        return 1
    key = utype.upper()
    if key in TYPE_WEIGHTS:
        return TYPE_WEIGHTS[key]
    # accept lowercase value form e.g. "investor_owned"
    return TYPE_WEIGHTS.get(key.replace("INVESTOR_OWNED", "IOU"), 1)

# Freshness decay: a verified tariff is worth less the older it is.
# (days_within, weight). Anything older than the last threshold but still
# verified gets RESIDUAL_VERIFIED_WEIGHT; never-verified gets 0.
FRESHNESS_TIERS = [(90, 1.0), (180, 0.8), (365, 0.5), (730, 0.25)]
RESIDUAL_VERIFIED_WEIGHT = 0.1


def letter_grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"


def bar(pct: float, width: int = 40) -> str:
    filled = int(round(pct / 100 * width))
    return "█" * filled + "·" * (width - filled)


# --- SQL ---------------------------------------------------------------------

COVERAGE_SQL = text("""
WITH good_tariffs AS (
    SELECT t.utility_id,
           MAX(CASE WHEN lower(t.customer_class::text) = 'residential'
                     AND EXISTS (SELECT 1 FROM rate_components rc
                                 WHERE rc.tariff_id = t.id
                                   AND lower(rc.component_type::text) = 'energy')
                THEN 1 ELSE 0 END) AS has_res_energy
    FROM tariffs t
    WHERE t.last_verified_at IS NOT NULL
      AND t.superseded_by_tariff_id IS NULL
      AND t.supersede_reason IS NULL
      AND EXISTS (SELECT 1 FROM rate_components rc WHERE rc.tariff_id = t.id)
    GROUP BY t.utility_id
)
SELECT u.utility_type::text AS utype,
       COUNT(*) AS total_utils,
       COUNT(*) FILTER (WHERE gt.has_res_energy = 1) AS covered_utils,
       COUNT(*) FILTER (WHERE gt.utility_id IS NOT NULL) AS covered_any
FROM utilities u
LEFT JOIN good_tariffs gt ON gt.utility_id = u.id
WHERE u.is_active
GROUP BY u.utility_type
""")

QUALITY_SQL = text("""
WITH pt AS (
    SELECT t.id,
           t.last_verified_at,
           t.source_url,
           t.source_type,
           t.effective_date,
           t.openei_id,
           bool_or(lower(rc.component_type::text) = 'energy')             AS has_energy,
           bool_or(lower(rc.component_type::text) IN ('fixed','minimum')) AS has_fixed
    FROM tariffs t
    LEFT JOIN rate_components rc ON rc.tariff_id = t.id
    WHERE t.superseded_by_tariff_id IS NULL
      AND t.supersede_reason IS NULL
    GROUP BY t.id
)
SELECT
    COUNT(*)                                                         AS served,
    COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL)             AS verified,
    COUNT(*) FILTER (WHERE last_verified_at IS NULL
                       AND openei_id IS NOT NULL)                    AS stale_seed,
    -- freshness points
    SUM(CASE
          WHEN last_verified_at >= now() - interval '90 days'  THEN 1.0
          WHEN last_verified_at >= now() - interval '180 days' THEN 0.8
          WHEN last_verified_at >= now() - interval '365 days' THEN 0.5
          WHEN last_verified_at >= now() - interval '730 days' THEN 0.25
          WHEN last_verified_at IS NOT NULL                    THEN 0.1
          ELSE 0.0
        END)                                                        AS freshness_points,
    -- completeness points (only meaningful for verified rows)
    COUNT(*) FILTER (WHERE has_energy)                              AS has_energy_n,
    COUNT(*) FILTER (WHERE has_fixed)                               AS has_fixed_n,
    COUNT(*) FILTER (WHERE effective_date IS NOT NULL)             AS has_eff_n,
    COUNT(*) FILTER (WHERE source_url IS NOT NULL)                 AS has_source_n,
    -- provenance classes (tariffs.source_type)
    COUNT(*) FILTER (WHERE source_type = 'official')               AS src_official_n,
    COUNT(*) FILTER (WHERE source_type = 'third_party')            AS src_third_party_n,
    COUNT(*) FILTER (WHERE source_type NOT IN ('official', 'third_party')) AS src_unknown_n
FROM pt
""")

# Best live, verified residential source per active utility
# (official > unknown > third_party).
OFFICIAL_LENS_SQL = text("""
WITH best AS (
    SELECT t.utility_id,
           MIN(CASE t.source_type WHEN 'official' THEN 0
                                  WHEN 'third_party' THEN 2
                                  ELSE 1 END) AS best_rank
    FROM tariffs t
    JOIN utilities u ON u.id = t.utility_id
    WHERE u.is_active
      AND lower(t.customer_class::text) = 'residential'
      AND t.last_verified_at IS NOT NULL
      AND t.superseded_by_tariff_id IS NULL
      AND t.supersede_reason IS NULL
    GROUP BY t.utility_id
)
SELECT
    COUNT(*)                                  AS utilities_with_residential,
    COUNT(*) FILTER (WHERE best_rank = 0)     AS best_official,
    COUNT(*) FILTER (WHERE best_rank = 1)     AS best_unknown,
    COUNT(*) FILTER (WHERE best_rank = 2)     AS best_third_party
FROM best
""")

FRESHNESS_BUCKETS_SQL = text("""
SELECT
  COUNT(*) FILTER (WHERE last_verified_at >= now() - interval '90 days')                                            AS d90,
  COUNT(*) FILTER (WHERE last_verified_at >= now() - interval '180 days' AND last_verified_at < now() - interval '90 days')  AS d180,
  COUNT(*) FILTER (WHERE last_verified_at >= now() - interval '365 days' AND last_verified_at < now() - interval '180 days') AS d365,
  COUNT(*) FILTER (WHERE last_verified_at < now() - interval '365 days')                                            AS older,
  COUNT(*) FILTER (WHERE last_verified_at IS NULL)                                                                  AS never
FROM tariffs
WHERE superseded_by_tariff_id IS NULL
  AND supersede_reason IS NULL
""")

MONITORING_SQL = text("""
SELECT
  COUNT(*)                                                        AS total,
  COUNT(*) FILTER (WHERE lower(status::text) = 'error')           AS errors,
  COUNT(*) FILTER (WHERE lower(status::text) = 'unchanged')       AS ok,
  COUNT(*) FILTER (WHERE last_checked_at IS NULL)                 AS never_checked
FROM monitoring_sources
""")


def compute_computable(session: Session) -> dict:
    """Computable-contract lens over live, verified residential tariffs."""
    from collections import Counter

    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from app.models import CustomerClass, Tariff, Utility
    from app.services.computable import evaluate_computable

    rows = session.execute(
        select(Tariff, Utility.holiday_calendar)
        .join(Utility, Utility.id == Tariff.utility_id)
        .options(selectinload(Tariff.rate_components))
        .where(
            Utility.is_active.is_(True),
            Tariff.customer_class == CustomerClass.RESIDENTIAL,
            Tariff.last_verified_at.is_not(None),
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
    ).all()
    ok = 0
    utils_ok: set[int] = set()
    reasons: Counter = Counter()
    for t, holiday_calendar in rows:
        res = evaluate_computable(
            t.rate_type, t.rate_components, name=t.name, holiday_calendar=holiday_calendar
        )
        if res.computable:
            ok += 1
            utils_ok.add(t.utility_id)
        else:
            reasons.update(r.split(":", 1)[0] for r in res.reasons)
    return {
        "verified_residential_tariffs": len(rows),
        "computable_residential_tariffs": ok,
        "utilities_with_computable_residential": len(utils_ok),
        "top_blocking_reasons": dict(reasons.most_common(8)),
    }


def compute(session: Session) -> dict:
    # ---- Coverage ----
    cov_rows = session.execute(COVERAGE_SQL).all()
    total_utils = sum(r.total_utils for r in cov_rows)
    covered_utils = sum(r.covered_utils for r in cov_rows)
    covered_any = sum(r.covered_any for r in cov_rows)

    weighted_total = 0.0
    weighted_covered = 0.0
    by_type = []
    for r in cov_rows:
        w = type_weight(r.utype)
        weighted_total += w * r.total_utils
        weighted_covered += w * r.covered_utils
        by_type.append(
            {
                "type": r.utype,
                "total": r.total_utils,
                "covered": r.covered_utils,
                "covered_any": r.covered_any,
                "pct": round(100.0 * r.covered_utils / max(r.total_utils, 1), 1),
                "weight": w,
            }
        )

    coverage_raw = 100.0 * covered_utils / max(total_utils, 1)
    coverage_weighted = 100.0 * weighted_covered / max(weighted_total, 1)

    # ---- Quality ----
    q = session.execute(QUALITY_SQL).first()
    served = q.served or 1
    freshness = 100.0 * float(q.freshness_points or 0) / served
    completeness = 100.0 * (
        0.5 * q.has_energy_n + 0.25 * q.has_fixed_n + 0.25 * q.has_eff_n
    ) / served
    source_type_counts = {
        "official": q.src_official_n or 0,
        "unknown": q.src_unknown_n or 0,
        "third_party": q.src_third_party_n or 0,
    }
    provenance = provenance_score(source_type_counts)

    # ---- Composite ----
    composite = (
        COMPOSITE_WEIGHTS["coverage"] * coverage_weighted
        + COMPOSITE_WEIGHTS["freshness"] * freshness
        + COMPOSITE_WEIGHTS["completeness"] * completeness
        + COMPOSITE_WEIGHTS["provenance"] * provenance
    )

    fb = session.execute(FRESHNESS_BUCKETS_SQL).first()
    mon = session.execute(MONITORING_SQL).first()
    ol = session.execute(OFFICIAL_LENS_SQL).first()

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "health_score": round(composite, 1),
        "grade": letter_grade(composite),
        "components": {
            "coverage_weighted": round(coverage_weighted, 1),
            "coverage_raw": round(coverage_raw, 1),
            "freshness": round(freshness, 1),
            "completeness": round(completeness, 1),
            "provenance": round(provenance, 1),
        },
        "coverage": {
            "active_utilities": total_utils,
            "utilities_with_res_energy_tariff": covered_utils,
            "utilities_with_any_good_tariff": covered_any,
            "by_type": sorted(by_type, key=lambda x: -x["weight"]),
        },
        "quality": {
            "served_tariffs": q.served,
            "verified": q.verified,
            "stale_seeds_served": q.stale_seed,
            "has_energy_component": q.has_energy_n,
            "has_fixed_charge": q.has_fixed_n,
            "has_effective_date": q.has_eff_n,
            "has_source_url": q.has_source_n,
            "source_type_counts": source_type_counts,
        },
        "provenance_method": PROVENANCE_METHOD,
        "provenance": {
            "method": PROVENANCE_METHOD,
            "weights": dict(PROVENANCE_WEIGHTS),
            "source_type_counts": source_type_counts,
        },
        "official_source": {
            "utilities_with_verified_residential": ol.utilities_with_residential,
            "best_residential_official": ol.best_official,
            "best_residential_unknown": ol.best_unknown,
            "best_residential_third_party_only": ol.best_third_party,
            "pct_active_utilities_official": round(
                100.0 * ol.best_official / max(total_utils, 1), 1
            ),
        },
        "freshness_buckets": {
            "<=90d": fb.d90,
            "91-180d": fb.d180,
            "181-365d": fb.d365,
            ">365d": fb.older,
            "never": fb.never,
        },
        "monitoring": {
            "sources": mon.total,
            "errors": mon.errors,
            "ok": mon.ok,
            "never_checked": mon.never_checked,
        },
        "computable": compute_computable(session),
    }


def print_scorecard(r: dict) -> None:
    c = r["components"]
    print("=" * 64)
    print(f"  DATABASE HEALTH SCORE: {r['health_score']}/100   (grade {r['grade']})")
    print(f"  generated {r['generated_at']}")
    print("=" * 64)
    print()
    print("  COMPONENT SCORES")
    print(f"    Coverage (weighted)  {c['coverage_weighted']:5.1f}  {bar(c['coverage_weighted'])}  ×{COMPOSITE_WEIGHTS['coverage']:.0%}")
    print(f"    Freshness            {c['freshness']:5.1f}  {bar(c['freshness'])}  ×{COMPOSITE_WEIGHTS['freshness']:.0%}")
    print(f"    Completeness         {c['completeness']:5.1f}  {bar(c['completeness'])}  ×{COMPOSITE_WEIGHTS['completeness']:.0%}")
    print(f"    Provenance           {c['provenance']:5.1f}  {bar(c['provenance'])}  ×{COMPOSITE_WEIGHTS['provenance']:.0%}")
    print(f"    (Coverage unweighted {c['coverage_raw']:5.1f})")
    print()

    cov = r["coverage"]
    print("  LENS 1 — COVERAGE (breadth)")
    print(f"    {cov['utilities_with_res_energy_tariff']:,} / {cov['active_utilities']:,} active utilities have a verified residential tariff w/ energy rate")
    print(f"    ({cov['utilities_with_any_good_tariff']:,} have any verified tariff with components)")
    print(f"    {'type':<22}{'res+kWh':>9}{'any':>6}{'total':>8}{'pct':>7}  wt")
    for t in cov["by_type"]:
        print(f"    {t['type']:<22}{t['covered']:>9}{t['covered_any']:>6}{t['total']:>8}{t['pct']:>6.0f}%  {t['weight']}")
    print()

    q = r["quality"]
    served = q["served_tariffs"] or 1
    print("  LENS 2 — QUALITY (depth, of served tariffs)")
    print(f"    served (non-superseded): {q['served_tariffs']:,}")
    print(f"    verified:                {q['verified']:,}  ({100*q['verified']/served:.0f}%)")
    print(f"    stale OpenEI seeds:      {q['stale_seeds_served']:,}  ({100*q['stale_seeds_served']/served:.0f}%)")
    print(f"    has energy component:    {q['has_energy_component']:,}  ({100*q['has_energy_component']/served:.0f}%)")
    print(f"    has fixed/customer chg:  {q['has_fixed_charge']:,}  ({100*q['has_fixed_charge']/served:.0f}%)")
    print(f"    has effective date:      {q['has_effective_date']:,}  ({100*q['has_effective_date']/served:.0f}%)")
    print(f"    has source URL:          {q['has_source_url']:,}  ({100*q['has_source_url']/served:.0f}%)")
    print()

    sc = q["source_type_counts"]
    print(f"  PROVENANCE (source quality, method {r['provenance_method']})")
    for st in ("official", "unknown", "third_party"):
        print(
            f"    {st:<12} ×{PROVENANCE_WEIGHTS[st]:.1f}  {sc[st]:>8,}  "
            f"({100*sc[st]/served:.0f}%)"
        )
    o = r["official_source"]
    print(
        f"    {o['best_residential_official']:,} / {r['coverage']['active_utilities']:,} active utilities "
        f"({o['pct_active_utilities_official']:.1f}%) have an official-sourced verified residential tariff"
    )
    print(
        f"    ({o['best_residential_third_party_only']:,} third-party only, "
        f"{o['best_residential_unknown']:,} best is unknown)"
    )
    print()

    fb = r["freshness_buckets"]
    print("  FRESHNESS DISTRIBUTION (served tariffs)")
    for k, v in fb.items():
        print(f"    {k:<10}{v:>7,}  {bar(100*v/served, 30)}")
    print()

    k = r["computable"]
    print("  COMPUTABLE CONTRACT (informational, not in composite)")
    print(f"    {k['computable_residential_tariffs']:,} / {k['verified_residential_tariffs']:,} verified residential tariffs computable")
    print(f"    {k['utilities_with_computable_residential']:,} / {r['coverage']['active_utilities']:,} active utilities have one")
    for reason, n in k["top_blocking_reasons"].items():
        print(f"      {reason:<36}{n:>7,}")
    print()

    m = r["monitoring"]
    print("  MONITORING SOURCES")
    print(f"    {m['ok']:,} ok / {m['errors']:,} error / {m['never_checked']:,} unchecked  (of {m['sources']:,})")
    print("=" * 64)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Compute database health score")
    parser.add_argument("--json", action="store_true", help="Print JSON only")
    parser.add_argument("--snapshot", action="store_true", help="Append a JSON line to history log")
    args = parser.parse_args(argv or sys.argv[1:])

    engine = get_sync_engine()
    with Session(engine) as session:
        result = compute(session)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        print_scorecard(result)

    if args.snapshot:
        log_dir = os.environ.get("APP_LOG_DIR", "/app/logs")
        os.makedirs(log_dir, exist_ok=True)
        path = os.path.join(log_dir, "health_score_history.jsonl")
        with open(path, "a") as fh:
            fh.write(json.dumps(result, default=str) + "\n")
        print(f"\nSnapshot appended to {path}")


if __name__ == "__main__":
    main()
