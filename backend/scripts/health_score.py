"""Database health score for utility tariff coverage & quality.

RESIDENTIAL-ONLY. Commercial / industrial / lighting tariffs are excluded
from every composite input and every printed lens. Mysa abandoned
commercial clients for this product; the scorecard measures residential
readiness only. JSON output carries ``"scope": "residential"``.

Produces a single 0-100 Health Score from two lenses, each of which is
also reported on its own so you can see WHERE the score comes from:

  LENS 1 — COVERAGE (breadth): of the active utilities we are supposed to
           serve, what fraction have what the product actually needs — a
           live, verified RESIDENTIAL tariff with an energy component?
           A looser "any verified residential tariff with components"
           count is also reported for context. Reported unweighted AND
           weighted by utility type (an IOU serving millions matters more
           than a 500-meter coop). Commercial-only utilities do not count
           as covered.

  LENS 2 — QUALITY (depth): for live (non-superseded) RESIDENTIAL tariffs,
           how fresh, complete, and well-sourced are they?

Composite Health Score = weighted blend:
    40%  Coverage (type-weighted)
    30%  Freshness
    20%  Completeness (Mysa-ready share — energy + TOU clocks/day type +
         season calendar dates when the rate type needs them; fixed /
         customer charges and bare effective_date are NOT scored)
    10%  Provenance (source quality: official / unknown / third-party)

COMPLETENESS BEHAVIOUR CHANGE (2026-09, ``completeness_method``:
``mysa_energy_tou_season_v1``). Completeness used to be a weighted blend of
``has_energy`` (50%) + ``has_fixed/minimum`` (25%) + ``effective_date``
(25%). Mysa prices kWh and builds TOU schedules — it does not need a fixed
charge to show cost, and ``effective_date`` is the wrong proxy for season
calendar dates. Completeness is now the share of live residential tariffs
that pass the computable contract (``app/services/computable.py``): energy
rates present; TOU family also needs clock windows + day type (and a 24 h
partition); seasonal family also needs inclusive season start/end dates.
Snapshots before the change have no ``completeness_method`` key. Fixed-
charge and effective-date counts remain informational only.

PROVENANCE BEHAVIOUR CHANGE (2026-09, issue #28 — "provenance_method":
"source_type_v2" in the JSON). Provenance used to be "served tariff has a
non-null source_url", which sat at ~100 while live rows cited rate blogs.
It now scores each served residential tariff by ``tariffs.source_type``
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
its history on the other axes): the COMPUTABLE lens mirrors the Mysa-ready
denominator used for Completeness (verified subset + utilities with one),
and the OFFICIAL-SOURCE lens — how many active utilities' best live,
verified residential tariff cites an official source.

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
from collections import Counter
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
# Completeness = share of live residential tariffs Mysa can price
# (evaluate_computable). Old formula used has_energy/has_fixed/effective_date.
COMPLETENESS_METHOD = "mysa_energy_tou_season_v1"
# JSON / scorecard scope: every tariff lens filters to residential.
SCORE_SCOPE = "residential"


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


def completeness_score(mysa_ready: int, served: int) -> float:
    """0-100 Completeness: share of live residential tariffs that are Mysa-ready."""
    if served <= 0:
        return 0.0
    return 100.0 * mysa_ready / served

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
    -- Residential only: commercial-only utilities never count as covered.
    SELECT t.utility_id,
           MAX(CASE WHEN EXISTS (SELECT 1 FROM rate_components rc
                                 WHERE rc.tariff_id = t.id
                                   AND lower(rc.component_type::text) = 'energy')
                THEN 1 ELSE 0 END) AS has_res_energy
    FROM tariffs t
    WHERE t.last_verified_at IS NOT NULL
      AND t.superseded_by_tariff_id IS NULL
      AND t.supersede_reason IS NULL
      AND lower(t.customer_class::text) = 'residential'
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
      AND lower(t.customer_class::text) = 'residential'
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
    -- informational component counts (not used in Completeness)
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
  AND lower(customer_class::text) = 'residential'
""")

MONITORING_SQL = text("""
SELECT
  COUNT(*)                                                        AS total,
  COUNT(*) FILTER (WHERE lower(status::text) = 'error')           AS errors,
  COUNT(*) FILTER (WHERE lower(status::text) = 'unchanged')       AS ok,
  COUNT(*) FILTER (WHERE last_checked_at IS NULL)                 AS never_checked
FROM monitoring_sources
""")


def _live_residential_rows(session: Session, *, verified_only: bool = False):
    """Load live residential tariffs (+ holiday calendar) for Mysa-readiness checks."""
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    from app.models import CustomerClass, Tariff, Utility

    stmt = (
        select(Tariff, Utility.holiday_calendar)
        .join(Utility, Utility.id == Tariff.utility_id)
        .options(selectinload(Tariff.rate_components))
        .where(
            Tariff.customer_class == CustomerClass.RESIDENTIAL,
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
    )
    if verified_only:
        stmt = stmt.where(
            Utility.is_active.is_(True),
            Tariff.last_verified_at.is_not(None),
        )
    return session.execute(stmt).all()


def _mysa_ready_stats(rows) -> dict:
    """Evaluate the computable contract over tariff rows; return counts + reasons."""
    from app.services.computable import evaluate_computable
    from app.services.price_basis import base_only_from_factors

    ok = 0
    utils_ok: set[int] = set()
    reasons: Counter = Counter()
    # Mysa-relevant axis counters (informational breakdown for the scorecard).
    has_energy = 0
    missing_energy = 0
    tou_missing_clocks = 0
    tou_missing_day_type = 0
    seasonal_missing_dates = 0
    for t, holiday_calendar in rows:
        res = evaluate_computable(
            t.rate_type, t.rate_components, name=t.name, holiday_calendar=holiday_calendar
        )
        reasons_all = list(res.reasons)
        # R21: base-only prices (riders referenced, not added) are not Mysa-complete.
        if base_only_from_factors(getattr(t, "confidence_factors", None), t.rate_components):
            reasons_all.append("base_only_riders_not_added")
        reason_codes = {r.split(":", 1)[0] for r in reasons_all}
        # evaluate_computable emits missing_energy_rates when no numeric ENERGY
        # rows exist — that is the same gate Mysa uses for flat tariffs.
        if "missing_energy_rates" in reason_codes:
            missing_energy += 1
        else:
            has_energy += 1
        if "tou_missing_clock_windows" in reason_codes:
            tou_missing_clocks += 1
        if "tou_missing_day_type" in reason_codes:
            tou_missing_day_type += 1
        if "seasonal_missing_calendar_dates" in reason_codes:
            seasonal_missing_dates += 1
        if not reasons_all:
            ok += 1
            utils_ok.add(t.utility_id)
        else:
            reasons.update(r.split(":", 1)[0] for r in reasons_all)
    return {
        "served": len(rows),
        "mysa_ready": ok,
        "utilities_with_mysa_ready": len(utils_ok),
        "has_energy_rates": has_energy,
        "missing_energy_rates": missing_energy,
        "tou_missing_clock_windows": tou_missing_clocks,
        "tou_missing_day_type": tou_missing_day_type,
        "seasonal_missing_calendar_dates": seasonal_missing_dates,
        "top_blocking_reasons": dict(reasons.most_common(8)),
    }


def compute_mysa_completeness(session: Session) -> dict:
    """Mysa Completeness over all live (served) residential tariffs."""
    stats = _mysa_ready_stats(_live_residential_rows(session, verified_only=False))
    return {
        "method": COMPLETENESS_METHOD,
        "served_tariffs": stats["served"],
        "mysa_ready_tariffs": stats["mysa_ready"],
        "has_energy_rates": stats["has_energy_rates"],
        "missing_energy_rates": stats["missing_energy_rates"],
        "tou_missing_clock_windows": stats["tou_missing_clock_windows"],
        "tou_missing_day_type": stats["tou_missing_day_type"],
        "seasonal_missing_calendar_dates": stats["seasonal_missing_calendar_dates"],
        "top_blocking_reasons": stats["top_blocking_reasons"],
    }


def compute_computable(session: Session) -> dict:
    """Computable-contract lens over live, verified residential tariffs."""
    stats = _mysa_ready_stats(_live_residential_rows(session, verified_only=True))
    return {
        "verified_residential_tariffs": stats["served"],
        "computable_residential_tariffs": stats["mysa_ready"],
        "utilities_with_computable_residential": stats["utilities_with_mysa_ready"],
        "top_blocking_reasons": stats["top_blocking_reasons"],
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

    mysa = compute_mysa_completeness(session)
    # Denominator matches QUALITY_SQL served (live residential); mysa stats
    # use the same filter so the ratio is consistent.
    completeness = completeness_score(mysa["mysa_ready_tariffs"], mysa["served_tariffs"] or served)

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
        "scope": SCORE_SCOPE,
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
        "completeness_method": COMPLETENESS_METHOD,
        "completeness": {
            "method": COMPLETENESS_METHOD,
            "mysa_ready_tariffs": mysa["mysa_ready_tariffs"],
            "served_tariffs": mysa["served_tariffs"],
            "has_energy_rates": mysa["has_energy_rates"],
            "missing_energy_rates": mysa["missing_energy_rates"],
            "tou_missing_clock_windows": mysa["tou_missing_clock_windows"],
            "tou_missing_day_type": mysa["tou_missing_day_type"],
            "seasonal_missing_calendar_dates": mysa["seasonal_missing_calendar_dates"],
            "top_blocking_reasons": mysa["top_blocking_reasons"],
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
    print(f"  DATABASE HEALTH SCORE (residential-only): {r['health_score']}/100   (grade {r['grade']})")
    print(f"  scope: residential tariffs only — commercial/other classes excluded")
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
    print("  LENS 1 — COVERAGE (breadth, residential)")
    print(f"    {cov['utilities_with_res_energy_tariff']:,} / {cov['active_utilities']:,} active utilities have a verified residential tariff w/ energy rate")
    print(f"    ({cov['utilities_with_any_good_tariff']:,} have any verified residential tariff with components)")
    print(f"    {'type':<22}{'res+kWh':>9}{'any':>6}{'total':>8}{'pct':>7}  wt")
    for t in cov["by_type"]:
        print(f"    {t['type']:<22}{t['covered']:>9}{t['covered_any']:>6}{t['total']:>8}{t['pct']:>6.0f}%  {t['weight']}")
    print()

    q = r["quality"]
    served = q["served_tariffs"] or 1
    print("  LENS 2 — QUALITY (depth, live residential tariffs)")
    print(f"    served (non-superseded residential): {q['served_tariffs']:,}")
    print(f"    verified:                {q['verified']:,}  ({100*q['verified']/served:.0f}%)")
    print(f"    stale OpenEI seeds:      {q['stale_seeds_served']:,}  ({100*q['stale_seeds_served']/served:.0f}%)")
    print(f"    has source URL:          {q['has_source_url']:,}  ({100*q['has_source_url']/served:.0f}%)")
    print(f"    has fixed/customer chg:  {q['has_fixed_charge']:,}  ({100*q['has_fixed_charge']/served:.0f}%)  [informational — not in Completeness]")
    print(f"    has effective date:      {q['has_effective_date']:,}  ({100*q['has_effective_date']/served:.0f}%)  [informational — not in Completeness]")
    print()

    cm = r["completeness"]
    cm_served = cm["served_tariffs"] or 1
    print(f"  COMPLETENESS (Mysa-ready, method {r['completeness_method']})")
    print(
        f"    {cm['mysa_ready_tariffs']:,} / {cm['served_tariffs']:,} live residential tariffs "
        f"Mysa can price ({100*cm['mysa_ready_tariffs']/cm_served:.0f}%)"
    )
    print(
        f"    has energy rates:          {cm['has_energy_rates']:,}  "
        f"({100*cm['has_energy_rates']/cm_served:.0f}%)"
    )
    print(
        f"    missing energy rates:      {cm['missing_energy_rates']:,}  "
        f"({100*cm['missing_energy_rates']/cm_served:.0f}%)"
    )
    print(
        f"    TOU missing clock windows: {cm['tou_missing_clock_windows']:,}  "
        f"({100*cm['tou_missing_clock_windows']/cm_served:.0f}%)"
    )
    print(
        f"    TOU missing day type:      {cm['tou_missing_day_type']:,}  "
        f"({100*cm['tou_missing_day_type']/cm_served:.0f}%)"
    )
    print(
        f"    seasonal missing dates:    {cm['seasonal_missing_calendar_dates']:,}  "
        f"({100*cm['seasonal_missing_calendar_dates']/cm_served:.0f}%)"
    )
    if cm["top_blocking_reasons"]:
        print("    top blocking reasons:")
        for reason, n in cm["top_blocking_reasons"].items():
            print(f"      {reason:<36}{n:>7,}")
    print()

    sc = q["source_type_counts"]
    print(f"  PROVENANCE (residential source quality, method {r['provenance_method']})")
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
    print("  FRESHNESS DISTRIBUTION (live residential tariffs)")
    for k, v in fb.items():
        print(f"    {k:<10}{v:>7,}  {bar(100*v/served, 30)}")
    print()

    k = r["computable"]
    print("  COMPUTABLE CONTRACT (verified subset; Completeness uses all live rows)")
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
