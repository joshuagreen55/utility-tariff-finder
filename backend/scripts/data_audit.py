"""Comprehensive audit of utility and tariff data quality."""
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine

engine = get_sync_engine()

with Session(engine) as s:
    print("=" * 70)
    print("DATA AUDIT — UTILITY TARIFF FINDER")
    print("=" * 70)

    # 1. Utility counts
    print("\n1. UTILITY COUNTS")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.country,
               COUNT(*) as total,
               SUM(CASE WHEN tc.cnt > 0 THEN 1 ELSE 0 END) as with_tariffs,
               SUM(CASE WHEN tc.cnt IS NULL OR tc.cnt = 0 THEN 1 ELSE 0 END) as without_tariffs
        FROM utilities u
        LEFT JOIN (SELECT utility_id, COUNT(*) as cnt FROM tariffs GROUP BY utility_id) tc
            ON tc.utility_id = u.id
        WHERE u.is_active = true
        GROUP BY u.country ORDER BY u.country
    """)).fetchall()
    for r in rows:
        pct = round(100 * r[2] / r[1]) if r[1] else 0
        print(f"  {r[0]}: {r[1]} active utilities, {r[2]} with tariffs ({pct}%), {r[3]} missing tariffs")

    # 2. Tariff counts
    print("\n2. TARIFF COUNTS")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.country, COUNT(t.id) as total,
               SUM(CASE WHEN t.customer_class::text = 'residential' THEN 1 ELSE 0 END) as res,
               SUM(CASE WHEN t.customer_class::text = 'commercial' THEN 1 ELSE 0 END) as com,
               SUM(CASE WHEN t.customer_class::text NOT IN ('residential','commercial') THEN 1 ELSE 0 END) as other
        FROM tariffs t JOIN utilities u ON u.id = t.utility_id
        WHERE u.is_active = true GROUP BY u.country ORDER BY u.country
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]} tariffs ({r[2]} residential, {r[3]} commercial, {r[4]} other)")

    # 3. Rate components
    print("\n3. RATE COMPONENTS")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.country,
               COUNT(rc.id) as total_components,
               COUNT(DISTINCT t.id) as tariffs_with_components,
               ROUND(AVG(sub.cnt)::numeric, 1) as avg_per_tariff
        FROM tariffs t
        JOIN utilities u ON u.id = t.utility_id
        JOIN (SELECT tariff_id, COUNT(*) as cnt FROM rate_components GROUP BY tariff_id) sub
            ON sub.tariff_id = t.id
        JOIN rate_components rc ON rc.tariff_id = t.id
        WHERE u.is_active = true
        GROUP BY u.country ORDER BY u.country
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]} components across {r[2]} tariffs (avg {r[3]} per tariff)")

    # Tariffs with 0 components
    rows = s.execute(text("""
        SELECT u.country, COUNT(t.id) as empty
        FROM tariffs t JOIN utilities u ON u.id = t.utility_id
        LEFT JOIN rate_components rc ON rc.tariff_id = t.id
        WHERE u.is_active = true AND rc.id IS NULL
        GROUP BY u.country ORDER BY u.country
    """)).fetchall()
    for r in rows:
        print(f"  {r[0]}: {r[1]} tariffs with ZERO components (empty shells)")

    # 4. Data freshness
    print("\n4. DATA FRESHNESS")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.country,
               SUM(CASE WHEN t.last_verified_at >= NOW() - INTERVAL '2 years'
                         OR t.effective_date >= CURRENT_DATE - INTERVAL '2 years' THEN 1 ELSE 0 END) as current_cnt,
               SUM(CASE WHEN (t.last_verified_at < NOW() - INTERVAL '2 years'
                         OR t.effective_date < CURRENT_DATE - INTERVAL '2 years')
                         AND (t.last_verified_at >= NOW() - INTERVAL '5 years'
                         OR t.effective_date >= CURRENT_DATE - INTERVAL '5 years') THEN 1 ELSE 0 END) as aging_cnt,
               SUM(CASE WHEN t.last_verified_at IS NULL AND t.effective_date IS NULL THEN 1 ELSE 0 END) as no_date,
               COUNT(t.id) as total
        FROM tariffs t JOIN utilities u ON u.id = t.utility_id
        WHERE u.is_active = true GROUP BY u.country ORDER BY u.country
    """)).fetchall()
    for r in rows:
        cur_pct = round(100 * r[1] / r[4]) if r[4] else 0
        no_pct = round(100 * r[3] / r[4]) if r[4] else 0
        print(f"  {r[0]}: {r[1]} current ({cur_pct}%), {r[2]} aging, {r[3]} no date info ({no_pct}%) — {r[4]} total")

    # 5. Rate type breakdown
    print("\n5. RATE TYPE BREAKDOWN")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.country, t.rate_type::text, COUNT(*) as cnt
        FROM tariffs t JOIN utilities u ON u.id = t.utility_id
        WHERE u.is_active = true
        GROUP BY u.country, t.rate_type::text ORDER BY u.country, cnt DESC
    """)).fetchall()
    cur = None
    for r in rows:
        if r[0] != cur:
            cur = r[0]
            print(f"  {cur}:")
        print(f"    {str(r[1]):20s} {r[2]:5d}")

    # 6. Component quality
    print("\n6. COMPONENT QUALITY (energy rate values)")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.country,
               SUM(CASE WHEN rc.component_type::text = 'energy' AND rc.rate_value > 0 AND rc.rate_value < 1 THEN 1 ELSE 0 END) as reasonable,
               SUM(CASE WHEN rc.component_type::text = 'energy' AND (rc.rate_value <= 0 OR rc.rate_value >= 1) THEN 1 ELSE 0 END) as suspect,
               SUM(CASE WHEN rc.component_type::text = 'fixed' THEN 1 ELSE 0 END) as fixed,
               SUM(CASE WHEN rc.component_type::text = 'demand' THEN 1 ELSE 0 END) as demand,
               COUNT(rc.id) as total
        FROM rate_components rc
        JOIN tariffs t ON t.id = rc.tariff_id
        JOIN utilities u ON u.id = t.utility_id
        WHERE u.is_active = true GROUP BY u.country ORDER BY u.country
    """)).fetchall()
    for r in rows:
        sus_pct = round(100 * r[2] / (r[1] + r[2])) if (r[1] + r[2]) else 0
        print(f"  {r[0]}: {r[1]} reasonable energy rates, {r[2]} suspect ({sus_pct}%), {r[3]} fixed, {r[4]} demand — {r[5]} total")

    # 7. Canada coverage by province
    print("\n7. CANADA — COVERAGE BY PROVINCE")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.state_province,
               COUNT(DISTINCT u.id) as utils,
               COUNT(DISTINCT CASE WHEN tc.cnt > 0 THEN u.id END) as with_tariffs,
               COALESCE(SUM(tc.cnt), 0) as tariffs,
               COALESCE(SUM(rc_cnt.cnt), 0) as components
        FROM utilities u
        LEFT JOIN (SELECT utility_id, COUNT(*) as cnt FROM tariffs GROUP BY utility_id) tc ON tc.utility_id = u.id
        LEFT JOIN (SELECT t.utility_id, COUNT(rc.id) as cnt FROM tariffs t JOIN rate_components rc ON rc.tariff_id = t.id GROUP BY t.utility_id) rc_cnt ON rc_cnt.utility_id = u.id
        WHERE u.country = 'CA' AND u.is_active = true
        GROUP BY u.state_province ORDER BY u.state_province
    """)).fetchall()
    print(f"  {'Prov':4s}  {'Utils':>5s}  {'w/Tariffs':>9s}  {'Tariffs':>7s}  {'Components':>10s}  {'Avg Comp/Tariff':>15s}")
    for r in rows:
        avg = round(r[4] / r[3], 1) if r[3] else 0
        pct = round(100 * r[2] / r[1]) if r[1] else 0
        print(f"  {r[0]:4s}  {r[1]:5d}  {r[2]:5d} ({pct:2d}%)  {int(r[3]):7d}  {int(r[4]):10d}  {avg:15.1f}")

    # 8. US coverage by state (full)
    print("\n8. US — COVERAGE BY STATE")
    print("-" * 50)
    rows = s.execute(text("""
        SELECT u.state_province,
               COUNT(DISTINCT u.id) as utils,
               COUNT(DISTINCT CASE WHEN tc.cnt > 0 THEN u.id END) as with_tariffs,
               COALESCE(SUM(tc.cnt), 0) as tariffs,
               COALESCE(SUM(rc_cnt.cnt), 0) as components
        FROM utilities u
        LEFT JOIN (SELECT utility_id, COUNT(*) as cnt FROM tariffs GROUP BY utility_id) tc ON tc.utility_id = u.id
        LEFT JOIN (SELECT t.utility_id, COUNT(rc.id) as cnt FROM tariffs t JOIN rate_components rc ON rc.tariff_id = t.id GROUP BY t.utility_id) rc_cnt ON rc_cnt.utility_id = u.id
        WHERE u.country = 'US' AND u.is_active = true
        GROUP BY u.state_province ORDER BY u.state_province
    """)).fetchall()
    print(f"  {'State':5s}  {'Utils':>5s}  {'w/Tariffs':>9s}  {'Tariffs':>7s}  {'Components':>10s}")
    for r in rows:
        pct = round(100 * r[2] / r[1]) if r[1] else 0
        print(f"  {r[0]:5s}  {r[1]:5d}  {r[2]:5d} ({pct:2d}%)  {int(r[3]):7d}  {int(r[4]):10d}")

    # 9. Side-by-side summary
    print("\n" + "=" * 70)
    print("9. SIDE-BY-SIDE COMPARISON: CANADA vs US")
    print("=" * 70)
    rows = s.execute(text("""
        SELECT u.country,
               COUNT(DISTINCT u.id) as utils,
               COUNT(DISTINCT CASE WHEN tc.cnt > 0 THEN u.id END) as utils_with,
               COALESCE(SUM(tc.cnt), 0) as tariffs,
               COALESCE(SUM(rc_cnt.cnt), 0) as components,
               ROUND(COALESCE(SUM(tc.cnt), 0)::numeric / NULLIF(COUNT(DISTINCT CASE WHEN tc.cnt > 0 THEN u.id END), 0), 1) as tariffs_per_util,
               ROUND(COALESCE(SUM(rc_cnt.cnt), 0)::numeric / NULLIF(COALESCE(SUM(tc.cnt), 0), 0), 1) as comp_per_tariff
        FROM utilities u
        LEFT JOIN (SELECT utility_id, COUNT(*) as cnt FROM tariffs GROUP BY utility_id) tc ON tc.utility_id = u.id
        LEFT JOIN (SELECT t.utility_id, COUNT(rc.id) as cnt FROM tariffs t JOIN rate_components rc ON rc.tariff_id = t.id GROUP BY t.utility_id) rc_cnt ON rc_cnt.utility_id = u.id
        WHERE u.is_active = true
        GROUP BY u.country ORDER BY u.country
    """)).fetchall()
    print(f"  {'Metric':<35s}  {'CA':>10s}  {'US':>10s}")
    print(f"  {'-'*35}  {'-'*10}  {'-'*10}")
    ca = {r[0]: r for r in rows}.get("CA")
    us = {r[0]: r for r in rows}.get("US")
    if ca and us:
        ca_cov = round(100 * ca[2] / ca[1]) if ca[1] else 0
        us_cov = round(100 * us[2] / us[1]) if us[1] else 0
        print(f"  {'Active utilities':<35s}  {ca[1]:10d}  {us[1]:10d}")
        print(f"  {'Utilities with tariffs':<35s}  {ca[2]:10d}  {us[2]:10d}")
        print(f"  {'Coverage %':<35s}  {ca_cov:9d}%  {us_cov:9d}%")
        print(f"  {'Total tariffs':<35s}  {int(ca[3]):10d}  {int(us[3]):10d}")
        print(f"  {'Total rate components':<35s}  {int(ca[4]):10d}  {int(us[4]):10d}")
        print(f"  {'Avg tariffs per utility':<35s}  {ca[5]:10.1f}  {us[5]:10.1f}")
        print(f"  {'Avg components per tariff':<35s}  {ca[6]:10.1f}  {us[6]:10.1f}")
