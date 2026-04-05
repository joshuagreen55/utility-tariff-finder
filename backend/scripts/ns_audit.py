"""Audit Nova Scotia residential tariff data."""
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine

engine = get_sync_engine()

with Session(engine) as s:
    rows = s.execute(text("""
        SELECT u.id, u.name, COUNT(t.id) as tariff_count
        FROM utilities u
        LEFT JOIN tariffs t ON t.utility_id = u.id
        WHERE u.state_province = 'NS' AND u.country = 'CA' AND u.is_active = true
        GROUP BY u.id, u.name ORDER BY u.name
    """)).fetchall()
    print("NS Utilities:")
    for r in rows:
        print(f"  id={r[0]:5d}  {r[1]:40s}  tariffs: {r[2]}")

    print()

    rows = s.execute(text("""
        SELECT u.name, t.id, t.name as tariff_name, t.rate_type::text, t.customer_class::text,
               t.effective_date, t.source_url, COUNT(rc.id) as components
        FROM tariffs t
        JOIN utilities u ON u.id = t.utility_id
        LEFT JOIN rate_components rc ON rc.tariff_id = t.id
        WHERE u.state_province = 'NS' AND u.country = 'CA' AND u.is_active = true
          AND t.customer_class::text = 'residential'
        GROUP BY u.name, t.id, t.name, t.rate_type, t.customer_class, t.effective_date, t.source_url
        ORDER BY u.name, t.name
    """)).fetchall()
    print(f"NS Residential Tariffs ({len(rows)} total):")
    for r in rows:
        src = (r[6] or "")[:70]
        print(f"  {r[0]:35s} | {r[2]:40s} | {r[3]:15s} | comps: {r[7]} | eff: {r[5]}")
        if src:
            print(f"  {'':35s}   src: {src}")

    print()

    rows = s.execute(text("""
        SELECT u.name as util_name, t.name as tariff_name,
               rc.component_type::text, rc.unit, rc.rate_value,
               rc.tier_label, rc.tier_min_kwh, rc.tier_max_kwh,
               rc.period_label, rc.season
        FROM rate_components rc
        JOIN tariffs t ON t.id = rc.tariff_id
        JOIN utilities u ON u.id = t.utility_id
        WHERE u.state_province = 'NS' AND u.country = 'CA' AND u.is_active = true
          AND t.customer_class::text = 'residential'
        ORDER BY u.name, t.name, rc.tier_min_kwh NULLS FIRST, rc.component_type
    """)).fetchall()
    print(f"Rate Components ({len(rows)} total):")
    cur_tariff = None
    for r in rows:
        label = f"{r[0]} -- {r[1]}"
        if label != cur_tariff:
            cur_tariff = label
            print(f"\n  {label}")
        tier = f"tier: {r[5]}" if r[5] else ""
        kwh_range = ""
        if r[6] is not None or r[7] is not None:
            kwh_range = f"[{r[6] or 0}-{r[7] or 'inf'}kWh]"
        period = f"period: {r[8]}" if r[8] else ""
        season = f"season: {r[9]}" if r[9] else ""
        extras = "  ".join(x for x in [tier, kwh_range, period, season] if x)
        print(f"    {r[2]:10s} {r[4]:>12.6f} {r[3]:12s}  {extras}")
