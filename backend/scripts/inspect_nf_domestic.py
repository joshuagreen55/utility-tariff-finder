"""Inspect Newfoundland Power Domestic Service tariff components."""
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine

engine = get_sync_engine()
with Session(engine) as s:
    rows = s.execute(text("""
        SELECT t.id, t.name, t.code, t.rate_type::text, t.description, t.source_url,
               rc.id as rc_id, rc.component_type::text, rc.rate_value, rc.unit,
               rc.period_label, rc.period_index, rc.season, rc.tier_label,
               rc.tier_min_kwh, rc.tier_max_kwh
        FROM tariffs t
        JOIN rate_components rc ON rc.tariff_id = t.id
        JOIN utilities u ON u.id = t.utility_id
        WHERE u.name LIKE '%Newfoundland Power%'
          AND t.name = 'Domestic Service'
        ORDER BY rc.component_type::text, rc.rate_value
    """)).fetchall()

    if not rows:
        print("No rows found!")
    else:
        print(f"Tariff: {rows[0][1]} (id={rows[0][0]}, code={rows[0][2]}, type={rows[0][3]})")
        print(f"Source: {rows[0][5]}")
        print(f"Description: {rows[0][4]}")
        print()
        print(f"{'RC_ID':>6s} {'Type':<12s} {'Rate':>12s} {'Unit':<12s} {'Period':<20s} {'Season':<15s} {'Tier':<20s} {'Min':>8s} {'Max':>8s}")
        for r in rows:
            tier = r[13] or ""
            period = r[10] or ""
            season = r[12] or ""
            mn = str(r[14]) if r[14] is not None else ""
            mx = str(r[15]) if r[15] is not None else ""
            print(f"{r[6]:>6d} {r[7]:<12s} {r[8]:>12.5f} {r[9]:<12s} {period:<20s} {season:<15s} {tier:<20s} {mn:>8s} {mx:>8s}")
