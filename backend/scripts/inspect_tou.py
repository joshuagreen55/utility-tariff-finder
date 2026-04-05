"""Inspect TOU tariff components for NS Power."""
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine

engine = get_sync_engine()
with Session(engine) as s:
    rows = s.execute(text("""
        SELECT t.id, t.name, t.rate_type::text, t.description,
               rc.id as rc_id, rc.component_type::text, rc.rate_value, rc.unit,
               rc.period_label, rc.period_index, rc.season, rc.tier_label
        FROM tariffs t
        JOIN rate_components rc ON rc.tariff_id = t.id
        JOIN utilities u ON u.id = t.utility_id
        WHERE u.state_province = 'NS' AND t.name LIKE '%Time%'
          AND t.customer_class::text = 'RESIDENTIAL'
          AND u.name = 'Nova Scotia Power'
        ORDER BY t.name, rc.component_type::text, rc.period_index, rc.rate_value
    """)).fetchall()

    cur = None
    for r in rows:
        if r[1] != cur:
            cur = r[1]
            print(f"\nTariff: {r[1]} (id={r[0]}, type={r[2]})")
            desc = (r[3] or "none")[:140]
            print(f"  Desc: {desc}")
            print(f"  {'Type':<12s} {'Rate':>10s} {'Unit':<10s} {'Period':<30s} {'Idx':>4s} {'Season':<15s} {'Tier':<15s}")
        season = r[10] or ""
        tier = r[11] or ""
        period = r[8] or ""
        idx = str(r[9]) if r[9] is not None else ""
        print(f"  {r[5]:<12s} {r[6]:>10.5f} {r[7]:<10s} {period:<30s} {idx:>4s} {season:<15s} {tier:<15s}")
