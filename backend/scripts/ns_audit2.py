"""Show all Nova Scotia tariffs with their classes."""
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine

engine = get_sync_engine()
with Session(engine) as s:
    rows = s.execute(text("""
        SELECT u.name, t.name, t.customer_class::text, t.rate_type::text,
               COUNT(rc.id) as comps,
               rc2.component_type::text as sample_type,
               rc2.rate_value as sample_rate,
               rc2.unit as sample_unit
        FROM tariffs t
        JOIN utilities u ON u.id = t.utility_id
        LEFT JOIN rate_components rc ON rc.tariff_id = t.id
        LEFT JOIN LATERAL (
            SELECT component_type, rate_value, unit
            FROM rate_components
            WHERE tariff_id = t.id AND component_type::text = 'energy'
            ORDER BY rate_value
            LIMIT 1
        ) rc2 ON true
        WHERE u.state_province = 'NS' AND u.country = 'CA' AND u.is_active = true
        GROUP BY u.name, t.id, t.name, t.customer_class, t.rate_type,
                 rc2.component_type, rc2.rate_value, rc2.unit
        ORDER BY u.name, t.customer_class, t.name
    """)).fetchall()

    print(f"ALL NS Tariffs ({len(rows)} total):\n")
    cur_util = None
    for r in rows:
        if r[0] != cur_util:
            cur_util = r[0]
            print(f"\n  {cur_util}")
            print(f"  {'':2s}{'Tariff':<40s} {'Class':<15s} {'Type':<15s} Comps  Sample Rate")
        rate_str = f"{r[6]:.4f} {r[7]}" if r[6] else "n/a"
        print(f"    {r[1]:<40s} {r[2]:<15s} {r[3]:<15s} {r[4]:3d}    {rate_str}")

    print("\n\nCustomer class breakdown:")
    rows2 = s.execute(text("""
        SELECT t.customer_class::text, COUNT(*) FROM tariffs t
        JOIN utilities u ON u.id = t.utility_id
        WHERE u.state_province = 'NS' AND u.country = 'CA' AND u.is_active = true
        GROUP BY t.customer_class ORDER BY COUNT(*) DESC
    """)).fetchall()
    for r in rows2:
        print(f"  {r[0]:15s} {r[1]:3d}")
