"""Compare NS Power residential tariffs to identify duplicates."""
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine

engine = get_sync_engine()
with Session(engine) as s:
    tariffs = s.execute(text("""
        SELECT t.id, t.name, t.code, t.rate_type::text, t.source_url,
               t.description
        FROM tariffs t
        JOIN utilities u ON u.id = t.utility_id
        WHERE u.name = 'Nova Scotia Power'
          AND t.customer_class::text = 'RESIDENTIAL'
        ORDER BY t.name
    """)).fetchall()

    for t in tariffs:
        print(f"\n{'='*80}")
        print(f"ID: {t[0]}  |  Name: {t[1]}  |  Code: {t[2]}  |  Type: {t[3]}")
        print(f"Source: {(t[4] or 'none')[:100]}")
        print(f"Desc: {(t[5] or 'none')[:120]}")

        comps = s.execute(text("""
            SELECT rc.component_type::text, rc.rate_value, rc.unit,
                   rc.period_label, rc.season, rc.tier_label
            FROM rate_components rc
            WHERE rc.tariff_id = :tid
            ORDER BY rc.component_type::text, rc.season, rc.rate_value
        """), {"tid": t[0]}).fetchall()

        print(f"Components ({len(comps)}):")
        for c in comps:
            season = c[4] or ""
            period = c[3] or ""
            tier = c[5] or ""
            print(f"  {c[0]:<12s} ${c[1]:<12.5f} {c[2]:<12s} {period:<25s} {season:<15s} {tier}")
