"""Count tariffs and components by customer class and country."""
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine

engine = get_sync_engine()
with Session(engine) as s:
    r = s.execute(text("""
        SELECT t.customer_class::text, u.country, COUNT(t.id), COUNT(DISTINCT rc.id)
        FROM tariffs t
        JOIN utilities u ON u.id = t.utility_id
        LEFT JOIN rate_components rc ON rc.tariff_id = t.id
        GROUP BY t.customer_class, u.country
        ORDER BY u.country, t.customer_class
    """)).fetchall()

    print(f"{'Class':<18s} {'Country':<8s} {'Tariffs':>8s}  {'Components':>10s}")
    print("-" * 50)
    for row in r:
        print(f"{row[0]:<18s} {row[1]:<8s} {row[2]:>8d}  {row[3]:>10d}")

    total_t = sum(row[2] for row in r)
    total_c = sum(row[3] for row in r)
    drop_t = sum(row[2] for row in r if row[0] in ("LIGHTING", "INDUSTRIAL"))
    drop_c = sum(row[3] for row in r if row[0] in ("LIGHTING", "INDUSTRIAL"))
    print("-" * 50)
    print(f"{'TOTAL':<18s} {'':8s} {total_t:>8d}  {total_c:>10d}")
    print(f"{'TO REMOVE':<18s} {'':8s} {drop_t:>8d}  {drop_c:>10d}")
    print(f"{'AFTER CLEANUP':<18s} {'':8s} {total_t - drop_t:>8d}  {total_c - drop_c:>10d}")
    print(f"\nRemoving {drop_t / total_t * 100:.1f}% of tariffs")
