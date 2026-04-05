"""Quick audit of a batch's data quality after a pipeline run."""
import argparse
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine


def audit(states: list[str]):
    engine = get_sync_engine()
    params = {"states": states}

    with Session(engine) as s:
        # 1. Tariffs with no core components
        print("=== TARIFFS WITH NO CORE COMPONENTS (energy/fixed/demand) ===\n")
        r = s.execute(text("""
            SELECT t.id, t.name, t.customer_class::text, u.name, u.state_province,
                   COUNT(rc.id) as total_comps,
                   COUNT(rc.id) FILTER (
                       WHERE LOWER(rc.component_type::text) IN ('energy','fixed','demand')
                   ) as core,
                   STRING_AGG(DISTINCT rc.component_type::text, ', ') as types
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            LEFT JOIN rate_components rc ON rc.tariff_id = t.id
            WHERE u.country = 'US' AND u.state_province = ANY(:states)
            GROUP BY t.id, t.name, t.customer_class, u.name, u.state_province
            HAVING COUNT(rc.id) FILTER (
                WHERE LOWER(rc.component_type::text) IN ('energy','fixed','demand')
            ) = 0
            ORDER BY u.state_province, u.name
        """), params).fetchall()
        print(f"Found {len(r)} tariffs with no core components:")
        for row in r:
            print(f"  [{row[4]}] {row[3][:35]:<35s} | {row[1][:45]:<45s} | {row[2]:<12s} | comps: {row[5]} ({row[7] or 'none'})")

        # 2. Tariffs with zero components
        print(f"\n=== TARIFFS WITH ZERO COMPONENTS ===\n")
        r5 = s.execute(text("""
            SELECT t.id, t.name, u.name, u.state_province
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            LEFT JOIN rate_components rc ON rc.tariff_id = t.id
            WHERE u.country = 'US' AND u.state_province = ANY(:states)
            GROUP BY t.id, t.name, u.name, u.state_province
            HAVING COUNT(rc.id) = 0
            ORDER BY u.state_province, u.name
        """), params).fetchall()
        print(f"Found {len(r5)} tariffs with zero components:")
        for row in r5:
            print(f"  [{row[3]}] {row[2][:35]:<35s} | {row[1][:50]}")

        # 3. High-count utility+class combos (potential over-extraction)
        print(f"\n=== UTILITIES WITH 5+ TARIFFS IN SAME CLASS (potential over-extraction) ===\n")
        r2 = s.execute(text("""
            SELECT u.state_province, u.name, t.customer_class::text,
                   COUNT(t.id) as cnt,
                   STRING_AGG(t.name, ' | ' ORDER BY t.name) as names
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            WHERE u.country = 'US' AND u.state_province = ANY(:states)
            GROUP BY u.state_province, u.name, t.customer_class
            HAVING COUNT(t.id) >= 5
            ORDER BY COUNT(t.id) DESC
        """), params).fetchall()
        print(f"Found {len(r2)} utility+class combos with 5+ tariffs:")
        for row in r2:
            print(f"  [{row[0]}] {row[1][:35]:<35s} | {row[2]:<12s} | {row[3]} tariffs")
            print(f"       {row[4][:200]}")

        # 4. Suspicious energy rates
        print(f"\n=== SUSPICIOUS ENERGY RATES (>$1/kWh or <$0.001/kWh) ===\n")
        r3 = s.execute(text("""
            SELECT u.state_province, u.name, t.name, rc.rate_value, rc.unit
            FROM rate_components rc
            JOIN tariffs t ON t.id = rc.tariff_id
            JOIN utilities u ON u.id = t.utility_id
            WHERE u.country = 'US' AND u.state_province = ANY(:states)
              AND LOWER(rc.component_type::text) = 'energy'
              AND (rc.rate_value > 1.0 OR rc.rate_value < 0.001)
              AND rc.rate_value != 0
            ORDER BY rc.rate_value DESC
            LIMIT 30
        """), params).fetchall()
        print(f"Found {len(r3)} suspicious energy rates:")
        for row in r3:
            print(f"  [{row[0]}] {row[1][:30]:<30s} | {row[2][:40]:<40s} | ${row[3]} {row[4] or ''}")

        # 5. Customer class distribution
        print(f"\n=== CUSTOMER CLASS DISTRIBUTION ===\n")
        r4 = s.execute(text("""
            SELECT t.customer_class::text, COUNT(*)
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            WHERE u.country = 'US' AND u.state_province = ANY(:states)
            GROUP BY t.customer_class
            ORDER BY COUNT(*) DESC
        """), params).fetchall()
        for row in r4:
            print(f"  {row[0]:<20s} {row[1]:>5d}")

        # 6. Per-utility summary
        print(f"\n=== PER-UTILITY TARIFF COUNTS ===\n")
        r6 = s.execute(text("""
            SELECT u.state_province, u.name, COUNT(t.id) as tariffs,
                   COUNT(DISTINCT t.customer_class) as classes,
                   SUM(CASE WHEN rc_counts.core > 0 THEN 1 ELSE 0 END) as with_core
            FROM utilities u
            LEFT JOIN tariffs t ON t.utility_id = u.id
            LEFT JOIN (
                SELECT tariff_id,
                       COUNT(*) FILTER (
                           WHERE LOWER(component_type::text) IN ('energy','fixed','demand')
                       ) as core
                FROM rate_components GROUP BY tariff_id
            ) rc_counts ON rc_counts.tariff_id = t.id
            WHERE u.country = 'US' AND u.state_province = ANY(:states) AND u.is_active = true
            GROUP BY u.state_province, u.name
            ORDER BY u.state_province, u.name
        """), params).fetchall()
        print(f"{'State':<6s} {'Utility':<40s} {'Tariffs':>8s} {'Classes':>8s} {'w/Core':>8s}")
        print("-" * 75)
        for row in r6:
            tariffs = row[2] or 0
            flag = " ***" if tariffs == 0 else ""
            print(f"  {row[0]:<4s} {row[1][:38]:<40s} {tariffs:>6d} {row[3] or 0:>8d} {row[4] or 0:>8d}{flag}")

        # 7. Failed utilities (0 tariffs)
        print(f"\n=== UTILITIES WITH ZERO TARIFFS ===\n")
        r7 = s.execute(text("""
            SELECT u.state_province, u.name, u.website_url
            FROM utilities u
            LEFT JOIN tariffs t ON t.utility_id = u.id
            WHERE u.country = 'US' AND u.state_province = ANY(:states) AND u.is_active = true
            GROUP BY u.state_province, u.name, u.website_url
            HAVING COUNT(t.id) = 0
            ORDER BY u.state_province, u.name
        """), params).fetchall()
        print(f"Found {len(r7)} utilities with zero tariffs:")
        for row in r7:
            print(f"  [{row[0]}] {row[1][:40]:<40s} | {row[2] or 'no website'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", required=True, help="Comma-separated state codes")
    args = parser.parse_args()
    states = [s.strip() for s in args.states.split(",")]
    audit(states)
