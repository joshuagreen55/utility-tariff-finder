"""Check details of utilities that have zero tariffs in given states."""
import argparse
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine


def check(states: list[str]):
    engine = get_sync_engine()
    with Session(engine) as s:
        r = s.execute(text("""
            SELECT u.id, u.name, u.state_province, u.website_url, u.eia_id
            FROM utilities u
            LEFT JOIN tariffs t ON t.utility_id = u.id
            WHERE u.country = 'US' AND u.state_province = ANY(:states) AND u.is_active = true
            GROUP BY u.id, u.name, u.state_province, u.website_url, u.eia_id
            HAVING COUNT(t.id) = 0
            ORDER BY u.state_province, u.name
        """), {"states": states}).fetchall()

        print(f"Found {len(r)} utilities with zero tariffs:\n")
        for row in r:
            uid, name, state, website, eia = row
            print(f"  ID: {uid}")
            print(f"  Name: {name} ({state})")
            print(f"  Website: {website or 'NONE'}")
            print(f"  EIA ID: {eia or 'N/A'}")

            ms = s.execute(text("""
                SELECT url, status::text, last_checked_at
                FROM monitoring_sources
                WHERE utility_id = :uid
            """), {"uid": uid}).fetchone()
            if ms:
                print(f"  Monitoring: {ms[1]} | URL: {ms[0] or 'none'}")
            else:
                print(f"  Monitoring: no record")
            print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--states", required=True)
    args = parser.parse_args()
    states = [s.strip() for s in args.states.split(",")]
    check(states)
