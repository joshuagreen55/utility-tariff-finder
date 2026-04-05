"""Quick script to list utilities by province/state."""
import sys
import psycopg2

states = sys.argv[1].split(",") if len(sys.argv) > 1 else ["ON", "AB"]
conn = psycopg2.connect(
    dbname="utility_tariff_finder", user="postgres",
    password="postgres", host="db", port=5432,
)
cur = conn.cursor()
placeholders = ",".join(["%s"] * len(states))
cur.execute(
    f"SELECT id, name, state_province FROM utilities WHERE country = 'CA' "
    f"AND state_province IN ({placeholders}) AND is_active = true ORDER BY state_province, name",
    states,
)
ids = []
for row in cur.fetchall():
    print(f"{row[0]}|{row[1]}|{row[2]}")
    ids.append(str(row[0]))
print(f"\nIDS: {','.join(ids)}")
print(f"COUNT: {len(ids)}")
conn.close()
