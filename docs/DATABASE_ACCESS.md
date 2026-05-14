# Database access

How to give a teammate (analyst, partner, contractor) read-only query
access to the Utility Tariff Finder production database, and how
they should connect.

The database is **PostgreSQL 16 + PostGIS**, running in Docker on the
production GCP VM (`utility-tariff-finder` in `us-central1-a`). It is
**not** exposed to the public internet — every connection has to come
in over SSH. That is the property we want.

## TL;DR (for the person who wants to query)

1. Get your GCP account added to the project (one-time, ask the owner).
2. Install `gcloud` and `psql` (or DBeaver / TablePlus / pgcli).
3. Open an SSH tunnel:

   ```bash
   gcloud compute ssh utility-tariff-finder \
     --zone=us-central1-a \
     --project=ultra-tendril-490700-q3 \
     -- -L 5432:localhost:5432 -N
   ```

4. In another terminal, connect to `localhost:5432` as
   `readonly_user` using the password the owner shared:

   ```bash
   psql "postgresql://readonly_user:YOUR_PASSWORD@localhost:5432/utility_tariff_finder"
   ```

That's it. Skip to [Schema cheat sheet](#schema-cheat-sheet) for what
to query, and [Example queries](#example-queries) for ready-to-run
snippets.

---

## Architecture and threat model

```
your laptop  ──SSH (port 22)──▶  GCP VM (utility-tariff-finder)
                                  └─ docker network ─ Postgres :5432
                                                       (bound only to
                                                        the VM loopback)
```

- The Postgres container binds **`127.0.0.1:5432` on the VM host**
  (see `docker-compose.yml`). It is **not** reachable from the
  public internet — only processes on the VM can connect.
- SSH (port 22) is gated by **GCP IAM** plus the VM's firewall rule.
  Only people with project access and an SSH key on the VM (managed
  automatically by GCP OS Login) can get in.
- The `readonly_user` role can `SELECT` but cannot `INSERT`, `UPDATE`,
  `DELETE`, or run DDL. It also has a 5-minute statement timeout to
  prevent a runaway query from eating the VM.

So a leaked `readonly_user` password is still useless without GCP
project access, and a curious GCP user who somehow has access still
can't mutate data through this role.

---

## One-time setup (owner side)

You only need to do this section **once**, when first turning on
external access. Skip to [Granting a teammate access](#granting-a-teammate-access)
on every subsequent person.

### 1. Roll the change to docker-compose

The repo's `docker-compose.yml` already binds the DB port to the VM
loopback. On the production VM, pull and recreate the `db` service:

```bash
gcloud compute ssh utility-tariff-finder --zone=us-central1-a
# on the VM:
cd ~/utility-tariff-finder
git pull
docker compose up -d db   # recreates the container with the new port binding
docker compose ps         # confirm: db should now show 127.0.0.1:5432->5432/tcp
```

> **Why `127.0.0.1:` and not just `5432:5432`?** Without the loopback
> prefix Docker would publish the port on every interface, including
> the VM's public IP, and we'd be one firewall mistake away from a
> world-open database. The loopback prefix is the safety belt.

### 2. Create the read-only role

The role and its grants are defined in
[`backend/scripts/create_readonly_role.sql`](../backend/scripts/create_readonly_role.sql).
Pick a strong password (e.g. `openssl rand -base64 24`) and run:

```bash
# from your laptop, in the repo root, with the VM logged in
PASSWORD='paste-the-generated-password-here'

gcloud compute scp backend/scripts/create_readonly_role.sql \
  utility-tariff-finder:~/create_readonly_role.sql \
  --zone=us-central1-a

gcloud compute ssh utility-tariff-finder --zone=us-central1-a -- \
  "docker compose -f ~/utility-tariff-finder/docker-compose.yml \
     exec -T db psql -U postgres -d utility_tariff_finder \
     -v readonly_password=\"'${PASSWORD}'\" \
     -f /dev/stdin" < backend/scripts/create_readonly_role.sql
```

Re-running the script with a new `${PASSWORD}` **rotates** the
existing role's password. Do this any time someone leaves the project.

Verify:

```bash
gcloud compute ssh utility-tariff-finder --zone=us-central1-a -- \
  "docker compose -f ~/utility-tariff-finder/docker-compose.yml \
     exec -T db psql -U postgres -d utility_tariff_finder -c '\\du readonly_user'"
```

You should see `readonly_user` listed with no special attributes.

---

## Granting a teammate access

For each person who needs to query:

1. **Add them to the GCP project.** In the Google Cloud Console for
   `ultra-tendril-490700-q3`, give them at minimum:
   - `roles/compute.osLogin` — lets `gcloud compute ssh` work without
     manual SSH key plumbing.
   - `roles/iap.tunnelResourceAccessor` *(only if you front SSH with
     IAP, which we currently don't)*.
   - `roles/compute.viewer` — convenience, lets them list the VM.

   Owner-only roles like `roles/owner` or `roles/editor` are **not**
   required and should be avoided.

2. **Share the readonly password out-of-band** (1Password, Bitwarden,
   etc. — never email, Slack DM is acceptable for short-lived
   credentials only). Tell them which DB host/user/db to use; the
   canonical values are in the TL;DR above.

3. **Point them at this doc.** Everything below is for them.

---

## How to connect (querier side)

### Prerequisites

- A Google account that's been added to the `ultra-tendril-490700-q3`
  GCP project (the owner does this).
- [Google Cloud CLI (`gcloud`)](https://cloud.google.com/sdk/docs/install)
  installed and logged in:

  ```bash
  gcloud auth login
  gcloud config set project ultra-tendril-490700-q3
  ```

- A Postgres client. Any of these will work:
  - **`psql`** — comes with `brew install postgresql@16` on macOS or
    `apt install postgresql-client` on Linux.
  - **DBeaver** — free, cross-platform, has a nice GUI and query
    history. <https://dbeaver.io/>
  - **TablePlus** — paid macOS app, very polished.
  - **`pgcli`** — `pip install pgcli`, autocomplete in the terminal.
  - **Python / pandas** — `pip install psycopg2-binary sqlalchemy pandas`.

### Open the SSH tunnel

This single command (a) opens an SSH session to the VM and
(b) forwards your laptop's port `5432` to the VM's
`localhost:5432` where Postgres is listening. Leave it running:

```bash
gcloud compute ssh utility-tariff-finder \
  --zone=us-central1-a \
  --project=ultra-tendril-490700-q3 \
  -- -L 5432:localhost:5432 -N
```

Flags explained:
- `-L 5432:localhost:5432` — local port 5432 → remote `localhost:5432`.
- `-N` — don't open a shell, just hold the tunnel open. Press
  `Ctrl-C` to close it.

If your laptop already has something on port 5432 (e.g. a local
Postgres), pick a different local port:

```bash
gcloud compute ssh utility-tariff-finder \
  --zone=us-central1-a \
  --project=ultra-tendril-490700-q3 \
  -- -L 5433:localhost:5432 -N
```

…and then connect to `localhost:5433` below.

### Connect with `psql`

In a second terminal:

```bash
psql "postgresql://readonly_user:YOUR_PASSWORD@localhost:5432/utility_tariff_finder"
```

Or, to avoid putting the password in your shell history, use `PGPASSWORD`:

```bash
PGPASSWORD='YOUR_PASSWORD' psql \
  -h localhost -p 5432 \
  -U readonly_user \
  -d utility_tariff_finder
```

### Connect with DBeaver / TablePlus

Create a new **PostgreSQL** connection with:

| Field    | Value                    |
|----------|--------------------------|
| Host     | `localhost`              |
| Port     | `5432`                   |
| Database | `utility_tariff_finder`  |
| User     | `readonly_user`          |
| Password | (whatever was shared)    |
| SSL      | Off (the tunnel itself is encrypted) |

Make sure your `gcloud` SSH tunnel from the previous step is running
when you hit **Test connection**.

### Connect from Python / pandas

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://readonly_user:YOUR_PASSWORD@localhost:5432/utility_tariff_finder"
)

df = pd.read_sql(
    """
    SELECT u.name AS utility, t.name AS tariff, t.customer_class, t.rate_type
    FROM utilities u
    JOIN tariffs   t ON t.utility_id = u.id
    WHERE u.state_province = 'CA' AND t.customer_class = 'residential'
    LIMIT 100
    """,
    engine,
)
print(df.head())
```

---

## Schema cheat sheet

The full SQLAlchemy models live in `backend/app/models/`. The
tables you care about:

| Table | What's in it |
|-------|--------------|
| `utilities` | One row per US/Canadian electric utility. Name, state/province, EIA id, type (IOU / municipal / co-op / …), optional `rate_page_url_override`. |
| `tariffs` | One row per rate schedule (e.g. "Rate D", "Schedule TOU-A"). Linked to a utility. Has `customer_class` (residential / commercial / industrial / lighting), `rate_type` (flat / tou / tiered / demand / …), `source_url`, `last_verified_at`, `confidence_score`, and a JSONB blob `raw_openei_data` for seeded rows. |
| `rate_components` | The actual numbers. One row per energy / demand / fixed / minimum / adjustment component of a tariff, with units, tier ranges, TOU period labels, etc. |
| `service_territories` | PostGIS geometry + ZIP / postal-code arrays used to map an address to a utility. |
| `monitoring_sources` | URLs we re-check periodically for tariff changes. |
| `monitoring_logs` | One row per check, with content hash and whether it changed. |
| `refresh_runs` | One row per pipeline run (the LLM extraction job). Holds counts of tariffs added/updated/stale and a `summary_json` with per-utility detail. |

Enum values (handy for `WHERE` clauses):

- `tariffs.customer_class`: `residential`, `commercial`, `industrial`, `lighting`
- `tariffs.rate_type`: `flat`, `tou`, `tiered`, `demand`, `seasonal`, `tou_tiered`, `seasonal_tou`, `seasonal_tiered`, `demand_tou`, `complex`
- `rate_components.component_type`: `energy`, `demand`, `fixed`, `minimum`, `adjustment`
- `utilities.utility_type`: `investor_owned`, `municipal`, `cooperative`, `political_subdivision`, `federal`, `state`, `retail_marketer`, `behind_meter`, `community_choice`, `other`
- `utilities.country`: `US`, `CA`

### Quick exploration

```sql
-- list all tables
\dt

-- describe a table
\d tariffs

-- biggest tables (rough size sanity check)
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC
LIMIT 10;
```

---

## Example queries

Residential tariffs for a single utility, newest first:

```sql
SELECT t.id, t.name, t.rate_type, t.last_verified_at, t.source_url
FROM utilities u
JOIN tariffs t ON t.utility_id = u.id
WHERE u.name ILIKE 'Pacific Gas%' AND t.customer_class = 'residential'
ORDER BY t.last_verified_at DESC NULLS LAST;
```

Energy rates ($/kWh) for a specific tariff, including TOU periods:

```sql
SELECT rc.period_label, rc.season, rc.tier_label,
       rc.rate_value, rc.unit
FROM tariffs t
JOIN rate_components rc ON rc.tariff_id = t.id
WHERE t.id = 12345 AND rc.component_type = 'energy'
ORDER BY rc.season, rc.period_index, rc.tier_min_kwh;
```

How fresh is each state's residential coverage:

```sql
SELECT u.state_province,
       COUNT(*)                                            AS residential_tariffs,
       COUNT(*) FILTER (WHERE t.last_verified_at IS NOT NULL) AS verified,
       ROUND(100.0 * COUNT(*) FILTER (WHERE t.last_verified_at IS NOT NULL)
                   / NULLIF(COUNT(*), 0), 1)               AS pct_verified
FROM utilities u
JOIN tariffs   t ON t.utility_id = u.id
WHERE t.customer_class = 'residential'
GROUP BY u.state_province
ORDER BY residential_tariffs DESC;
```

Most recent pipeline (refresh) runs:

```sql
SELECT id, refresh_type, started_at, finished_at,
       utilities_targeted, tariffs_added, tariffs_updated, errors
FROM refresh_runs
ORDER BY started_at DESC
LIMIT 10;
```

Tariffs that still come from the original OpenEI seed (no fresh
extraction yet):

```sql
SELECT u.state_province, COUNT(*) AS stale_openei_tariffs
FROM tariffs t
JOIN utilities u ON u.id = t.utility_id
WHERE t.openei_id IS NOT NULL
  AND t.last_verified_at IS NULL
GROUP BY u.state_province
ORDER BY stale_openei_tariffs DESC;
```

Address-to-utility check using PostGIS (find utilities serving a
point):

```sql
SELECT DISTINCT u.id, u.name
FROM utilities u
JOIN service_territories st ON st.utility_id = u.id
WHERE ST_Contains(st.geometry,
                  ST_SetSRID(ST_MakePoint(-122.4194, 37.7749), 4326));
```

---

## Safety and etiquette

- The role is `SELECT`-only — you can't accidentally clobber data.
- Statement timeout is **5 minutes**. If a query gets killed, narrow
  it (add `WHERE`, `LIMIT`) or ask the owner to bump the timeout for
  that session.
- Idle transactions are killed after **10 minutes**. Don't leave a
  DBeaver session open over the weekend with an uncommitted query.
- The DB is **shared with the live application**. Heavy joins on
  `monitoring_logs` or `service_territories` (PostGIS) can pressure
  the same VM that serves API traffic — consider running them off-hours
  if they're slow.
- **Never** put `readonly_user`'s password in a committed file, in
  CI config, in Slack, or in chat with an AI assistant.

## Troubleshooting

**`bind: Address already in use`** when opening the tunnel.
Something on your laptop is already on port 5432 (often a local
Postgres). Use a different local port: `-L 5433:localhost:5432`,
then connect to `localhost:5433`.

**`Permission denied (publickey)`** from `gcloud compute ssh`.
Your IAM role is probably missing `roles/compute.osLogin`. Ask the
project owner to grant it, then re-run `gcloud auth login`.

**`FATAL: password authentication failed for user "readonly_user"`.**
Either the password is wrong, or it was rotated. Ask the owner for
the current one.

**`canceling statement due to statement timeout`.**
You hit the 5-minute cap. Narrow the query, add a `LIMIT`, or
materialize part of it into a CTE that hits an index.

**Tunnel is up but `psql` hangs / can't connect.**
Confirm the `gcloud compute ssh ... -L ...` command is still running
in the first terminal. Confirm on the VM that the DB is actually
listening on the loopback:

```bash
gcloud compute ssh utility-tariff-finder --zone=us-central1-a \
  --command='docker compose -f ~/utility-tariff-finder/docker-compose.yml ps db'
```

…should show `127.0.0.1:5432->5432/tcp`.
