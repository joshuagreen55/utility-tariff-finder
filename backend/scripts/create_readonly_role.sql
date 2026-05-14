-- create_readonly_role.sql
--
-- Provision a read-only Postgres role for ad-hoc querying by humans
-- (data analysts, partners, reviewers). The role can SELECT from every
-- table in the public schema but cannot INSERT/UPDATE/DELETE/DDL.
--
-- Run as the superuser inside the `utility_tariff_finder` database,
-- passing the desired password via psql's `-v` flag (note the
-- single-quotes around the value so it becomes a SQL literal):
--
--   docker compose exec -T db psql -U postgres -d utility_tariff_finder \
--     -v readonly_password="'choose-a-strong-password'" \
--     -f - < backend/scripts/create_readonly_role.sql
--
-- Re-running with a new password rotates it. The plaintext secret only
-- ever lives in your shell history (clear it afterwards) and not in
-- this file.

\set ON_ERROR_STOP on

-- Stash the password in a session-level custom GUC. psql substitutes
-- `:'readonly_password'` into a SQL literal *here* (outside any
-- dollar-quoted block, where substitution would be skipped), so the
-- DO block below can read it back via current_setting() and use it
-- inside an EXECUTE.
SET myapp.readonly_password = :'readonly_password';

-- Create the login role on first run, or rotate the password on
-- subsequent runs.
DO $$
DECLARE
  pw text := current_setting('myapp.readonly_password');
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'readonly_user') THEN
    EXECUTE format('CREATE ROLE readonly_user WITH LOGIN PASSWORD %L', pw);
  ELSE
    EXECUTE format('ALTER ROLE readonly_user WITH LOGIN PASSWORD %L', pw);
  END IF;
END
$$;

REVOKE ALL ON DATABASE utility_tariff_finder FROM readonly_user;
GRANT CONNECT ON DATABASE utility_tariff_finder TO readonly_user;

GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO readonly_user;

-- New tables created later (migrations, seeds) also get SELECT.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON TABLES TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT ON SEQUENCES TO readonly_user;

-- Hard-cap the role so a runaway query cannot exhaust the VM.
-- 5 minute statement timeout is generous for analytics but stops
-- infinite scans. Idle transactions are killed after 10 minutes so a
-- forgotten DBeaver window can't pin a connection forever.
ALTER ROLE readonly_user SET statement_timeout = '5min';
ALTER ROLE readonly_user SET idle_in_transaction_session_timeout = '10min';

\echo 'readonly_user provisioned. Verify with:  \\du readonly_user'
