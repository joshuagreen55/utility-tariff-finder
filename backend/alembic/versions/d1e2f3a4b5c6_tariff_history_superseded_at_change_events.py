"""Tariff history: superseded_at + append-only tariff_change_events.

- ``tariffs.superseded_at`` (nullable timestamptz). A BEFORE UPDATE trigger
  stamps it on the live → superseded transition so every writer (ORM,
  raw-SQL scripts, Track B) records when a row stopped being live. Rows
  superseded before this revision stay NULL (the time is unknown).
- ``tariff_change_events``: who / what / why / before→after for tariff
  writes. UPDATE / DELETE / TRUNCATE are rejected by trigger.
- A BEFORE DELETE trigger on ``tariffs`` appends a ``hard_delete`` event
  with a JSON snapshot of the row and its components, so any remaining
  legacy hard delete is at least recoverable from the log.

Revision ID: d1e2f3a4b5c6
Revises: c0d1e2f3a4b5
Create Date: 2026-09-25
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "d1e2f3a4b5c6"
down_revision = "c0d1e2f3a4b5"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tariffs",
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "tariff_change_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("utility_id", sa.Integer(), nullable=True),
        sa.Column("decision", sa.String(length=40), nullable=False),
        sa.Column("reason", sa.String(length=50), nullable=True),
        sa.Column("actor_type", sa.String(length=20), nullable=False),
        sa.Column("actor_id", sa.String(length=200), nullable=True),
        sa.Column("before_tariff_id", sa.Integer(), nullable=True),
        sa.Column("after_tariff_id", sa.Integer(), nullable=True),
        sa.Column("ticket_id", sa.String(length=100), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("source_document_hash", sa.String(length=64), nullable=True),
        sa.Column("refresh_run_id", sa.Integer(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=200), nullable=True),
        sa.Column("payload", postgresql.JSONB(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.UniqueConstraint("idempotency_key", name="uq_tariff_change_events_idempotency_key"),
    )
    op.create_index("ix_tariff_change_events_occurred_at", "tariff_change_events", ["occurred_at"])
    op.create_index("ix_tariff_change_events_utility_id", "tariff_change_events", ["utility_id"])
    op.create_index("ix_tariff_change_events_before_tariff_id", "tariff_change_events", ["before_tariff_id"])
    op.create_index("ix_tariff_change_events_after_tariff_id", "tariff_change_events", ["after_tariff_id"])

    op.execute(
        """
        CREATE OR REPLACE FUNCTION tariff_change_events_append_only()
        RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'tariff_change_events is append-only (% rejected)', TG_OP;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER tariff_change_events_no_update_delete
        BEFORE UPDATE OR DELETE ON tariff_change_events
        FOR EACH ROW EXECUTE FUNCTION tariff_change_events_append_only();
        """
    )
    op.execute(
        """
        CREATE TRIGGER tariff_change_events_no_truncate
        BEFORE TRUNCATE ON tariff_change_events
        FOR EACH STATEMENT EXECUTE FUNCTION tariff_change_events_append_only();
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION tariffs_stamp_superseded_at()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.superseded_by_tariff_id IS NULL AND NEW.supersede_reason IS NULL THEN
                NEW.superseded_at := NULL;
            ELSIF OLD.superseded_by_tariff_id IS NULL AND OLD.supersede_reason IS NULL
                  AND NEW.superseded_at IS NULL THEN
                NEW.superseded_at := now();
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER tariffs_stamp_superseded_at
        BEFORE UPDATE OF superseded_by_tariff_id, supersede_reason ON tariffs
        FOR EACH ROW EXECUTE FUNCTION tariffs_stamp_superseded_at();
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION tariffs_audit_hard_delete()
        RETURNS trigger AS $$
        BEGIN
            INSERT INTO tariff_change_events
                (utility_id, decision, reason, actor_type, before_tariff_id, source_url, payload)
            VALUES (
                OLD.utility_id,
                'hard_delete',
                OLD.supersede_reason,
                'db_trigger',
                OLD.id,
                OLD.source_url,
                jsonb_build_object(
                    'tariff', to_jsonb(OLD) - 'raw_openei_data',
                    'components', (
                        SELECT COALESCE(jsonb_agg(to_jsonb(rc) ORDER BY rc.id), '[]'::jsonb)
                        FROM rate_components rc
                        WHERE rc.tariff_id = OLD.id
                    )
                )
            );
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER tariffs_audit_hard_delete
        BEFORE DELETE ON tariffs
        FOR EACH ROW EXECUTE FUNCTION tariffs_audit_hard_delete();
        """
    )


def downgrade():
    op.execute("DROP TRIGGER IF EXISTS tariffs_audit_hard_delete ON tariffs")
    op.execute("DROP FUNCTION IF EXISTS tariffs_audit_hard_delete()")
    op.execute("DROP TRIGGER IF EXISTS tariffs_stamp_superseded_at ON tariffs")
    op.execute("DROP FUNCTION IF EXISTS tariffs_stamp_superseded_at()")
    op.execute("DROP TRIGGER IF EXISTS tariff_change_events_no_truncate ON tariff_change_events")
    op.execute("DROP TRIGGER IF EXISTS tariff_change_events_no_update_delete ON tariff_change_events")
    op.drop_index("ix_tariff_change_events_after_tariff_id", table_name="tariff_change_events")
    op.drop_index("ix_tariff_change_events_before_tariff_id", table_name="tariff_change_events")
    op.drop_index("ix_tariff_change_events_utility_id", table_name="tariff_change_events")
    op.drop_index("ix_tariff_change_events_occurred_at", table_name="tariff_change_events")
    op.drop_table("tariff_change_events")
    op.execute("DROP FUNCTION IF EXISTS tariff_change_events_append_only()")
    op.drop_column("tariffs", "superseded_at")
