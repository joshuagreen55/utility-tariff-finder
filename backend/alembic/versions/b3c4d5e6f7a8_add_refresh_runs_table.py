"""Add refresh_runs table for tracking monthly tariff refresh history.

Revision ID: b3c4d5e6f7a8
Revises: a1b2c3d4e5f6
Create Date: 2026-04-02
"""
from alembic import op
import sqlalchemy as sa

revision = "b3c4d5e6f7a8"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        DO $$ BEGIN
            CREATE TYPE refreshtype AS ENUM ('monthly', 'quarterly', 'manual');
        EXCEPTION
            WHEN duplicate_object THEN NULL;
        END $$;
    """)
    op.execute("""
        CREATE TABLE refresh_runs (
            id SERIAL PRIMARY KEY,
            refresh_type refreshtype NOT NULL,
            started_at TIMESTAMPTZ DEFAULT now(),
            finished_at TIMESTAMPTZ,
            utilities_targeted INTEGER NOT NULL DEFAULT 0,
            utilities_processed INTEGER NOT NULL DEFAULT 0,
            tariffs_added INTEGER NOT NULL DEFAULT 0,
            tariffs_updated INTEGER NOT NULL DEFAULT 0,
            tariffs_stale INTEGER NOT NULL DEFAULT 0,
            errors INTEGER NOT NULL DEFAULT 0,
            summary_json JSONB,
            error_details TEXT
        );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS refresh_runs")
    op.execute("DROP TYPE IF EXISTS refreshtype")
