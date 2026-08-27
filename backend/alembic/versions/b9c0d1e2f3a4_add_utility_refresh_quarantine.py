"""Add refresh-quarantine bookkeeping columns to utilities.

Chronically "sourceless" utilities (small co-ops/munis with no
machine-readable rates on the web) fail extraction every run — the LLM
reaches a page and returns 0 tariffs, or no page ever has rate content.
Retrying them monthly *and* quarterly burns LLM spend for no new data.

These columns let the refresh scheduler track consecutive *structural*
failures (as opposed to transient network errors) and soft-quarantine a
utility after a threshold: skip it on routine runs, but re-check it on a
slow cadence or when its monitoring sources detect a genuinely new page.

State was previously kept in a Redis hash, which is ephemeral (a flush
wipes the quarantine) and invisible to reporting. Moving it into the DB
makes it durable and queryable.

Revision ID: b9c0d1e2f3a4
Revises: a8b9c0d1e2f3
Create Date: 2026-07-08
"""
from alembic import op
import sqlalchemy as sa

revision = "b9c0d1e2f3a4"
down_revision = "a8b9c0d1e2f3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "utilities",
        sa.Column(
            "refresh_fail_streak",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "utilities",
        sa.Column(
            "refresh_quarantined_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "utilities",
        sa.Column("refresh_last_reason", sa.String(length=200), nullable=True),
    )
    op.add_column(
        "utilities",
        sa.Column(
            "refresh_last_attempt_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_utilities_refresh_quarantined_at",
        "utilities",
        ["refresh_quarantined_at"],
        if_not_exists=True,
    )


def downgrade():
    op.drop_index(
        "ix_utilities_refresh_quarantined_at",
        table_name="utilities",
        if_exists=True,
    )
    op.drop_column("utilities", "refresh_last_attempt_at")
    op.drop_column("utilities", "refresh_last_reason")
    op.drop_column("utilities", "refresh_quarantined_at")
    op.drop_column("utilities", "refresh_fail_streak")
