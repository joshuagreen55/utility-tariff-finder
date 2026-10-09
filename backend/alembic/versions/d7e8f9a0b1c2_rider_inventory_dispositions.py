"""Closed-world rider inventory + per-plan dispositions (PR B).

Revision ID: d7e8f9a0b1c2
Revises: c6d7e8f9a0b1
Create Date: 2026-10-09
"""
from alembic import op
import sqlalchemy as sa

revision = "d7e8f9a0b1c2"
down_revision = "c6d7e8f9a0b1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "rider_inventory_entries",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "utility_id",
            sa.Integer(),
            sa.ForeignKey("utilities.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("code", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=500), nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("discovered_from", sa.String(length=40), nullable=True),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("source_page", sa.String(length=80), nullable=True),
        sa.Column("source_quote", sa.Text(), nullable=True),
        sa.Column(
            "superseded_by_entry_id",
            sa.Integer(),
            sa.ForeignKey("rider_inventory_entries.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
        sa.Column("supersede_reason", sa.String(length=50), nullable=True),
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    # One live inventory row per (utility, code); superseded history may reuse.
    op.execute(
        """
        CREATE UNIQUE INDEX uq_rider_inventory_one_live_per_code
        ON rider_inventory_entries (utility_id, code)
        WHERE superseded_by_entry_id IS NULL
          AND supersede_reason IS NULL
        """
    )

    op.create_table(
        "plan_rider_dispositions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "composition_id",
            sa.Integer(),
            sa.ForeignKey("plan_compositions.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column("rider_code", sa.String(length=100), nullable=False),
        sa.Column("disposition", sa.String(length=40), nullable=False),
        sa.Column("disposition_page", sa.String(length=80), nullable=True),
        sa.Column("disposition_quote", sa.Text(), nullable=True),
        sa.Column(
            "inventory_entry_id",
            sa.Integer(),
            sa.ForeignKey("rider_inventory_entries.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "composition_id", "rider_code",
            name="uq_plan_rider_dispositions_comp_code",
        ),
    )


def downgrade():
    op.drop_table("plan_rider_dispositions")
    op.execute("DROP INDEX IF EXISTS uq_rider_inventory_one_live_per_code")
    op.drop_table("rider_inventory_entries")
