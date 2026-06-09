"""Add superseded_by_tariff_id + supersede_reason columns to tariffs.

Tracks LLM-decided 1:N absorption: when a fresh marketing-style tariff
subsumes one or more granular OpenEI URDB seeds, we point the OpenEI
rows at the fresh tariff rather than deleting them. Keeps the audit
trail so we can revisit decisions, and lets the API filter
superseded rows out of default lookups while keeping them queryable
for historical / sanity-check purposes.

Revision ID: a8b9c0d1e2f3
Revises: f7a8b9c0d1e2
Create Date: 2026-05-14
"""
from alembic import op
import sqlalchemy as sa

revision = "a8b9c0d1e2f3"
down_revision = "f7a8b9c0d1e2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tariffs",
        sa.Column(
            "superseded_by_tariff_id",
            sa.Integer(),
            sa.ForeignKey("tariffs.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
    )
    op.add_column(
        "tariffs",
        sa.Column("supersede_reason", sa.String(length=50), nullable=True),
    )
    op.create_index(
        "ix_tariffs_superseded_by_tariff_id",
        "tariffs",
        ["superseded_by_tariff_id"],
        if_not_exists=True,
    )


def downgrade():
    op.drop_index("ix_tariffs_superseded_by_tariff_id", table_name="tariffs", if_exists=True)
    op.drop_column("tariffs", "supersede_reason")
    op.drop_column("tariffs", "superseded_by_tariff_id")
