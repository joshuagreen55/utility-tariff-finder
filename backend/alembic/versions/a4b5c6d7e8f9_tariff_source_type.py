"""tariffs.source_type (official | third_party | unknown) + classifier backfill.

Adds ``source_type`` (NOT NULL, default ``unknown``, CHECK-constrained) and
``source_type_reason`` to ``tariffs``, then classifies every existing row —
live and superseded — with ``app.services.source_type.classify_source``
against its utility's website / configured rate URLs. No row is edited by
hand; re-run the same classifier later with
``python -m scripts.backfill_source_type``.

Revision ID: a4b5c6d7e8f9
Revises: f3a4b5c6d7e8
Create Date: 2026-09-26
"""
from alembic import op
import sqlalchemy as sa

revision = "a4b5c6d7e8f9"
down_revision = "f3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tariffs",
        sa.Column("source_type", sa.String(length=20), nullable=False, server_default="unknown"),
    )
    op.add_column("tariffs", sa.Column("source_type_reason", sa.String(length=40), nullable=True))
    op.create_check_constraint(
        "ck_tariffs_source_type",
        "tariffs",
        "source_type IN ('official', 'third_party', 'unknown')",
    )
    op.create_index("ix_tariffs_source_type", "tariffs", ["source_type"])

    from app.services.source_type import reclassify_tariffs

    reclassify_tariffs(op.get_bind())


def downgrade():
    op.drop_index("ix_tariffs_source_type", table_name="tariffs")
    op.drop_constraint("ck_tariffs_source_type", "tariffs", type_="check")
    op.drop_column("tariffs", "source_type_reason")
    op.drop_column("tariffs", "source_type")
