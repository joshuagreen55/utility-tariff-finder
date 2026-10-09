"""Texas TDU / OER policy columns on plan_compositions.

Joshua 2026-10-09:
- Texas competitive: delivery only, supply_status=choose_a_retailer, no all-in.
- Ontario OER: bill-level note (like taxes), never in per-kWh.

Revision ID: c6d7e8f9a0b1
Revises: b5c6d7e8f9a0
Create Date: 2026-10-09
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "c6d7e8f9a0b1"
down_revision = "b5c6d7e8f9a0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "plan_compositions",
        sa.Column(
            "has_all_in",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )
    op.add_column(
        "plan_compositions",
        sa.Column("supply_status", sa.String(length=40), nullable=True),
    )
    op.add_column(
        "plan_compositions",
        sa.Column(
            "bill_level_notes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column("plan_compositions", "bill_level_notes")
    op.drop_column("plan_compositions", "supply_status")
    op.drop_column("plan_compositions", "has_all_in")
