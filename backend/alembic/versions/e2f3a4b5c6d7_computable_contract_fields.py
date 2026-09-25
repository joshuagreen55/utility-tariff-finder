"""Computable contract: utility timezone / holiday calendar, rider inclusion flag.

- ``utilities.timezone`` (IANA, nullable): override for utilities in
  multi-zone jurisdictions. Single-zone states/provinces are derived at read
  time (app.services.timezones), so no backfill.
- ``utilities.holiday_calendar`` (nullable): code of the holiday list that
  ``day_type='holiday'`` TOU rows refer to. Not populated here.
- ``rate_components.included_in_energy`` (bool, default false): marks an
  ADJUSTMENT row already folded into all-in ENERGY so cost consumers do not
  double count it. Existing rows default to false; the computable contract
  flags legacy all-in tariffs with unflagged riders as ambiguous.

Revision ID: e2f3a4b5c6d7
Revises: d1e2f3a4b5c6
Create Date: 2026-09-25
"""
from alembic import op
import sqlalchemy as sa

revision = "e2f3a4b5c6d7"
down_revision = "d1e2f3a4b5c6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("utilities", sa.Column("timezone", sa.String(length=64), nullable=True))
    op.add_column("utilities", sa.Column("holiday_calendar", sa.String(length=40), nullable=True))
    op.add_column(
        "rate_components",
        sa.Column(
            "included_in_energy",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column("rate_components", "included_in_energy")
    op.drop_column("utilities", "holiday_calendar")
    op.drop_column("utilities", "timezone")
