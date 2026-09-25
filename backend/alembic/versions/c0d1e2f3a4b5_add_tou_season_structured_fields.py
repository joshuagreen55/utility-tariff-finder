"""Add structured TOU clock windows and seasonal calendar dates.

ENERGY rate_components previously relied on free-text ``period_label`` /
``season`` (and optional OpenEI schedule matrices) for TOU/seasonal shape.
Flux and completeness audits need machine-readable clock windows and
inclusive season month/day ranges so UIs can show "On-peak 7–11am" without
parsing labels.

New nullable columns on ``rate_components`` (display labels retained):

- TOU clock: ``period_start_time``, ``period_end_time`` (TIME), ``day_type``
  (weekday | weekend | holiday | all)
- Season calendar: ``season_start_month``, ``season_start_day``,
  ``season_end_month``, ``season_end_day`` (integers; Nov→Mar wrap OK)

No backfill — existing incomplete keepers stay as-is until refreshed.

Revision ID: c0d1e2f3a4b5
Revises: b9c0d1e2f3a4
Create Date: 2026-09-25
"""
from alembic import op
import sqlalchemy as sa

revision = "c0d1e2f3a4b5"
down_revision = "b9c0d1e2f3a4"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "rate_components",
        sa.Column("period_start_time", sa.Time(), nullable=True),
    )
    op.add_column(
        "rate_components",
        sa.Column("period_end_time", sa.Time(), nullable=True),
    )
    op.add_column(
        "rate_components",
        sa.Column("day_type", sa.String(length=20), nullable=True),
    )
    op.add_column(
        "rate_components",
        sa.Column("season_start_month", sa.Integer(), nullable=True),
    )
    op.add_column(
        "rate_components",
        sa.Column("season_start_day", sa.Integer(), nullable=True),
    )
    op.add_column(
        "rate_components",
        sa.Column("season_end_month", sa.Integer(), nullable=True),
    )
    op.add_column(
        "rate_components",
        sa.Column("season_end_day", sa.Integer(), nullable=True),
    )


def downgrade():
    op.drop_column("rate_components", "season_end_day")
    op.drop_column("rate_components", "season_end_month")
    op.drop_column("rate_components", "season_start_day")
    op.drop_column("rate_components", "season_start_month")
    op.drop_column("rate_components", "day_type")
    op.drop_column("rate_components", "period_end_time")
    op.drop_column("rate_components", "period_start_time")
