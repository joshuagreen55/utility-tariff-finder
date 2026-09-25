"""Document-scoped tariff pins, verification proposals, monitoring text.

- ``tariff_pins``: one active pin per curated live tariff (manual
  correction or agent-verified), scoped to a source document.
- ``tariff_verifications``: proposed refreshes of pinned tariffs and their
  automated accept / hold decision.
- ``monitoring_sources.last_content_text``: normalized text behind
  ``last_content_hash`` (capped) so CHANGED checks can diff old vs new.

Revision ID: f3a4b5c6d7e8
Revises: e2f3a4b5c6d7
Create Date: 2026-09-25
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "f3a4b5c6d7e8"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "tariff_pins",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("tariff_id", sa.Integer(), sa.ForeignKey("tariffs.id"), nullable=False),
        sa.Column("utility_id", sa.Integer(), nullable=False),
        sa.Column("origin", sa.String(length=20), nullable=False),
        sa.Column("cause", sa.String(length=30), nullable=True),
        sa.Column("pinned_source_url", sa.Text(), nullable=False),
        sa.Column("pinned_source_hash", sa.String(length=64), nullable=True),
        sa.Column("ticket_id", sa.String(length=100), nullable=True),
        sa.Column("pinned_by", sa.String(length=200), nullable=True),
        sa.Column("pinned_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("state", sa.String(length=20), nullable=False),
        sa.Column("hold_reason", sa.String(length=60), nullable=True),
        sa.Column("consecutive_holds", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_tariff_pins_tariff_id", "tariff_pins", ["tariff_id"])
    op.create_index("ix_tariff_pins_utility_id", "tariff_pins", ["utility_id"])
    op.create_index(
        "uq_tariff_pins_one_open_per_tariff",
        "tariff_pins",
        ["tariff_id"],
        unique=True,
        postgresql_where=sa.text("state <> 'released'"),
    )

    op.create_table(
        "tariff_verifications",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("pin_id", sa.Integer(), sa.ForeignKey("tariff_pins.id"), nullable=False),
        sa.Column("tariff_id", sa.Integer(), nullable=False),
        sa.Column("utility_id", sa.Integer(), nullable=False),
        sa.Column("trigger", sa.String(length=30), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("new_source_url", sa.Text(), nullable=True),
        sa.Column("new_source_hash", sa.String(length=64), nullable=True),
        sa.Column("proposed", postgresql.JSONB(), nullable=True),
        sa.Column("gate_results", postgresql.JSONB(), nullable=True),
        sa.Column("hold_reason", sa.String(length=60), nullable=True),
        sa.Column("accepted_tariff_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_tariff_verifications_pin_id", "tariff_verifications", ["pin_id"])
    op.create_index("ix_tariff_verifications_tariff_id", "tariff_verifications", ["tariff_id"])
    op.create_index("ix_tariff_verifications_status", "tariff_verifications", ["status"])

    op.add_column("monitoring_sources", sa.Column("last_content_text", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("monitoring_sources", "last_content_text")
    op.drop_index("ix_tariff_verifications_status", table_name="tariff_verifications")
    op.drop_index("ix_tariff_verifications_tariff_id", table_name="tariff_verifications")
    op.drop_index("ix_tariff_verifications_pin_id", table_name="tariff_verifications")
    op.drop_table("tariff_verifications")
    op.drop_index("uq_tariff_pins_one_open_per_tariff", table_name="tariff_pins")
    op.drop_index("ix_tariff_pins_utility_id", table_name="tariff_pins")
    op.drop_index("ix_tariff_pins_tariff_id", table_name="tariff_pins")
    op.drop_table("tariff_pins")
