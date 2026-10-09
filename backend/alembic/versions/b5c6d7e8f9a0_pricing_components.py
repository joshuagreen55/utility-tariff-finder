"""Pricing components, versions, and plan compositions (PR A).

Additive tables for the component-first pricing core. Existing ``tariffs`` /
``rate_components`` are unchanged. Soft-supersede columns mirror the tariff
history pattern. A partial unique index enforces one live composition per
``plan_key``.

Revision ID: b5c6d7e8f9a0
Revises: a4b5c6d7e8f9
Create Date: 2026-10-09
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "b5c6d7e8f9a0"
down_revision = "a4b5c6d7e8f9"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "pricing_components",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "utility_id",
            sa.Integer(),
            sa.ForeignKey("utilities.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("kind", sa.String(length=40), nullable=False, index=True),
        sa.Column("code", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=500), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("charge_category", sa.String(length=40), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "utility_id", "kind", "code",
            name="uq_pricing_components_utility_kind_code",
        ),
    )

    op.create_table(
        "pricing_component_versions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "component_id",
            sa.Integer(),
            sa.ForeignKey("pricing_components.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("edition_label", sa.String(length=200), nullable=True),
        sa.Column(
            "superseded_by_version_id",
            sa.Integer(),
            sa.ForeignKey("pricing_component_versions.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
        sa.Column("supersede_reason", sa.String(length=50), nullable=True),
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("unit", sa.String(length=40), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=True),
        sa.Column(
            "cells",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column(
            "percent_base_codes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "multiplier_target_codes",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "loss_sensitive",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("source_document_hash", sa.String(length=64), nullable=True),
        sa.Column("source_page", sa.String(length=80), nullable=True),
        sa.Column("source_quote", sa.Text(), nullable=True),
        sa.Column("applicability_quote", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    op.create_table(
        "plan_compositions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "utility_id",
            sa.Integer(),
            sa.ForeignKey("utilities.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("plan_key", sa.String(length=120), nullable=False, index=True),
        sa.Column("name", sa.String(length=500), nullable=False),
        sa.Column("code", sa.String(length=100), nullable=True),
        sa.Column("rate_type", sa.String(length=40), nullable=True),
        sa.Column("recipe_code", sa.String(length=40), nullable=False),
        sa.Column(
            "tariff_id",
            sa.Integer(),
            sa.ForeignKey("tariffs.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
        sa.Column(
            "is_closed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="live",
            index=True,
        ),
        sa.Column("price_basis_label", sa.String(length=80), nullable=True),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "superseded_by_composition_id",
            sa.Integer(),
            sa.ForeignKey("plan_compositions.id", ondelete="SET NULL"),
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
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    # One live composition per plan_key (VER-3).
    op.execute(
        """
        CREATE UNIQUE INDEX uq_plan_compositions_one_live_per_key
        ON plan_compositions (plan_key)
        WHERE status = 'live'
          AND superseded_by_composition_id IS NULL
          AND supersede_reason IS NULL
        """
    )

    op.create_table(
        "plan_composition_members",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "composition_id",
            sa.Integer(),
            sa.ForeignKey("plan_compositions.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "component_version_id",
            sa.Integer(),
            sa.ForeignKey("pricing_component_versions.id"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "disposition",
            sa.String(length=40),
            nullable=False,
            server_default="applies",
        ),
        sa.Column("disposition_page", sa.String(length=80), nullable=True),
        sa.Column("disposition_quote", sa.Text(), nullable=True),
        sa.UniqueConstraint(
            "composition_id", "component_version_id",
            name="uq_plan_composition_members_comp_version",
        ),
    )


def downgrade():
    op.drop_table("plan_composition_members")
    op.execute("DROP INDEX IF EXISTS uq_plan_compositions_one_live_per_key")
    op.drop_table("plan_compositions")
    op.drop_table("pricing_component_versions")
    op.drop_table("pricing_components")
