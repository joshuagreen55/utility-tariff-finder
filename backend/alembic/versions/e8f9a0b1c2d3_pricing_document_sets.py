"""Per-utility pricing document sets (PR R27-1).

One live document set per utility holds the official current tariff, every
referenced rider sheet, default-supply / provincial commodity, and delivery
docs — each with URL, edition label, and effective date. Soft-supersede only.

Revision ID: e8f9a0b1c2d3
Revises: d7e8f9a0b1c2
Create Date: 2026-10-09
"""
from alembic import op
import sqlalchemy as sa

revision = "e8f9a0b1c2d3"
down_revision = "d7e8f9a0b1c2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "pricing_document_sets",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "utility_id",
            sa.Integer(),
            sa.ForeignKey("utilities.id"),
            nullable=False,
            index=True,
        ),
        sa.Column("recipe_code", sa.String(length=40), nullable=True),
        sa.Column("as_of_date", sa.Date(), nullable=True),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default="live",
        ),
        sa.Column(
            "superseded_by_set_id",
            sa.Integer(),
            sa.ForeignKey("pricing_document_sets.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
        sa.Column("supersede_reason", sa.String(length=50), nullable=True),
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
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
    # One live set per utility.
    op.create_index(
        "uq_pricing_document_sets_live_utility",
        "pricing_document_sets",
        ["utility_id"],
        unique=True,
        postgresql_where=sa.text(
            "superseded_by_set_id IS NULL AND supersede_reason IS NULL"
        ),
    )

    op.create_table(
        "pricing_document_set_members",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "document_set_id",
            sa.Integer(),
            sa.ForeignKey("pricing_document_sets.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        # tariff | rider_sheet | default_supply | provincial_commodity | delivery
        sa.Column("role", sa.String(length=40), nullable=False, index=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.String(length=500), nullable=True),
        sa.Column("edition_label", sa.String(length=200), nullable=True),
        sa.Column("effective_date", sa.Date(), nullable=True),
        sa.Column("publisher_host", sa.String(length=200), nullable=True),
        # True = chosen current edition for this role (or selected rider).
        sa.Column(
            "is_selected",
            sa.Boolean(),
            nullable=False,
            server_default="true",
        ),
        # e.g. marketing_pdf, superseded_edition, draft
        sa.Column("reject_reason", sa.String(length=80), nullable=True),
        sa.Column("content_hash", sa.String(length=64), nullable=True),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_pricing_doc_set_members_set_role",
        "pricing_document_set_members",
        ["document_set_id", "role"],
    )


def downgrade():
    op.drop_index(
        "ix_pricing_doc_set_members_set_role",
        table_name="pricing_document_set_members",
    )
    op.drop_table("pricing_document_set_members")
    op.drop_index(
        "uq_pricing_document_sets_live_utility",
        table_name="pricing_document_sets",
    )
    op.drop_table("pricing_document_sets")
