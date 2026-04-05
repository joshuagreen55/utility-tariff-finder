"""Add rate_page_fingerprints table for incremental scraping.

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-04-05
"""
from alembic import op
import sqlalchemy as sa

revision = "e6f7a8b9c0d1"
down_revision = "d5e6f7a8b9c0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "rate_page_fingerprints",
        sa.Column("utility_id", sa.Integer(), sa.ForeignKey("utilities.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("url", sa.Text(), primary_key=True),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.Text(), nullable=True),
        sa.Column("content_length", sa.Integer(), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_fingerprints_utility_id", "rate_page_fingerprints", ["utility_id"])


def downgrade():
    op.drop_index("ix_fingerprints_utility_id", table_name="rate_page_fingerprints")
    op.drop_table("rate_page_fingerprints")
