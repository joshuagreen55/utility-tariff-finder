"""Add rate_page_url_override column to utilities table.

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-04-04
"""
from alembic import op
import sqlalchemy as sa

revision = "d5e6f7a8b9c0"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("utilities", sa.Column("rate_page_url_override", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("utilities", "rate_page_url_override")
