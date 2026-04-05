"""Add confidence_score and confidence_factors columns to tariffs table.

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-04-04
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "c4d5e6f7a8b9"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("tariffs", sa.Column("confidence_score", sa.Float(), nullable=True))
    op.add_column("tariffs", sa.Column("confidence_factors", JSONB(), nullable=True))


def downgrade():
    op.drop_column("tariffs", "confidence_factors")
    op.drop_column("tariffs", "confidence_score")
