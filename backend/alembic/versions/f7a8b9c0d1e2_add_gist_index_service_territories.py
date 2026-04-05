"""Add GiST spatial index on service_territories geometry.

Revision ID: f7a8b9c0d1e2
Revises: e6f7a8b9c0d1
Create Date: 2026-04-05
"""
from alembic import op

revision = "f7a8b9c0d1e2"
down_revision = "e6f7a8b9c0d1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "idx_service_territories_geometry",
        "service_territories",
        ["geometry"],
        postgresql_using="gist",
        if_not_exists=True,
    )


def downgrade():
    op.drop_index("idx_service_territories_geometry", table_name="service_territories")
