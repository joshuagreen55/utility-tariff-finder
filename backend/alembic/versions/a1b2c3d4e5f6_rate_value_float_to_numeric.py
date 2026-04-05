"""rate_value float to numeric

Revision ID: a1b2c3d4e5f6
Revises: 7e52f4a06009
Create Date: 2026-03-26 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "7e52f4a06009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "rate_components",
        "rate_value",
        existing_type=sa.Float(),
        type_=sa.Numeric(16, 6),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "rate_components",
        "rate_value",
        existing_type=sa.Numeric(16, 6),
        type_=sa.Float(),
        existing_nullable=False,
    )
