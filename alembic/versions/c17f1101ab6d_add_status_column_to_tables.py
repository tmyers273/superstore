"""Add status column to tables

Revision ID: c17f1101ab6d
Revises: 4aee20fc87fb
Create Date: 2025-05-26 08:40:18.866699

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c17f1101ab6d"
down_revision: Union[str, None] = "4aee20fc87fb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add the missing columns to the tables table
    op.add_column("tables", sa.Column("partition_keys", sa.JSON(), nullable=True))
    op.add_column("tables", sa.Column("sort_keys", sa.JSON(), nullable=True))
    op.add_column(
        "tables",
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
    )


def downgrade() -> None:
    # Remove the columns we added
    op.drop_column("tables", "status")
    op.drop_column("tables", "sort_keys")
    op.drop_column("tables", "partition_keys")
