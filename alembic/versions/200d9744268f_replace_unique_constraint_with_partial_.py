"""Replace unique constraint with partial unique constraint for active tables

Revision ID: 200d9744268f
Revises: c17f1101ab6d
Create Date: 2025-05-26 08:49:35.776735

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "200d9744268f"
down_revision: Union[str, None] = "c17f1101ab6d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Manually recreate the table without the unique constraint
    # First, create a new table with the desired structure
    op.create_table(
        "tables_new",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("schema_id", sa.Integer(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("partition_keys", sa.JSON(), nullable=True),
        sa.Column("sort_keys", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["database_id"], ["databases.id"]),
        sa.ForeignKeyConstraint(["schema_id"], ["schemas.id"]),
    )

    # Copy data from old table to new table
    op.execute("""
        INSERT INTO tables_new (id, name, schema_id, database_id, partition_keys, sort_keys, status)
        SELECT id, name, schema_id, database_id, partition_keys, sort_keys, status
        FROM tables
    """)

    # Drop the old table
    op.drop_table("tables")

    # Rename the new table
    op.rename_table("tables_new", "tables")

    # Create the partial unique index
    op.create_index(
        "ix_unique_active_table_name",
        "tables",
        ["database_id", "schema_id", "name"],
        unique=True,
        sqlite_where=sa.text("status = 'active'"),
    )


def downgrade() -> None:
    # Drop the partial unique index
    op.drop_index("ix_unique_active_table_name", table_name="tables")

    # Recreate the table with the original unique constraint
    op.create_table(
        "tables_new",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("schema_id", sa.Integer(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("partition_keys", sa.JSON(), nullable=True),
        sa.Column("sort_keys", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["database_id"], ["databases.id"]),
        sa.ForeignKeyConstraint(["schema_id"], ["schemas.id"]),
        sa.UniqueConstraint("database_id", "schema_id", "name"),
    )

    # Copy data back
    op.execute("""
        INSERT INTO tables_new (id, name, schema_id, database_id, partition_keys, sort_keys, status)
        SELECT id, name, schema_id, database_id, partition_keys, sort_keys, status
        FROM tables
    """)

    # Drop the old table and rename
    op.drop_table("tables")
    op.rename_table("tables_new", "tables")
