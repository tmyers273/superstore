"""Initial migration - create base tables

Revision ID: 4aee20fc87fb
Revises:
Create Date: 2025-05-26 08:37:05.508696

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4aee20fc87fb"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create user table
    op.create_table(
        "user",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("hashed_password", sa.String(length=1024), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("is_superuser", sa.Boolean(), nullable=False),
        sa.Column("is_verified", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_user_email"), "user", ["email"], unique=True)

    # Create table_versions table
    op.create_table(
        "table_versions",
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("table_name"),
    )

    # Create databases table
    op.create_table(
        "databases",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    # Create schemas table
    op.create_table(
        "schemas",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["database_id"],
            ["databases.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("database_id", "name"),
    )

    # Create tables table (without the columns that will be added in the next migration)
    op.create_table(
        "tables",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("schema_id", sa.Integer(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["database_id"],
            ["databases.id"],
        ),
        sa.ForeignKeyConstraint(
            ["schema_id"],
            ["schemas.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("database_id", "schema_id", "name"),
    )

    # Create micro_partitions table
    op.create_table(
        "micro_partitions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("stats", sa.JSON(), nullable=False),
        sa.Column("key_prefix", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["table_name"],
            ["table_versions.table_name"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sqlite_autoincrement=True,
    )

    # Create operations table
    op.create_table(
        "operations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("operation_type", sa.String(), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["table_name"],
            ["table_versions.table_name"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create operation_snapshots table
    op.create_table(
        "operation_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("data", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["table_name"],
            ["table_versions.table_name"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sqlite_autoincrement=True,
    )


def downgrade() -> None:
    # Drop tables in reverse order
    op.drop_table("operation_snapshots")
    op.drop_table("operations")
    op.drop_table("micro_partitions")
    op.drop_table("tables")
    op.drop_table("schemas")
    op.drop_table("databases")
    op.drop_table("table_versions")
    op.drop_index(op.f("ix_user_email"), table_name="user")
    op.drop_table("user")
