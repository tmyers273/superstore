import os
from collections.abc import AsyncGenerator
from typing import Any

from fastapi import Depends
from fastapi_users.db import SQLAlchemyBaseUserTableUUID, SQLAlchemyUserDatabase
from sqlalchemy import JSON, ForeignKey, Index, Integer, String, UniqueConstraint, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from classes import Database, Schema, Table, TableStatus
from set.set_ops import SetOp, SetOpAdd, SetOpDelete, SetOpDeleteAndAdd, SetOpReplace

# Make DATABASE_URL configurable via environment variables
DATA_DIR = os.getenv("DATA_DIR", ".")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{DATA_DIR}/test.db")


class Base(DeclarativeBase):
    pass


class User(SQLAlchemyBaseUserTableUUID, Base):
    pass


class TableVersion(Base):
    __tablename__ = "table_versions"

    table_name: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class MicroPartitionMetadata(Base):
    __tablename__ = "micro_partitions"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(
        String, ForeignKey("table_versions.table_name"), nullable=False
    )
    stats: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    key_prefix: Mapped[str | None] = mapped_column(String, nullable=True)


class Operation(Base):
    __tablename__ = "operations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(
        String, ForeignKey("table_versions.table_name"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'add' or 'replace'
    data: Mapped[Any] = mapped_column(
        JSON, nullable=False
    )  # For add: list of ids, for replace: list of (old_id, new_id) tuples

    def __repr__(self) -> str:
        return f"Operation(id={self.id}, table_name={self.table_name}, version={self.version}, operation_type={self.operation_type}, data={self.data})"

    def to_set_op(self) -> SetOp:
        if self.operation_type == "add":
            return SetOpAdd(self.data)
        elif self.operation_type == "replace":
            d = [tuple(d) for d in self.data]
            return SetOpReplace(d)
        elif self.operation_type == "delete":
            return SetOpDelete(self.data)
        elif self.operation_type == "delete_and_add":
            return SetOpDeleteAndAdd((self.data[0], self.data[1]))
        else:
            raise ValueError(f"Unknown operation type: {self.operation_type}")


class OperationSnapshot(Base):
    __tablename__ = "operation_snapshots"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(
        String, ForeignKey("table_versions.table_name"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    data: Mapped[Any] = mapped_column(JSON, nullable=False)

    def __repr__(self) -> str:
        return f"OperationSnapshot(id={self.id}, table_name={self.table_name}, version={self.version}, data={self.data})"


class DatabaseModel(Base):
    __tablename__ = "databases"

    id: Mapped[int | None] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)

    @classmethod
    def from_database(cls, database: Database) -> "DatabaseModel":
        return DatabaseModel(id=database.id, name=database.name)

    def to_database(self) -> Database:
        assert self.id is not None, (
            "Database id should not be None for persisted objects"
        )
        return Database(id=self.id, name=self.name)


class SchemaModel(Base):
    __tablename__ = "schemas"

    id: Mapped[int | None] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    database_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("databases.id"), nullable=False
    )

    __table_args__ = (UniqueConstraint("database_id", "name"),)

    @classmethod
    def from_schema(cls, schema: Schema) -> "SchemaModel":
        return SchemaModel(
            id=schema.id, name=schema.name, database_id=schema.database_id
        )

    def to_schema(self) -> Schema:
        assert self.id is not None, "Schema id should not be None for persisted objects"
        return Schema(id=self.id, name=self.name, database_id=self.database_id)


class TableModel(Base):
    __tablename__ = "tables"

    id: Mapped[int | None] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    schema_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("schemas.id"), nullable=False
    )
    database_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("databases.id"), nullable=False
    )
    partition_keys: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    sort_keys: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=TableStatus.ACTIVE.value
    )

    __table_args__ = (
        Index(
            "ix_unique_active_table_name",
            "database_id",
            "schema_id",
            "name",
            unique=True,
            sqlite_where=text("status = 'active'"),
        ),
    )

    @classmethod
    def from_table(cls, table: Table) -> "TableModel":
        return TableModel(
            id=table.id,
            name=table.name,
            schema_id=table.schema_id,
            database_id=table.database_id,
            partition_keys=table.partition_keys,
            sort_keys=table.sort_keys,
            status=table.status.value,
        )

    def to_table(self) -> Table:
        assert self.id is not None, "Table id should not be None for persisted objects"
        return Table(
            id=self.id,
            name=self.name,
            schema_id=self.schema_id,
            database_id=self.database_id,
            columns=[],
            partition_keys=self.partition_keys,
            sort_keys=self.sort_keys,
            status=TableStatus(self.status),
        )


engine = create_async_engine(DATABASE_URL)
async_session_maker = async_sessionmaker(engine, expire_on_commit=False)


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_maker() as session:
        yield session


async def get_user_db(session: AsyncSession = Depends(get_async_session)):
    yield SQLAlchemyUserDatabase(session, User)
