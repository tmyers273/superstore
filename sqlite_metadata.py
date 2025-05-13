from typing import Generator
from sqlalchemy import (
    UniqueConstraint,
    create_engine,
    Column,
    Integer,
    String,
    JSON,
    ForeignKey,
    select,
)
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.ext.declarative import declared_attr

from .classes import Database, Schema
from .metadata import MetadataStore
from .classes import MicroPartition, Statistics, Table
from .s3 import S3Like
import json
import polars as pl
from .set_ops import SetOp, SetOpAdd, SetOpDelete, SetOpReplace, apply

Base = declarative_base()


class TableVersion(Base):
    __tablename__ = "table_versions"

    table_name = Column(String, primary_key=True)
    version = Column(Integer, nullable=False, default=0)


class MicroPartitionMetadata(Base):
    __tablename__ = "micro_partitions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, ForeignKey("table_versions.table_name"), nullable=False)
    header = Column(JSON, nullable=False)


class Operation(Base):
    __tablename__ = "operations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, ForeignKey("table_versions.table_name"), nullable=False)
    version = Column(Integer, nullable=False)
    operation_type = Column(String, nullable=False)  # 'add' or 'replace'
    data = Column(
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
        else:
            raise ValueError(f"Unknown operation type: {self.operation_type}")


class DatabaseModel(Base):
    __tablename__ = "databases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)

    @classmethod
    def from_database(cls, database: Database) -> "DatabaseModel":
        return DatabaseModel(id=database.id, name=database.name)

    def to_database(self) -> Database:
        return Database(id=self.id, name=self.name)


class SchemaModel(Base):
    __tablename__ = "schemas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    database_id = Column(Integer, ForeignKey("databases.id"), nullable=False)

    __table_args__ = (UniqueConstraint("database_id", "name"),)

    @classmethod
    def from_schema(cls, schema: Schema) -> "SchemaModel":
        return SchemaModel(
            id=schema.id, name=schema.name, database_id=schema.database_id
        )

    def to_schema(self) -> Schema:
        return Schema(id=self.id, name=self.name, database_id=self.database_id)


class TableModel(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    schema_id = Column(Integer, ForeignKey("schemas.id"), nullable=False)
    database_id = Column(Integer, ForeignKey("databases.id"), nullable=False)

    __table_args__ = (UniqueConstraint("database_id", "schema_id", "name"),)

    @classmethod
    def from_table(cls, table: Table) -> "TableModel":
        return TableModel(
            id=table.id,
            name=table.name,
            schema_id=table.schema_id,
            database_id=table.database_id,
        )

    def to_table(self) -> Table:
        return Table(
            id=self.id,
            name=self.name,
            schema_id=self.schema_id,
            database_id=self.database_id,
            columns=[],
        )


class SqliteMetadata(MetadataStore):
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)

    def get_databases(self) -> list[Database]:
        with Session(self.engine) as session:
            stmt = select(DatabaseModel)
            items = session.execute(stmt).scalars().all()
            return [item.to_database() for item in items]

    def get_database(self, name: str) -> Database | None:
        with Session(self.engine) as session:
            stmt = select(DatabaseModel).where(DatabaseModel.name == name)
            item = session.execute(stmt).scalars().one_or_none()
            if item is None:
                return None
            return item.to_database()

    def create_database(self, database: Database) -> Database:
        with Session(self.engine) as session:
            db = DatabaseModel.from_database(database)
            db.id = None
            session.add(db)
            session.commit()
            return db.to_database()

    def get_schemas(self) -> list[Schema]:
        with Session(self.engine) as session:
            stmt = select(SchemaModel)
            items = session.execute(stmt).scalars().all()
            return [item.to_schema() for item in items]

    def get_schema(self, name: str) -> Schema | None:
        with Session(self.engine) as session:
            stmt = select(SchemaModel).where(SchemaModel.name == name)
            item = session.execute(stmt).scalars().one_or_none()
            if item is None:
                return None
            return item.to_schema()

    def create_schema(self, schema: Schema) -> Schema:
        with Session(self.engine) as session:
            schema = SchemaModel.from_schema(schema)
            schema.id = None
            session.add(schema)
            session.commit()
            return schema.to_schema()

    def get_tables(self) -> list[Table]:
        with Session(self.engine) as session:
            stmt = select(TableModel)
            items = session.execute(stmt).scalars().all()
            return [item.to_table() for item in items]

    def get_table(self, name: str) -> Table | None:
        with Session(self.engine) as session:
            stmt = select(TableModel).where(TableModel.name == name)
            item = session.execute(stmt).scalars().one_or_none()
            if item is None:
                return None
            return item.to_table()

    def create_table(self, table: Table) -> Table:
        with Session(self.engine) as session:
            table = TableModel.from_table(table)
            table.id = None
            session.add(table)
            session.commit()
            return table.to_table()

    def get_stats(
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> list[Statistics]:
        stats = []
        for mp in self.micropartitions(table, s3, version):
            stats.append(mp.statistics())
        return stats

    def get_ops(self, table: Table) -> list[SetOp]:
        with Session(self.engine) as session:
            stmt = (
                select(Operation)
                .where(Operation.table_name == table.name)
                .order_by(Operation.id)
            )
            ops = session.execute(stmt).scalars().all()
            return [op.to_set_op() for op in ops]

    def get_op(self, table: Table, version: int) -> SetOp | None:
        with Session(self.engine) as session:
            stmt = (
                select(Operation)
                .where(Operation.table_name == table.name)
                .where(Operation.version == version)
                .order_by(Operation.id)
            )
            op = session.execute(stmt).scalars().one_or_none()
            if op is None:
                return None
            return op.to_set_op()

    def get_table_version(self, table: Table) -> int:
        with Session(self.engine) as session:
            table_version = session.get(TableVersion, table.name)
            if not table_version:
                table_version = TableVersion(table_name=table.name, version=0)
                session.add(table_version)
                session.commit()
            return int(table_version.version)

    def add_micro_partitions(
        self, table: Table, current_version: int, micro_partitions: list[MicroPartition]
    ):
        with Session(self.engine) as session:
            if self.get_table_version(table) != current_version:
                session.rollback()
                raise ValueError("Version mismatch")

            # Add micro partition metadata
            for mp in micro_partitions:
                metadata = MicroPartitionMetadata(
                    id=mp.id, table_name=table.name, header=mp.header.model_dump()
                )
                session.add(metadata)

            # Add operation
            operation = Operation(
                table_name=table.name,
                version=current_version + 1,
                operation_type="add",
                data=[mp.id for mp in micro_partitions],
            )
            print(f"Adding operation: {operation}")
            session.add(operation)

            # Update table version
            self.bump_table_version(session, table, current_version)

            session.commit()

    def all(self, table: Table, s3: S3Like) -> pl.DataFrame | None:
        """
        Returns a dataframe containing all the data for the table.
        """
        out: pl.DataFrame | None = None
        for metadata in self.micropartitions(table, s3):
            if out is None:
                out = metadata.dump()
            else:
                out = pl.concat([out, metadata.dump()])

        return out

    def bump_table_version(self, session: Session, table: Table, current_version: int):
        table_version = session.get(TableVersion, table.name)
        if table_version is None:
            table_version = TableVersion(table_name=table.name, version=0)
            session.add(table_version)
        elif table_version.version != current_version:
            session.rollback()
            raise ValueError("Version mismatch")

        table_version.version = current_version + 1

    def replace_micro_partitions(
        self,
        table: Table,
        current_version: int,
        replacements: dict[int, MicroPartition],
    ):
        with Session(self.engine) as session:
            if self.get_table_version(table) != current_version:
                session.rollback()
                raise ValueError("Version mismatch")

            # Add new micro partition metadata
            for mp in replacements.values():
                metadata = MicroPartitionMetadata(
                    id=mp.id, table_name=table.name, header=mp.header.model_dump()
                )
                session.add(metadata)

            # Add operation
            operation = Operation(
                table_name=table.name,
                version=current_version + 1,
                operation_type="replace",
                data=[(old_id, mp.id) for old_id, mp in replacements.items()],
            )
            session.add(operation)

            # Update table version
            self.bump_table_version(session, table, current_version)

            session.commit()

    def get_new_micropartition_id(self, table: Table) -> int:
        with Session(self.engine) as session:
            # Get the highest micro partition id for this table
            stmt = (
                select(MicroPartitionMetadata.id)
                .where(MicroPartitionMetadata.table_name == table.name)
                .order_by(MicroPartitionMetadata.id.desc())
                .limit(1)
            )

            result = session.execute(stmt).scalar()
            return 0 if result is None else result + 1

    def _get_ids(self, table: Table, version: int | None = None) -> set[int]:
        ops = self.get_ops(table)
        if version is not None:
            ops = ops[:version]

        return apply(set(), ops)

    def micropartitions(
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> Generator[MicroPartition, None, None]:
        ids = self._get_ids(table, version)

        with Session(self.engine) as session:
            # Get current micro partitions
            stmt = select(MicroPartitionMetadata).where(
                MicroPartitionMetadata.table_name == table.name,
                MicroPartitionMetadata.id.in_(ids),
            )
            micro_partitions = session.execute(stmt).scalars().all()

            # Yield micro partitions
            for mp in micro_partitions:
                micro_partition_raw = s3.get_object("bucket", str(mp.id))
                if micro_partition_raw is None:
                    raise ValueError(f"Micro partition `{mp.id}` not found")

                micro_partition_data = json.loads(micro_partition_raw)
                yield MicroPartition(
                    id=mp.id, header=mp.header, data=micro_partition_data["data"]
                )
