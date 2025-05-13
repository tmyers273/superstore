from typing import Generator
from sqlalchemy import create_engine, Column, Integer, String, JSON, ForeignKey, select
from sqlalchemy.orm import declarative_base, Session, relationship
from sqlalchemy.ext.declarative import declared_attr
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


class SqliteMetadata(MetadataStore):
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)

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
