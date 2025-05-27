from typing import Generator

import polars as pl
from sqlalchemy import (
    create_engine,
    select,
    text,
    update,
)
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from classes import (
    Database,
    Header,
    MicroPartition,
    Schema,
    Statistics,
    Table,
    TableStatus,
)
from db import (
    Base,
    DatabaseModel,
    MicroPartitionMetadata,
    Operation,
    SchemaModel,
    TableModel,
    TableVersion,
)
from metadata import MetadataStore
from repositories.sqlite_version_repository import SqliteVersionRepository
from repositories.version_repository import VersionRepository
from s3 import S3Like
from set.set_ops import (
    SetOp,
    SetOpAdd,
    SetOpDeleteAndAdd,
    SetOpReplace,
)


class SqliteMetadata(MetadataStore):
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.version_repo: VersionRepository[Session] = SqliteVersionRepository(
            self.engine
        )
        Base.metadata.create_all(self.engine)

    async def get_databases(self) -> list[Database]:
        with Session(self.engine) as session:
            stmt = select(DatabaseModel)
            items = session.execute(stmt).scalars().all()
            return [item.to_database() for item in items]

    async def get_database(self, name: str) -> Database | None:
        with Session(self.engine) as session:
            stmt = select(DatabaseModel).where(DatabaseModel.name == name)
            item = session.execute(stmt).scalars().one_or_none()
            if item is None:
                return None
            return item.to_database()

    async def create_database(self, database: Database) -> Database:
        with Session(self.engine) as session:
            db = DatabaseModel.from_database(database)
            db.id = None
            session.add(db)
            session.commit()
            return db.to_database()

    async def get_schemas(self) -> list[Schema]:
        with Session(self.engine) as session:
            stmt = select(SchemaModel)
            items = session.execute(stmt).scalars().all()
            return [item.to_schema() for item in items]

    async def get_schema(self, name: str) -> Schema | None:
        with Session(self.engine) as session:
            stmt = select(SchemaModel).where(SchemaModel.name == name)
            item = session.execute(stmt).scalars().one_or_none()
            if item is None:
                return None
            return item.to_schema()

    async def create_schema(self, schema: Schema) -> Schema:
        with Session(self.engine) as session:
            schema_model = SchemaModel.from_schema(schema)
            schema_model.id = None
            session.add(schema_model)
            session.commit()
            return schema_model.to_schema()

    async def get_tables(self, include_dropped: bool = False) -> list[Table]:
        with Session(self.engine) as session:
            stmt = select(TableModel)
            if not include_dropped:
                stmt = stmt.where(TableModel.status == TableStatus.ACTIVE.value)
            items = session.execute(stmt).scalars().all()
            return [item.to_table() for item in items]

    async def get_table(self, name: str, include_dropped: bool = False) -> Table | None:
        with Session(self.engine) as session:
            stmt = select(TableModel).where(TableModel.name == name)
            if not include_dropped:
                stmt = stmt.where(TableModel.status == TableStatus.ACTIVE.value)
            item = session.execute(stmt).scalars().one_or_none()
            if item is None:
                return None
            return item.to_table()

    async def get_table_by_id(
        self, table_id: int, include_dropped: bool = False
    ) -> Table | None:
        with Session(self.engine) as session:
            stmt = select(TableModel).where(TableModel.id == table_id)
            if not include_dropped:
                stmt = stmt.where(TableModel.status == TableStatus.ACTIVE.value)
            item = session.execute(stmt).scalars().one_or_none()
            if item is None:
                return None
            return item.to_table()

    async def create_table(self, table: Table) -> Table:
        with Session(self.engine) as session:
            table_model = TableModel.from_table(table)
            table_model.id = None
            session.add(table_model)
            session.commit()
            return table_model.to_table()

    async def drop_table(self, table: Table) -> Table:
        """
        Drop a table by changing its status to 'dropped'.
        This preserves all operations and micropartitions for historical purposes.
        """
        with Session(self.engine) as session:
            stmt = (
                update(TableModel)
                .where(TableModel.id == table.id)
                .values(status=TableStatus.DROPPED.value)
            )
            result = session.execute(stmt)
            if result.rowcount == 0:
                raise ValueError(f"Table with id {table.id} not found")

            session.commit()

            # Return the updated table
            updated_table = table.model_copy()
            updated_table.status = TableStatus.DROPPED
            return updated_table

    def get_stats(
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> list[Statistics]:
        stats = []
        for mp in self.micropartitions(table, s3, version, with_data=False):
            stats.append(mp.stats)
        return stats

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
            version = self._get_table_version(session, table)
            session.commit()
            return version

    def _get_table_version(self, session: Session, table: Table) -> int:
        table_version = session.get(TableVersion, table.name)
        if not table_version:
            table_version = TableVersion(table_name=table.name, version=0)
            session.add(table_version)
        return int(table_version.version)

    def add_micro_partitions(
        self, table: Table, current_version: int, micro_partitions: list[MicroPartition]
    ):
        with Session(self.engine) as session:
            session.execute(text("BEGIN IMMEDIATE"))

            if self._get_table_version(session, table) != current_version:
                raise ValueError("Version mismatch")

            # Add micro partition metadata
            session.bulk_save_objects(
                [
                    MicroPartitionMetadata(
                        id=mp.id,
                        table_name=table.name,
                        stats=mp.stats.model_dump(),
                        key_prefix=mp.key_prefix,
                    )
                    for mp in micro_partitions
                ]
            )

            # Add operation
            op = SetOpAdd([mp.id for mp in micro_partitions])
            print(f"Adding operation: at version {current_version + 1}: {op}")
            self.version_repo.add(table, current_version + 1, op, session)

            # Update table version
            self._bump_table_version(session, table, current_version)

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

    def _bump_table_version(self, session: Session, table: Table, current_version: int):
        tbl = table.name

        # insert row if absent
        session.execute(
            insert(TableVersion)
            .values(table_name=tbl, version=current_version)
            .on_conflict_do_nothing(index_elements=["table_name"])  # SQLite ≥3.24
        )

        # atomic optimistic bump
        rows = session.execute(
            update(TableVersion)
            .where(
                TableVersion.table_name == tbl, TableVersion.version == current_version
            )
            .values(version=TableVersion.version + 1)
        ).rowcount

        if rows == 0:
            raise ValueError("Version mismatch")

    def replace_micro_partitions(
        self,
        table: Table,
        current_version: int,
        replacements: dict[int, MicroPartition],
    ):
        with Session(self.engine) as session:
            session.execute(text("BEGIN IMMEDIATE"))

            if self._get_table_version(session, table) != current_version:
                raise ValueError("Version mismatch")

            # Add new micro partition metadata
            session.bulk_save_objects(
                [
                    MicroPartitionMetadata(
                        id=mp.id,
                        table_name=table.name,
                        stats=mp.stats.model_dump(),
                        key_prefix=mp.key_prefix,
                    )
                    for mp in replacements.values()
                ]
            )

            # Add operation using version repository
            op = SetOpReplace([(old_id, mp.id) for old_id, mp in replacements.items()])
            self.version_repo.add(table, current_version + 1, op, session)

            # Update table version
            self._bump_table_version(session, table, current_version)

            session.commit()

    def reserve_micropartition_ids(self, table: Table, number: int) -> list[int]:
        seq_name = MicroPartitionMetadata.__tablename__  # "micro_partitions"

        if number == 0:
            return []

        with Session(self.engine) as session:
            session.execute(text("BEGIN IMMEDIATE"))

            # Try to bump an existing row and fetch the new max
            res = session.execute(
                text("""
                UPDATE sqlite_sequence
                SET seq = seq + :n
                WHERE name = :tbl
                RETURNING seq
            """),
                {"n": number, "tbl": seq_name},
            )

            new_max = res.scalar_one_or_none()

            # Row wasn't there → create it with the desired value
            if new_max is None:
                new_max = number
                session.execute(
                    text("""
                    INSERT INTO sqlite_sequence(name, seq)
                    VALUES (:tbl, :seq)
                """),
                    {"tbl": seq_name, "seq": new_max},
                )

            session.commit()

        start = new_max - number + 1
        return list(range(start, new_max + 1))

    def _get_ids(self, table: Table, version: int | None = None) -> set[int]:
        if version is None:
            version = self.get_table_version(table)

        with Session(self.engine) as session:
            return self.version_repo.get_hams(table, version, session)

    def chunked(self, items: list, chunk_size: int) -> Generator[list, None, None]:
        for i in range(0, len(items), chunk_size):
            yield items[i : i + chunk_size]

    def micropartitions(
        self,
        table: Table,
        s3: S3Like,
        version: int | None = None,
        with_data: bool = True,
        prefix: str | None = None,
    ) -> Generator[MicroPartition, None, None]:
        if prefix is not None and not prefix.endswith("/"):
            prefix = f"{prefix}/"

        ids = self._get_ids(table, version)
        with Session(self.engine) as session:
            micro_partitions: list[MicroPartitionMetadata] = []
            # print(f"Loading {len(ids)} micro partitions")

            for id_chunk in self.chunked(list(ids), 1000):
                q = select(MicroPartitionMetadata).where(
                    MicroPartitionMetadata.id.in_(id_chunk)
                )
                if prefix is not None:
                    q = q.where(MicroPartitionMetadata.key_prefix == prefix)
                chunk_mps = session.execute(q).scalars().all()
                micro_partitions.extend(chunk_mps)
                # print(f"Loaded {len(micro_partitions)}/{len(ids)} micro partitions")

            # print(f"Loaded {len(micro_partitions)} micro partitions")

            # Yield micro partitions
            for mp in micro_partitions:
                micro_partition_raw = None
                if with_data:
                    prefix = mp.key_prefix or ""
                    key = f"{prefix}{mp.id}"
                    micro_partition_raw = s3.get_object("bucket", key)
                    if micro_partition_raw is None:
                        raise ValueError(f"Micro partition `{key}` not found")

                yield MicroPartition(
                    id=mp.id,
                    data=micro_partition_raw,
                    stats=Statistics.model_validate(mp.stats),
                    header=Header(table_id=table.id),
                    key_prefix=mp.key_prefix,
                )

    def delete_and_add_micro_partitions(
        self,
        table: Table,
        current_version: int,
        delete_ids: list[int],
        new_mps: list[MicroPartition],
    ):
        with Session(self.engine) as session:
            session.execute(text("BEGIN IMMEDIATE"))

            if self._get_table_version(session, table) != current_version:
                raise ValueError("Version mismatch")

            # Add new micro partition metadata
            session.bulk_save_objects(
                [
                    MicroPartitionMetadata(
                        id=mp.id,
                        table_name=table.name,
                        stats=mp.stats.model_dump(),
                        key_prefix=mp.key_prefix,
                    )
                    for mp in new_mps
                ]
            )

            # Add operation using version repository
            op = SetOpDeleteAndAdd((delete_ids, [mp.id for mp in new_mps]))
            self.version_repo.add(table, current_version + 1, op, session)

            # Update table version
            self._bump_table_version(session, table, current_version)

            session.commit()
