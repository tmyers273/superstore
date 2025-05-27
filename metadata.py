from typing import AsyncGenerator, Protocol

import polars as pl

from classes import Database, MicroPartition, Schema, Table, TableStatus
from repositories.fake_version_repository import FakeVersionRepository
from repositories.version_repository import VersionRepository
from s3 import S3Like
from set.set_ops import SetOp, SetOpAdd, SetOpDeleteAndAdd, SetOpReplace, apply


class MetadataStore(Protocol):
    async def create_database(self, database: Database) -> Database:
        """
        Create a new database.
        """
        raise NotImplementedError

    async def get_databases(self) -> list[Database]:
        """
        Returns a list of all the databases.
        """
        raise NotImplementedError

    async def get_database(self, name: str) -> Database | None:
        """
        Returns the database with the given name.
        """
        raise NotImplementedError

    async def create_schema(self, schema: Schema) -> Schema:
        """
        Create a new schema.
        """
        raise NotImplementedError

    async def get_schemas(self) -> list[Schema]:
        """
        Returns a list of all the schemas.
        """
        raise NotImplementedError

    async def get_schema(self, name: str) -> Schema | None:
        """
        Returns the schema with the given name.
        """
        raise NotImplementedError

    async def create_table(self, table: Table) -> Table:
        """
        Create a new table.
        """
        raise NotImplementedError

    async def get_tables(self, include_dropped: bool = False) -> list[Table]:
        """
        Returns a list of all the tables.
        """
        raise NotImplementedError

    async def get_table(self, name: str, include_dropped: bool = False) -> Table | None:
        """
        Returns the table with the given name.
        """
        raise NotImplementedError

    async def get_table_by_id(
        self, table_id: int, include_dropped: bool = False
    ) -> Table | None:
        """
        Returns the table with the given ID.
        """
        raise NotImplementedError

    async def drop_table(self, table: Table) -> Table:
        """
        Drop a table by changing its status to 'dropped'.
        This preserves all operations and micropartitions for historical purposes.
        """
        raise NotImplementedError

    async def get_op(self, table: Table, version: int) -> SetOp | None:
        """
        Returns the operation for the table at the given version.
        """
        raise NotImplementedError

    async def get_table_version(self, table: Table) -> int:
        """
        Returns the version of the table.
        """
        raise NotImplementedError

    async def add_micro_partitions(
        self, table: Table, version: int, micro_partitions: list[MicroPartition]
    ):
        raise NotImplementedError

    async def delete_and_add_micro_partitions(
        self,
        table: Table,
        current_version: int,
        delete_ids: list[int],
        new_mps: list[MicroPartition],
    ):
        raise NotImplementedError

    async def replace_micro_partitions(
        self,
        table: Table,
        current_version: int,
        replacements: dict[int, MicroPartition],
    ):
        """
        Replace the micro partitions in the table with the given replacements.

        This is intended for more point-wise updates and deletes.

        Replacements is a dict keyed by the old MP id, then the new MP.
        """

        raise NotImplementedError

    def reserve_micropartition_ids(self, table: Table, number: int) -> list[int]:
        """
        Reserve a list of micropartition ids for a table.
        """
        raise NotImplementedError

    async def micropartitions(
        self,
        table: Table,
        s3: S3Like,
        version: int | None = None,
        with_data: bool = True,
    ) -> AsyncGenerator[MicroPartition, None, None]:
        """
        A generator that loops through all the micro partitions for a table.

        Defaults to using the current version if no version is provided.
        """
        raise NotImplementedError

    async def _get_ids(self, table: Table, version: int | None = None) -> set[int]:
        """
        Returns the set of micropartition IDs for a table at a given version.
        """
        raise NotImplementedError

    async def all(self, table: Table, s3: S3Like) -> pl.DataFrame | None:
        """
        Returns a dataframe containing all the data for the table.
        """
        raise NotImplementedError

    async def micropartition_count(
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> int:
        """
        Returns the number of micropartitions for a table at a given version.
        """
        cnt = 0
        async for _ in await self.micropartitions(table, s3, version):
            cnt += 1
        return cnt


class FakeMetadataStore(MetadataStore):
    def __init__(self, version_repo: VersionRepository | None = None) -> None:
        if version_repo is None:
            version_repo = FakeVersionRepository()
        self.version_repo = version_repo
        self.table_versions: dict[str, int] = {}
        self.raw_micro_partitions: dict[int, dict] = {}
        """
        Keyed by the micro partition id, then the micro partition metadata items.
        """
        self.current_micro_partitions: dict[str, list[int]] = {}
        """
        Keyed by table name, then all the current micro partition ids.
        """
        self.global_micropartition_id_counter: int = 0
        """
        Used to generate new micro partition ids globally across all tables.
        """
        self.ops: dict[str, list[SetOp]] = {}
        """
        Keyed by the table name, then the list of operations.
        """
        self.databases: dict[str, Database] = {}
        """
        Keyed by the database name, then the database.
        """
        self.schemas: dict[str, Schema] = {}
        """
        Keyed by the schema name, then the schema.
        """
        self.tables: dict[str, Table] = {}
        """
        Keyed by the table name, then the table.
        """

    async def create_database(self, database: Database) -> Database:
        database.id = len(self.databases) + 1
        self.databases[database.name] = database
        return database

    async def get_databases(self) -> list[Database]:
        return list(self.databases.values())

    async def get_database(self, name: str) -> Database | None:
        return self.databases.get(name)

    async def create_schema(self, schema: Schema) -> Schema:
        schema.id = len(self.schemas) + 1
        self.schemas[schema.name] = schema
        return schema

    async def get_schemas(self) -> list[Schema]:
        return list(self.schemas.values())

    async def get_schema(self, name: str) -> Schema | None:
        return self.schemas.get(name)

    async def create_table(self, table: Table) -> Table:
        table.id = len(self.tables) + 1
        self.tables[table.name] = table
        return table

    async def get_tables(self, include_dropped: bool = False) -> list[Table]:
        tables = list(self.tables.values())
        if not include_dropped:
            tables = [t for t in tables if t.status == TableStatus.ACTIVE]
        return tables

    async def get_table(self, name: str, include_dropped: bool = False) -> Table | None:
        table = self.tables.get(name)
        if table is None:
            return None
        if not include_dropped and table.status != TableStatus.ACTIVE:
            return None
        return table

    async def get_table_by_id(
        self, table_id: int, include_dropped: bool = False
    ) -> Table | None:
        for table in self.tables.values():
            if table.id == table_id:
                if include_dropped or table.status == TableStatus.ACTIVE:
                    return table
        return None

    async def drop_table(self, table: Table) -> Table:
        """
        Drop a table by changing its status to 'dropped'.
        This preserves all operations and micropartitions for historical purposes.
        """
        if table.name not in self.tables:
            raise ValueError(f"Table {table.name} not found")

        updated_table = table.model_copy()
        updated_table.status = TableStatus.DROPPED
        self.tables[table.name] = updated_table
        return updated_table

    async def get_op(self, table: Table, version: int) -> SetOp | None:
        if table.name not in self.ops:
            return None

        if (version - 1) >= len(self.ops[table.name]):
            return None

        return self.ops[table.name][version - 1]

    async def get_table_version(self, table: Table) -> int:
        if table.name not in self.table_versions:
            self.table_versions[table.name] = 0

        return self.table_versions[table.name]

    async def add_micro_partitions(
        self, table: Table, current_version: int, micro_partitions: list[MicroPartition]
    ):
        if await self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        if table.name not in self.current_micro_partitions:
            self.current_micro_partitions[table.name] = []

        for micro_partition in micro_partitions:
            self.raw_micro_partitions[micro_partition.id] = {
                "id": micro_partition.id,
                "header": micro_partition.header,
                "stats": micro_partition.stats,
                "key_prefix": micro_partition.key_prefix,
            }

        self.table_versions[table.name] = current_version + 1

        if self.ops.get(table.name) is None:
            self.ops[table.name] = []
        ids = [p.id for p in micro_partitions]
        self.ops[table.name].append(SetOpAdd(ids))
        self.current_micro_partitions[table.name].extend(ids)

    def reserve_micropartition_ids(self, table: Table, number: int) -> list[int]:
        start = self.global_micropartition_id_counter + 1
        end = start + number
        self.global_micropartition_id_counter = end - 1
        return list(range(start, end))

    async def micropartitions(
        self,
        table: Table,
        s3: S3Like,
        version: int | None = None,
        with_data: bool = True,
    ) -> AsyncGenerator[MicroPartition, None, None]:
        if version is None:
            if table.name not in self.current_micro_partitions:
                raise ValueError(f"Table {table.name} has no micro partitions")

            micropartitions = self.current_micro_partitions[table.name]
        else:
            if table.name not in self.ops:
                return
            if len(self.ops[table.name]) < version:
                raise ValueError(f"Version {version} not found")

            micropartitions = list(apply(set(), self.ops[table.name][:version]))

        for micro_partition_id in micropartitions:
            metadata = self.raw_micro_partitions[micro_partition_id]

            micro_partition_raw = None
            prefix = metadata.get("key_prefix", None) or ""
            if with_data == True:
                key = f"{prefix}{metadata['id']}"
                micro_partition_raw = s3.get_object("bucket", key)
                if micro_partition_raw is None:
                    raise ValueError(f"Micro partition `{key}` not found")

            yield MicroPartition(
                id=metadata["id"],
                header=metadata["header"],
                data=micro_partition_raw,
                stats=metadata["stats"],
                key_prefix=prefix,
            )

    async def _get_ids(self, table: Table, version: int | None = None) -> set[int]:
        if version is None:
            return set(self.current_micro_partitions[table.name])
        else:
            if table.name not in self.ops:
                raise ValueError(f"Table {table.name} has no archived versions")
            if len(self.ops[table.name]) < version:
                raise ValueError(f"Version {version} not found")

            return set(apply(set(), self.ops[table.name][:version]))

    async def all(self, table: Table, s3: S3Like) -> pl.DataFrame | None:
        """
        Returns a dataframe containing all the data for the table.
        """
        out: pl.DataFrame | None = None
        async for metadata in await self.micropartitions(table, s3):
            if out is None:
                out = metadata.dump()
            else:
                out = pl.concat([out, metadata.dump()])

        return out

    async def replace_micro_partitions(
        self,
        table: Table,
        current_version: int,
        replacements: dict[int, MicroPartition],
    ):
        if await self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        if table.name not in self.current_micro_partitions:
            raise ValueError(f"Table {table.name} has no micro partitions")

        # Make sure all the ids exist
        current_ids = {p for p in self.current_micro_partitions[table.name]}
        for old_id in replacements.keys():
            if old_id not in current_ids:
                raise ValueError(f"Micro partition {old_id} not found: {current_ids}")

        # Replace the micro partitions in metadata
        index = {v: i for i, v in enumerate(self.current_micro_partitions[table.name])}
        for old_id, micro_partition in replacements.items():
            self.current_micro_partitions[table.name][index[old_id]] = (
                micro_partition.id
            )
            self.raw_micro_partitions[micro_partition.id] = {
                "id": micro_partition.id,
                "header": micro_partition.header,
                "stats": micro_partition.stats,
                "key_prefix": micro_partition.key_prefix,
            }

        self.table_versions[table.name] = current_version + 1

        # Construct a list of the replacements
        if self.ops.get(table.name) is None:
            self.ops[table.name] = []

        r = [
            (old_id, micro_partition.id)
            for old_id, micro_partition in replacements.items()
        ]
        self.ops[table.name].append(SetOpReplace(r))

    async def delete_and_add_micro_partitions(
        self,
        table: Table,
        current_version: int,
        delete_ids: list[int],
        new_mps: list[MicroPartition],
    ):
        if await self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        if table.name not in self.current_micro_partitions:
            self.current_micro_partitions[table.name] = []

        # Make sure all the ids exist
        current_ids = {p for p in self.current_micro_partitions[table.name]}
        for old_id in delete_ids:
            if old_id not in current_ids:
                raise ValueError(f"Micro partition {old_id} not found: {current_ids}")

        # Replace the micro partitions in metadata
        index = {v: i for i, v in enumerate(self.current_micro_partitions[table.name])}
        delete_indexes = [index[old_id] for old_id in delete_ids]
        delete_indexes.sort(reverse=True)
        for delete_index in delete_indexes:
            del self.current_micro_partitions[table.name][delete_index]

        for new_mp in new_mps:
            self.current_micro_partitions[table.name].append(new_mp.id)
            self.raw_micro_partitions[new_mp.id] = {
                "id": new_mp.id,
                "header": new_mp.header,
                "stats": new_mp.stats,
                "key_prefix": new_mp.key_prefix,
            }

        self.table_versions[table.name] = current_version + 1

        new_mp_ids = [p.id for p in new_mps]

        if table.name not in self.ops:
            self.ops[table.name] = []
        self.ops[table.name].append(SetOpDeleteAndAdd((delete_ids, new_mp_ids)))
