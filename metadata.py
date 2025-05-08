from copy import deepcopy
import json
from typing import Generator, Protocol
from classes import MicroPartition, Table
from s3 import S3Like
import polars as pl


class MetadataStore(Protocol):
    def get_table_version(self, table: Table) -> int:
        raise NotImplementedError

    def add_micro_partition(
        self, table: Table, version: int, micro_partition: MicroPartition
    ):
        raise NotImplementedError

    def replace_micro_partitions(
        self,
        table: Table,
        current_version: int,
        replacements: dict[int, MicroPartition],
    ):
        raise NotImplementedError

    def get_new_micropartition_id(self, table: Table) -> int:
        """
        Returns a new, unused micropartition id.
        """
        raise NotImplementedError

    def micropartitions(
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> Generator[MicroPartition, None, None]:
        raise NotImplementedError


class FakeMetadataStore(MetadataStore):
    def __init__(self) -> None:
        self.table_versions: dict[str, int] = {}
        self.raw_micro_partitions: dict[int, dict] = {}
        """
        Keyed by the micro partition id, then the micro partition metadata items.
        """
        self.current_micro_partitions: dict[str, list[int]] = {}
        """
        Keyed by table name, then all the current micro partition ids.
        """
        self.old_versions: dict[str, dict[int, list[int]]] = {}
        """
        Keyed by the table name, then the version number, then all the micro partition ids.
        """
        self.micropartition_ids: dict[str, int] = {}
        """
        Used to generate new micro partition ids.
        Keeps track of the highest id for each table.
        """

    def get_table_version(self, table: Table) -> int:
        if table.name not in self.table_versions:
            self.table_versions[table.name] = 0

        return self.table_versions[table.name]

    def __archive_version(self, table: Table):
        if table.name not in self.old_versions:
            self.old_versions[table.name] = {}

        current_version = self.get_table_version(table)
        if current_version in self.old_versions[table.name]:
            raise ValueError("Version already archived")

        self.old_versions[table.name][current_version] = deepcopy(
            self.current_micro_partitions.get(table.name, [])
        )

    def add_micro_partition(
        self, table: Table, current_version: int, micro_partition: MicroPartition
    ):
        if self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        self.__archive_version(table)

        if table.name not in self.current_micro_partitions:
            self.current_micro_partitions[table.name] = []

        self.raw_micro_partitions[micro_partition.id] = {
            "id": micro_partition.id,
            "header": micro_partition.header,
        }

        self.current_micro_partitions[table.name].append(micro_partition.id)

        self.table_versions[table.name] = current_version + 1

    def get_new_micropartition_id(self, table: Table) -> int:
        if table.name not in self.micropartition_ids:
            self.micropartition_ids[table.name] = 0
            return 0

        self.micropartition_ids[table.name] += 1
        return self.micropartition_ids[table.name]

    def micropartitions(
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> Generator[MicroPartition, None, None]:
        """
        A generator that loops through all the micro partitions for a table.
        """

        if version is None:
            micropartitions = self.current_micro_partitions[table.name]
            if table.name not in self.current_micro_partitions:
                return
        else:
            if table.name not in self.old_versions:
                raise ValueError(f"Table {table.name} has no archived versions")
            if version not in self.old_versions[table.name]:
                raise ValueError(f"Version {version} not found")

            micropartitions = self.old_versions[table.name][version]

        for micro_partition_id in micropartitions:
            metadata = self.raw_micro_partitions[micro_partition_id]
            micro_partition_raw = s3.get_object("bucket", f"{metadata['id']}")
            if micro_partition_raw is None:
                raise ValueError(f"Micro partition `{metadata['id']}` not found")
            micro_partition = json.loads(micro_partition_raw)

            yield MicroPartition(
                id=metadata["id"],
                header=metadata["header"],
                data=micro_partition["data"],
            )

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

    def replace_micro_partitions(
        self,
        table: Table,
        current_version: int,
        replacements: dict[int, MicroPartition],
    ):
        if self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        self.__archive_version(table)

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
            }

        self.table_versions[table.name] = current_version + 1
