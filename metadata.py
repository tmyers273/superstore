from copy import deepcopy
import json
from typing import Generator, Protocol
from classes import MicroPartition, Table
from s3 import S3Like
import polars as pl

from set_ops import SetOp, SetOpAdd, SetOpReplace, apply


class MetadataStore(Protocol):
    def get_table_version(self, table: Table) -> int:
        """
        Returns the current version of the table, creating it
        and returning 0 if it doesn't exist.
        """
        raise NotImplementedError

    def get_ops(self, table: Table) -> list[SetOp]:
        """
        Returns the list of operations for the table.
        """
        raise NotImplementedError

    def get_op(self, table: Table, version: int) -> SetOp | None:
        """
        Returns the operation for the table at the given version.
        """
        raise NotImplementedError

    def add_micro_partitions(
        self, table: Table, version: int, micro_partitions: list[MicroPartition]
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
        """
        A generator that loops through all the micro partitions for a table.

        Defaults to using the current version if no version is provided.
        """
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
        self.micropartition_ids: dict[str, int] = {}
        """
        Used to generate new micro partition ids.
        Keeps track of the highest id for each table.
        """
        self.ops: dict[str, list[SetOp]] = {}
        """
        Keyed by the table name, then the list of operations.
        """

    def get_ops(self, table: Table) -> list[SetOp]:
        return self.ops[table.name]

    def get_op(self, table: Table, version: int) -> SetOp | None:
        if version in self.ops[table.name]:
            return self.ops[table.name][version]
        else:
            return None

    def get_table_version(self, table: Table) -> int:
        if table.name not in self.table_versions:
            self.table_versions[table.name] = 0

        return self.table_versions[table.name]

    def add_micro_partitions(
        self, table: Table, current_version: int, micro_partitions: list[MicroPartition]
    ):
        if self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        if table.name not in self.current_micro_partitions:
            self.current_micro_partitions[table.name] = []

        for micro_partition in micro_partitions:
            self.raw_micro_partitions[micro_partition.id] = {
                "id": micro_partition.id,
                "header": micro_partition.header,
            }

        self.table_versions[table.name] = current_version + 1

        if self.ops.get(table.name) is None:
            self.ops[table.name] = []
        ids = [p.id for p in micro_partitions]
        self.ops[table.name].append(SetOpAdd(ids))
        self.current_micro_partitions[table.name].extend(ids)

    def get_new_micropartition_id(self, table: Table) -> int:
        if table.name not in self.micropartition_ids:
            self.micropartition_ids[table.name] = 0
            return 0

        self.micropartition_ids[table.name] += 1
        return self.micropartition_ids[table.name]

    def micropartitions(
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> Generator[MicroPartition, None, None]:
        if version is None:
            micropartitions = self.current_micro_partitions[table.name]
            if table.name not in self.current_micro_partitions:
                return
        else:
            if table.name not in self.ops:
                raise ValueError(f"Table {table.name} has no archived versions")
            if len(self.ops[table.name]) < version:
                raise ValueError(f"Version {version} not found")

            micropartitions = list(apply(set(), self.ops[table.name][:version]))
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

        # Construct a list of the replacements
        if self.ops.get(table.name) is None:
            self.ops[table.name] = []

        r = [
            (old_id, micro_partition.id)
            for old_id, micro_partition in replacements.items()
        ]
        self.ops[table.name].append(SetOpReplace(r))
