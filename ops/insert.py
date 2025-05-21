import io
from enum import Enum
from typing import Any, Protocol

import polars as pl

from classes import Header, MicroPartition, Statistics, Table
from compress import compress
from metadata import MetadataStore
from s3 import S3Like


class InsertStrategyType(Enum):
    UNPARTITIONED = "unpartitioned"
    """
    Unpartitioned. Creates new files for each insert.
    """
    PARTITIONED_NEW_FILES = "partitioned_new_files"
    """
    Partitioned. Creates new files with each insert for each partition key tuple.
    """
    PARTITIONED_APPEND = "partitioned_append"
    """
    Partitioned. Appends to the last file for each partition key tuple.
    """


def insert(
    table: Table, s3: S3Like, metadata_store: MetadataStore, items: pl.DataFrame
):
    # Sort the incoming items by the sort key, if it exists.
    if table.sort_keys is not None and len(table.sort_keys) > 0:
        items = items.sort(table.sort_keys)

    strategy = _insert_strategy(table)
    strategy.insert(table, s3, metadata_store, items)


def _build_key(key_values: list[Any], table: Table) -> str:
    if table.partition_keys is None or len(table.partition_keys) == 0:
        raise ValueError(f"Table `{table.name}` has no partition keys")

    return "/".join([f"{k}={v}" for k, v in zip(table.partition_keys, key_values)])


class InsertStrategy(Protocol):
    def insert(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        items: pl.DataFrame,
    ):
        raise NotImplementedError()

    def _insert_batch(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        parts: list[io.BytesIO] | list[tuple[str, io.BytesIO]],
    ):
        # Get the current table version number
        current_version = metadata_store.get_table_version(table)

        micro_partitions = []
        reserved_ids = metadata_store.reserve_micropartition_ids(table, len(parts))

        for i, raw_part in enumerate(parts):
            if isinstance(raw_part, tuple):
                key_prefix, part = raw_part
                key_prefix += "/"
            else:
                key_prefix = None
                part = raw_part

            id = reserved_ids[i]

            part.seek(0)
            stats = Statistics.from_bytes(part)
            part.seek(0)

            stats.id = id
            micro_partition = MicroPartition(
                id=id,
                header=Header(table_id=table.id),
                data=None,
                stats=stats,
                key_prefix=key_prefix,
            )
            key = f"{key_prefix or ''}{id}"

            if id == 2:
                df = (
                    pl.read_parquet(part)
                    .unique(subset=table.partition_keys)
                    .select(table.partition_keys)
                )
                part.seek(0)

            # Try saving to S3
            part.seek(0)
            s3.put_object("bucket", key, part.getvalue())

            micro_partitions.append(micro_partition)

        # Update metadata
        metadata_store.add_micro_partitions(table, current_version, micro_partitions)


class UnpartitionedInsertStrategy(InsertStrategy):
    def insert(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        items: pl.DataFrame,
    ):
        self._insert_batch(table, s3, metadata_store, compress(items))


class PartitionedNewFilesInsertStrategy(InsertStrategy):
    def insert(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        items: pl.DataFrame,
    ):
        if table.partition_keys is None or len(table.partition_keys) == 0:
            raise ValueError(f"Table `{table.name}` has no partition keys")

        parts: list[tuple[str, io.BytesIO]] = []
        res = items.partition_by(table.partition_keys, as_dict=True, include_key=True)

        for k, df in res.items():
            key = _build_key(k, table)
            unique = df.n_unique(table.partition_keys)
            assert unique == 1, (
                f"key={key} n_unique={unique} partition_keys={table.partition_keys}"
            )
            parts.extend([(key, df) for df in compress(df)])

        self._insert_batch(table, s3, metadata_store, parts)


def _insert_strategy(table: Table) -> InsertStrategy:
    if table.partition_keys is None or len(table.partition_keys) == 0:
        return UnpartitionedInsertStrategy()

    return PartitionedNewFilesInsertStrategy()
