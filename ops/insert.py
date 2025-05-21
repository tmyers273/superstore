import io
from typing import Any

import polars as pl

from classes import Header, MicroPartition, Statistics, Table
from compress import compress
from metadata import MetadataStore
from s3 import S3Like


def insert(
    table: Table, s3: S3Like, metadata_store: MetadataStore, items: pl.DataFrame
):
    if table.sort_keys is not None and len(table.sort_keys) > 0:
        items = items.sort(table.sort_keys)

    if table.partition_keys is not None and len(table.partition_keys) > 0:
        parts: list[tuple[str, io.BytesIO]] = []
        res = items.partition_by(table.partition_keys, as_dict=True, include_key=True)

        for k, df in res.items():
            key = _build_key(k, table)
            unique = df.n_unique(table.partition_keys)
            assert unique == 1, (
                f"key={key} n_unique={unique} partition_keys={table.partition_keys}"
            )
            parts.extend([(key, df) for df in compress(df)])

        _insert_batch(table, s3, metadata_store, parts)
    else:
        _insert_batch(table, s3, metadata_store, compress(items))


def _build_key(key_values: list[Any], table: Table) -> str:
    return "/".join([f"{k}={v}" for k, v in zip(table.partition_keys, key_values)])


def _insert_batch(
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
