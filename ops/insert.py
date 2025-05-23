import io
from enum import Enum
from time import perf_counter
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
        parts: list[tuple[pl.DataFrame, io.BytesIO]]
        | list[tuple[str, pl.DataFrame, io.BytesIO]],
    ):
        # Get the current table version number
        current_version = metadata_store.get_table_version(table)

        micro_partitions = []
        reserved_ids = metadata_store.reserve_micropartition_ids(table, len(parts))

        part: io.BytesIO
        key_prefix: str | None
        df_part: pl.DataFrame
        for i, raw_part in enumerate(parts):
            if isinstance(raw_part, tuple) and len(raw_part) == 3:
                key_prefix, df_part, part = raw_part
                if key_prefix is not None:
                    key_prefix += "/"
            else:
                key_prefix = None
                df_part, part = raw_part

            id = reserved_ids[i]

            parquet_buffer = part.getvalue()
            stats = Statistics.from_df(df_part)
            stats.filesize = len(parquet_buffer)

            stats.id = id
            micro_partition = MicroPartition(
                id=id,
                header=Header(table_id=table.id),
                data=None,
                stats=stats,
                key_prefix=key_prefix,
            )
            key = f"{key_prefix or ''}{id}"

            # Try saving to S3
            s3.put_object("bucket", key, parquet_buffer)

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


class PartitionedAppendInsertStrategy(InsertStrategy):
    def _get_most_recent_mps(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        current_version: int,
    ) -> dict[str, MicroPartition]:
        recent_start = perf_counter()
        latest_mps: dict[str, MicroPartition] = {}
        for mp in metadata_store.micropartitions(
            table, s3, current_version, with_data=False
        ):
            if mp.key_prefix is None:
                raise ValueError(
                    f"Micropartition `{mp.id}` has no key prefix in a partitioned table"
                )

            curr = latest_mps.get(mp.key_prefix)
            if curr is None:
                latest_mps[mp.key_prefix] = mp
            elif curr.id < mp.id:
                latest_mps[mp.key_prefix] = mp
        recent_dur = perf_counter() - recent_start
        print(f"Time to get most recent MPs: {recent_dur:.2f} seconds")
        return latest_mps

    def insert(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        items: pl.DataFrame,
    ):
        start = perf_counter()
        if table.partition_keys is None or len(table.partition_keys) == 0:
            raise ValueError(f"Table `{table.name}` has no partition keys")

        # Get the current table version number
        current_version = metadata_store.get_table_version(table)

        """A list of (prefix, parquet file) tuples"""
        parts: list[tuple[str, pl.DataFrame, io.BytesIO]] = []

        res = items.partition_by(table.partition_keys, as_dict=True, include_key=True)

        # Get the most recent MPs for each partition key
        latest_mps = self._get_most_recent_mps(
            table, s3, metadata_store, current_version
        )

        replacement_start = perf_counter()
        old_mp_ids: set[int] = set()
        for k, new_df in res.items():
            key = _build_key(k, table) + "/"
            unique = new_df.n_unique(table.partition_keys)
            assert unique == 1, (
                f"key={key} n_unique={unique} partition_keys={table.partition_keys}"
            )

            # Load the current MP, if it exists
            mp = latest_mps.get(key)
            if mp is None:
                # print(f"[key={key}] Could not find latest MP for key", key)
                if table.sort_keys is not None and len(table.sort_keys) > 0:
                    new_df = new_df.sort(table.sort_keys)

                parts.extend(
                    [(key, df_part, part) for (df_part, part) in compress(new_df)]
                )
            else:
                old_mp_ids.add(mp.id)
                # print(f"[key={key}] Append to {mp.id} for key_prefix={key}")
                existing_raw = s3.get_object("bucket", f"{mp.key_prefix or ''}{mp.id}")
                if existing_raw is None:
                    raise ValueError(f"Micropartition `{mp.id}` not found")

                existing_df = pl.read_parquet(existing_raw)
                df = existing_df.vstack(new_df)
                if table.sort_keys is not None and len(table.sort_keys) > 0:
                    df = df.sort(table.sort_keys)

                # print(
                #     f"[key={key}] Existing has {existing_df.height} rows, new has {new_df.height}, total has {df.height}"
                # )
                # TODO: if unique keys, maybe filter here?
                parts.extend([(key, df_part, part) for (df_part, part) in compress(df)])
        replacement_dur = perf_counter() - replacement_start
        print(f"Time to get replacements: {replacement_dur:.2f} seconds")

        reserved_ids = metadata_store.reserve_micropartition_ids(table, len(parts))
        new_mps: list[MicroPartition] = []
        s3_save_start = perf_counter()
        for i, (key_prefix, df_part, part) in enumerate(parts):
            id = reserved_ids[i]
            parquet_buffer = part.getvalue()
            stats = Statistics.from_df(df_part)
            stats.filesize = len(parquet_buffer)
            stats.id = id
            micro_partition = MicroPartition(
                id=id,
                header=Header(table_id=table.id),
                data=None,
                stats=stats,
                key_prefix=key_prefix,
            )
            key = f"{key_prefix}{id}"

            s3.put_object("bucket", key, parquet_buffer)
            new_mps.append(micro_partition)

        s3_save_dur = perf_counter() - s3_save_start

        metadata_store.delete_and_add_micro_partitions(
            table, current_version, list(old_mp_ids), new_mps
        )

        total_dur = perf_counter() - start
        print(
            f"Inserted {items.height} rows in {total_dur:.2f} seconds (s3 save: {s3_save_dur:.2f} seconds)"
        )


def _insert_strategy(table: Table) -> InsertStrategy:
    if table.partition_keys is None or len(table.partition_keys) == 0:
        return UnpartitionedInsertStrategy()

    # return PartitionedNewFilesInsertStrategy()
    return PartitionedAppendInsertStrategy()
