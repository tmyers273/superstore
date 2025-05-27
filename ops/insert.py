import io
from concurrent.futures import ThreadPoolExecutor, as_completed
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


async def insert(
    table: Table, s3: S3Like, metadata_store: MetadataStore, items: pl.DataFrame
):
    # Sort the incoming items by the sort key, if it exists.
    if table.sort_keys is not None and len(table.sort_keys) > 0:
        items = items.sort(table.sort_keys)

    strategy = _insert_strategy(table)
    await strategy.insert(table, s3, metadata_store, items)


def _build_key(key_values: list[Any], table: Table) -> str:
    if table.partition_keys is None or len(table.partition_keys) == 0:
        raise ValueError(f"Table `{table.name}` has no partition keys")

    return "/".join([f"{k}={v}" for k, v in zip(table.partition_keys, key_values)])


class InsertStrategy(Protocol):
    async def insert(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        items: pl.DataFrame,
    ):
        raise NotImplementedError()

    async def _insert_batch(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        parts: list[tuple[pl.DataFrame, io.BytesIO]]
        | list[tuple[str, pl.DataFrame, io.BytesIO]],
    ):
        # Get the current table version number
        current_version = await metadata_store.get_table_version(table)

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
        await metadata_store.add_micro_partitions(
            table, current_version, micro_partitions
        )


class UnpartitionedInsertStrategy(InsertStrategy):
    async def insert(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        items: pl.DataFrame,
    ):
        await self._insert_batch(table, s3, metadata_store, compress(items))


class PartitionedNewFilesInsertStrategy(InsertStrategy):
    async def insert(
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

        await self._insert_batch(table, s3, metadata_store, parts)


class PartitionedAppendInsertStrategy(InsertStrategy):
    async def _get_most_recent_mps(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        current_version: int,
    ) -> dict[str, MicroPartition]:
        # recent_start = perf_counter()
        latest_mps: dict[str, MicroPartition] = {}
        async for mp in await metadata_store.micropartitions(
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
        # recent_dur = perf_counter() - recent_start
        # print(f"Time to get most recent MPs: {recent_dur:.2f} seconds")
        return latest_mps

    def _process_partition(
        self,
        k: tuple,
        new_df: pl.DataFrame,
        table: Table,
        latest_mps: dict[str, MicroPartition],
        s3: S3Like,
    ) -> tuple[list[tuple[str, pl.DataFrame, io.BytesIO]], set[int]]:
        """Process a single partition key and return the parts and old MP IDs."""
        key = _build_key(k, table) + "/"
        unique = new_df.n_unique(table.partition_keys)
        assert unique == 1, (
            f"key={key} n_unique={unique} partition_keys={table.partition_keys}"
        )

        old_mp_ids: set[int] = set()
        parts: list[tuple[str, pl.DataFrame, io.BytesIO]] = []

        # Load the current MP, if it exists
        mp = latest_mps.get(key)
        if mp is None:
            if table.sort_keys is not None and len(table.sort_keys) > 0:
                new_df = new_df.sort(table.sort_keys)

            parts.extend([(key, df_part, part) for (df_part, part) in compress(new_df)])
        else:
            old_mp_ids.add(mp.id)
            existing_raw = s3.get_object("bucket", f"{mp.key_prefix or ''}{mp.id}")
            if existing_raw is None:
                raise ValueError(f"Micropartition `{mp.id}` not found")

            existing_df = pl.read_parquet(existing_raw)
            df = existing_df.vstack(new_df)
            if table.sort_keys is not None and len(table.sort_keys) > 0:
                df = df.sort(table.sort_keys)

            parts.extend([(key, df_part, part) for (df_part, part) in compress(df)])

        return parts, old_mp_ids

    def _generate_replacements(
        self,
        table: Table,
        s3: S3Like,
        res: dict,
        latest_mps: dict[str, MicroPartition],
    ) -> tuple[list[tuple[str, pl.DataFrame, io.BytesIO]], set[int]]:
        """Generate replacement parts using parallel processing."""
        # replacement_start = perf_counter()

        # Process partitions in parallel using ThreadPoolExecutor
        parts: list[tuple[str, pl.DataFrame, io.BytesIO]] = []
        old_mp_ids: set[int] = set()

        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all partition processing tasks
            future_to_key = {
                executor.submit(
                    self._process_partition, k, new_df, table, latest_mps, s3
                ): k
                for k, new_df in res.items()
            }

            # Collect results as they complete
            for future in as_completed(future_to_key):
                k = future_to_key[future]
                try:
                    partition_parts, partition_old_mp_ids = future.result()
                    parts.extend(partition_parts)
                    old_mp_ids.update(partition_old_mp_ids)
                except Exception as exc:
                    print(f"Partition {k} generated an exception: {exc}")
                    raise

        # replacement_dur = perf_counter() - replacement_start
        # print(f"Time to get replacements: {replacement_dur:.2f} seconds")

        return parts, old_mp_ids

    def _save_single_part(
        self,
        table: Table,
        s3: S3Like,
        part_data: tuple[int, str, pl.DataFrame, io.BytesIO],
    ) -> MicroPartition:
        """Save a single part to S3 and create a MicroPartition object."""
        id, key_prefix, df_part, part = part_data

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
        return micro_partition

    def _save_parts_to_s3_and_create_micropartitions(
        self,
        table: Table,
        s3: S3Like,
        metadata_store: MetadataStore,
        parts: list[tuple[str, pl.DataFrame, io.BytesIO]],
    ) -> list[MicroPartition]:
        """Save parts to S3 and create MicroPartition objects using parallel processing."""
        reserved_ids = metadata_store.reserve_micropartition_ids(table, len(parts))
        # s3_save_start = perf_counter()

        # Prepare data for parallel processing
        part_data_list = [
            (reserved_ids[i], key_prefix, df_part, part)
            for i, (key_prefix, df_part, part) in enumerate(parts)
        ]

        new_mps: list[MicroPartition] = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all part saving tasks
            future_to_index = {
                executor.submit(self._save_single_part, table, s3, part_data): i
                for i, part_data in enumerate(part_data_list)
            }

            # Collect results as they complete, maintaining order
            results: list[MicroPartition | None] = [None] * len(parts)
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    micro_partition = future.result()
                    results[index] = micro_partition
                except Exception as exc:
                    print(f"Part {index} generated an exception: {exc}")
                    raise

            # Add results in order (filter out None values, though there shouldn't be any)
            new_mps.extend([mp for mp in results if mp is not None])

        # s3_save_dur = perf_counter() - s3_save_start
        # print(f"S3 save time: {s3_save_dur:.2f} seconds")

        return new_mps

    async def insert(
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
        current_version = await metadata_store.get_table_version(table)

        # Get the most recent MPs for each partition key
        latest_mps = await self._get_most_recent_mps(
            table, s3, metadata_store, current_version
        )

        # Generate replacements in parallel
        res = items.partition_by(table.partition_keys, as_dict=True, include_key=True)
        parts, old_mp_ids = self._generate_replacements(table, s3, res, latest_mps)

        # Save parts to S3 and create MicroPartitions
        new_mps = self._save_parts_to_s3_and_create_micropartitions(
            table, s3, metadata_store, parts
        )

        await metadata_store.delete_and_add_micro_partitions(
            table, current_version, list(old_mp_ids), new_mps
        )

        total_dur = perf_counter() - start
        print(f"Inserted {items.height} rows in {total_dur:.2f} seconds")


def _insert_strategy(table: Table) -> InsertStrategy:
    if table.partition_keys is None or len(table.partition_keys) == 0:
        return UnpartitionedInsertStrategy()

    # return PartitionedNewFilesInsertStrategy()
    return PartitionedAppendInsertStrategy()
