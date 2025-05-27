import io
import os
import random
import tempfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from time import perf_counter

import polars as pl
import pytest
from datafusion import SessionContext

from classes import (
    ColumnDefinitions,
    Database,
    Header,
    MicroPartition,
    Schema,
    Statistics,
    Table,
)
from compress import compress
from local_s3 import LocalS3
from metadata import FakeMetadataStore, MetadataStore
from ops.insert import insert
from s3 import FakeS3, S3Like
from set.set_ops import SetOpAdd, SetOpDeleteAndAdd, SetOpReplace
from sqlite_metadata import SqliteMetadata
from sweep import find_ids_with_most_overlap


class Metadata:
    pass


async def delete(
    table: Table, s3: S3Like, metadata_store: MetadataStore, pks: list[int]
):
    if len(pks) == 0:
        return

    # TODO: validate schema
    # TODO: error if pk is not found?

    # Get the current table version number
    current_version = await metadata_store.get_table_version(table)

    # Load the parquet file
    new_mps: list[tuple[str | None, io.BytesIO]] = []
    old_mp_ids = []

    remaining_pks = set(pks)
    min_pk_id = min(remaining_pks)
    max_pk_id = max(remaining_pks)

    async for p in await metadata_store.micropartitions(
        table, s3, version=current_version, with_data=False
    ):
        if len(remaining_pks) == 0:
            break

        stats = p.stats.get("id")
        if stats is None:
            raise ValueError(f"Micro partition {p.id} has no id column statistics")

        if not (stats.min <= max_pk_id and stats.max >= min_pk_id):
            continue

        key = f"{p.key_prefix or ''}{p.id}"
        raw = s3.get_object("bucket", key)
        if raw is None:
            raise ValueError(f"Micro partition {p.id} not found")
        part = io.BytesIO(raw)
        df = pl.read_parquet(part)

        # df = p.dump()
        before_cnt = len(df)
        df_filtered = df.filter(~pl.col("id").is_in(pks))
        after_cnt = len(df_filtered)

        # Skip if no rows were deleted from this micro partition
        if after_cnt == before_cnt:
            continue

        # Remove the IDs we found from remaining_pks
        found_ids = set(df.filter(pl.col("id").is_in(pks))["id"].to_list())
        remaining_pks -= found_ids

        # Recalculate min and max if we still have remaining IDs
        if remaining_pks:
            min_pk_id = min(remaining_pks)
            max_pk_id = max(remaining_pks)

        old_mp_ids.append(p.id)
        if after_cnt == 0:
            continue

        buffer = io.BytesIO()
        df_filtered.write_parquet(buffer)
        new_mps.append((p.key_prefix, buffer))

    replacements: list[MicroPartition] = []
    reserved_ids = await metadata_store.reserve_micropartition_ids(table, len(new_mps))
    for i, (key_prefix, buffer) in enumerate(new_mps):
        # Create a new micro partition
        id = reserved_ids[i]
        stats = Statistics.from_bytes(buffer)
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
        s3.put_object("bucket", key, buffer.getvalue())

        replacements.append(micro_partition)

    # Update metadata
    await metadata_store.delete_and_add_micro_partitions(
        table, current_version, old_mp_ids, replacements
    )


async def delete_and_add(
    table: Table,
    s3: S3Like,
    metadata_store: MetadataStore,
    delete_ids: list[int],
    new_df: pl.DataFrame,
    key_prefix: str | None = None,
):
    """
    Deletes a number of micropartitions and adds a new set of micropartitions.
    This is generally used for clustering.

    Args:
    - delete_ids is a list of the micropartition ids to delete.
    """
    if table.partition_keys is not None and key_prefix is None:
        raise ValueError("key_prefix is required if table has a partition key")

    if table.partition_keys is None and key_prefix is not None:
        raise ValueError("key_prefix is not allowed if table has no partition key")

    current_version = await metadata_store.get_table_version(table)

    mps = []

    # print("In delete and add", new_df)
    dfs = compress(new_df)
    reserved_ids = await metadata_store.reserve_micropartition_ids(table, len(dfs))
    for i, (df_part, buffer) in enumerate(dfs):
        id = reserved_ids[i]

        # Create a new micro partition
        parquet_buffer = buffer.getvalue()
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
        mps.append(micro_partition)

        # Try saving to S3
        key = f"{key_prefix or ''}{id}"
        s3.put_object("bucket", key, parquet_buffer)

    await metadata_store.delete_and_add_micro_partitions(
        table, current_version, delete_ids, mps
    )


async def update(
    table: Table, s3: S3Like, metadata_store: MetadataStore, items: pl.DataFrame
):
    # print(f"[mutation] Updating {len(items)} rows in {table.name}")
    # TODO: validate schema
    # TODO: error if pk is not found?

    # Get the current table version number
    current_version = await metadata_store.get_table_version(table)

    # Load the parquet file
    replacements: dict[int, MicroPartition] = {}
    # TODO: this reserves far too many ids
    reserved_ids = await metadata_store.reserve_micropartition_ids(table, len(items))
    i = 0
    async for p in await metadata_store.micropartitions(
        table, s3, version=current_version
    ):
        df = p.dump()
        updated_items = items.filter(pl.col("id").is_in(df["id"]))

        # TODO: only flag this for an update if something actually changed

        # Skip if no rows were changed from this micro partition
        if len(updated_items) == 0:
            continue

        # Merge the new items
        df = df.vstack(updated_items).unique(subset=["id"], keep="last")

        if table.sort_keys is not None and len(table.sort_keys) > 0:
            df = df.sort(table.sort_keys)

        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)

        # Create a new micro partition
        id = reserved_ids[i]
        stats = Statistics.from_bytes(buffer)
        stats.id = id
        micro_partition = MicroPartition(
            id=id,
            header=Header(table_id=table.id),
            data=buffer.getvalue(),
            stats=stats,
            key_prefix=p.key_prefix,
        )
        key = f"{p.key_prefix or ''}{id}"

        # Try saving to S3
        s3.put_object("bucket", key, buffer.getvalue())

        replacements[p.id] = micro_partition
        i += 1

    # Update metadata
    await metadata_store.replace_micro_partitions(table, current_version, replacements)


async def _cluster_with_partitions(
    metadata: MetadataStore, s3: S3Like, table: Table
) -> None:
    version = await metadata.get_table_version(table)

    # print("Clustering with partitions")
    if table.sort_keys is None or len(table.sort_keys) == 0:
        raise ValueError("Table has no sort keys")

    stats: dict[str, dict[int, Statistics]] = {}
    mps: dict[str, dict[int, MicroPartition]] = {}

    async for mp in await metadata.micropartitions(
        table, s3, with_data=False, version=version
    ):
        prefix = mp.key_prefix or ""
        if prefix not in stats:
            stats[prefix] = {}
        if prefix not in mps:
            mps[prefix] = {}
        stats[prefix][mp.id] = mp.stats
        mps[prefix][mp.id] = mp

    # First, try to merge small MPs. Tables with partition keys will be more
    # like to have lots of small MPs than other tables.
    SMALL_CUTOFF = 8 * 1024 * 1024  # 8mb
    overlap_lists: list[tuple[str, int, list[Statistics]]] = []
    # max = 0
    # max_prefix = ""
    for prefix, stats_per_key in stats.items():
        tmp_overlaps: list[Statistics] = []
        tmp_total = 0
        for stat in stats_per_key.values():
            if stat.filesize < SMALL_CUTOFF:
                tmp_overlaps.append(stat)
                tmp_total += stat.filesize

        overlap_lists.append((prefix, tmp_total, tmp_overlaps))
    overlap_lists = sorted(overlap_lists, key=lambda x: x[1], reverse=True)

    cnt = 0
    for prefix, total, overlaps in overlap_lists:
        # print(
        #     f"Found {len(overlaps)} overlaps for prefix {prefix} with total size {total}"
        # )

        # If we didn't get one by merging small MPs
        # if max == 0:
        #     col_index = 0
        #     for stats_per_key in stats.values():
        #         for stat in stats_per_key.values():
        #             for col in stat.columns:
        #                 if col.name == table.sort_keys[0]:
        #                     col_index = col.index
        #                 break

        #     # print("Preparing sweep")
        #     to_sweep: dict[str, list[tuple[str, str, int]]] = {}
        #     for prefix, stats_per_key in stats.items():
        #         for stat in stats_per_key.values():
        #             col = stat.columns[col_index]
        #             if prefix not in to_sweep:
        #                 to_sweep[prefix] = []
        #             to_sweep[prefix].append((col.min, col.max, stat.id))

        #     # print(f"Finding overlap of {len(to_sweep)} MPs")
        #     overlaps: list[Statistics] = []
        #     max = 0
        #     max_prefix = ""

        #     for prefix, to_sweep_list in to_sweep.items():
        #         overlap_set: set[int] = find_ids_with_most_overlap(to_sweep_list)

        #         if len(overlap_set) > max:
        #             max = len(overlap_set)
        #             overlaps = [stats[prefix][id] for id in overlap_set]
        #             overlaps = sorted(overlaps, key=lambda x: x.id)
        #             max_prefix = prefix

        ###

        # if len(overlaps) <= 1:
        #     print("No overlaps found, trying to combine small MPs")

        #     # Try to combine small MPs
        #     SMALL_CUTOFF = 8 * 1024 * 1024  # 8mb
        #     for stat in stats.values():
        #         if stat.filesize < SMALL_CUTOFF:
        #             overlaps.append(stat)
        #     overlaps = sorted(overlaps, key=lambda x: x.id)

        if len(overlaps) <= 1:
            # print("No overlaps found, exiting")
            return

        # Cap the total filesize of the parquet files
        # We expect the ram usage of the df to be some
        # multiple (2-5x?) of the parquet filesize.
        #
        # If we want to cap the total ram usage to 512mb,
        # then we can cap the total parquet filesize to 512mb / 5 = ~100mb
        # or 512mb / 2 = 256mb
        max_filesize = 128 * 1024 * 1024  # 128mb
        total_size = 0
        early_break = False
        for i, overlap in enumerate(overlaps):
            total_size += overlap.filesize
            if total_size > max_filesize:
                early_break = True
                break
        if not early_break:
            i = len(overlaps)

        # print(f"Found a total of {len(overlaps)} overlapping MPs")
        overlaps = overlaps[:i]

        # print(f"Found {len(overlaps)} overlapping MPs")

        # Load each and vstack info a single df
        df: pl.DataFrame | None = None
        rows = 0
        # print("Overlaps", overlaps, len(overlaps))
        for overlap in overlaps:
            mp: MicroPartition = mps[prefix][overlap.id]

            key = f"{mp.key_prefix or ''}{mp.id}"
            raw = s3.get_object("bucket", key)
            if raw is None:
                raise Exception(f"Micropartition {overlap.id} not found")

            buffer = io.BytesIO(raw)
            new_df = pl.read_parquet(buffer)

            rows += new_df.height
            if df is None:
                df = new_df
            else:
                df = df.vstack(new_df)
        if df is None:
            raise Exception("No data found")

        # print(f"Loaded a total of {rows} rows. New df has {df.height} rows")
        if df.height != rows:
            raise Exception("Rows mismatch")

        df = df.sort(table.sort_keys)
        delete_ids = [overlap.id for overlap in overlaps]
        await delete_and_add(table, s3, metadata, delete_ids, df, key_prefix=prefix)

        cnt += 1
        if cnt > 500:
            break


async def cluster(metadata: MetadataStore, s3: S3Like, table: Table) -> None:
    if table.sort_keys is None or len(table.sort_keys) == 0:
        raise ValueError("Table has no sort keys")
    if table.partition_keys is not None and len(table.partition_keys) > 0:
        return await _cluster_with_partitions(metadata, s3, table)

    stats: dict[int, Statistics] = {}
    mps: dict[int, MicroPartition] = {}
    # print("Loading stats")
    async for mp in await metadata.micropartitions(table, s3, with_data=False):
        stats[mp.id] = mp.stats
        mps[mp.id] = mp

    index = 0
    for stat in stats.values():
        for col in stat.columns:
            if col.name == table.sort_keys[0]:
                index = col.index
                break

    # print("Preparing sweep")
    to_sweep: list[tuple[str, str, int]] = []
    for stat in stats.values():
        col = stat.columns[index]
        to_sweep.append((col.min, col.max, stat.id))

    # print(f"Finding overlap of {len(to_sweep)} MPs")
    overlap_set: set[int] = find_ids_with_most_overlap(to_sweep)
    overlaps: list[Statistics] = [stats[id] for id in overlap_set]
    overlaps: list[Statistics] = sorted(overlaps, key=lambda x: x.id)

    if len(overlaps) <= 1:
        # print("No overlaps found, trying to combine small MPs")

        # Try to combine small MPs
        SMALL_CUTOFF = 8 * 1024 * 1024  # 8mb
        for stat in stats.values():
            if stat.filesize < SMALL_CUTOFF:
                overlaps.append(stat)
        overlaps = sorted(overlaps, key=lambda x: x.id)

    if len(overlaps) <= 0:
        # print("No overlaps found, exiting")
        return

    # Cap the total filesize of the parquet files
    # We expect the ram usage of the df to be some
    # multiple (2-5x?) of the parquet filesize.
    #
    # If we want to cap the total ram usage to 512mb,
    # then we can cap the total parquet filesize to 512mb / 5 = ~100mb
    # or 512mb / 2 = 256mb
    max_filesize = 128 * 1024 * 1024  # 128mb
    total_size = 0
    for i, overlap in enumerate(overlaps):
        total_size += overlap.filesize
        if total_size > max_filesize:
            break

    # print(f"Found a total of {len(overlaps)} overlapping MPs")
    overlaps = overlaps[:i]

    # print(f"Found {len(overlaps)} overlapping MPs")

    # Load each and vstack info a single df
    df: pl.DataFrame | None = None
    rows = 0
    for overlap in overlaps:
        mp: MicroPartition = mps[overlap.id]

        key = f"{mp.key_prefix or ''}{mp.id}"
        raw = s3.get_object("bucket", key)
        if raw is None:
            raise Exception(f"Micropartition {overlap.id} not found")

        buffer = io.BytesIO(raw)
        new_df = pl.read_parquet(buffer)

        rows += new_df.height
        if df is None:
            df = new_df
        else:
            df = df.vstack(new_df)
    if df is None:
        raise Exception("No data found")

    # print(f"Loaded a total of {rows} rows. New df has {df.height} rows")
    if df.height != rows:
        raise Exception("Rows mismatch")

    df = df.sort(table.sort_keys)
    delete_ids = [overlap.id for overlap in overlaps]
    await delete_and_add(table, s3, metadata, delete_ids, df)


@asynccontextmanager
async def build_table(
    table: Table,
    metadata_store: MetadataStore,
    s3: S3Like,
    version: int | None = None,
    table_name: str = "users",
    with_data: bool = True,
    included_mp_ids: set[int] | None = None,
    paths: list[str] | None = None,
):
    ctx = SessionContext()

    await s3.register_dataset(
        ctx=ctx,
        table_name=table_name,
        table=table,
        metadata_store=metadata_store,
        version=version,
        with_data=with_data,
        included_mp_ids=included_mp_ids,
        paths=paths,
    )

    yield ctx


async def simple_insert(metadata_store: MetadataStore, s3: S3Like):
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="users",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="name", type="String"),
            ColumnDefinitions(name="email", type="String"),
        ],
    )

    users = [
        {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"},
        {"id": 3, "name": "John Smith", "email": "john.smith@example.com"},
    ]
    df = pl.DataFrame(users)

    await insert(table, s3, metadata_store, df)

    # Expect the table version to be incremented
    assert await metadata_store.get_table_version(table) == 1
    assert await metadata_store.get_op(table, 1) == SetOpAdd([1])

    async for p in await metadata_store.micropartitions(table, s3):
        assert p.dump().to_dicts() == users

    users.append({"id": 4, "name": "Bill Doe", "email": "bill.doe@example.com"})
    users.append({"id": 5, "name": "Bill Smith", "email": "bill.smith@example.com"})
    df = pl.DataFrame(users[3:])
    await insert(table, s3, metadata_store, df)

    assert await metadata_store.get_table_version(table) == 2
    assert await metadata_store.get_op(table, 2) == SetOpAdd([2])

    i = 0
    async for p in await metadata_store.micropartitions(table, s3):
        if i == 0:
            expected = users[:3]
        elif i == 1:
            expected = users[3:]
        else:
            raise ValueError("Unexpected micro partition")

        dump = p.dump()
        assert dump.to_dicts() == expected, (
            f"Mismatch in micro partition #{p.id}\n\nExp: {expected}\n\nGot: {dump}\n\n"
        )
        i += 1

    assert (await metadata_store.all(table, s3)).to_dicts() == users

    await delete(table, s3, metadata_store, [3])
    assert await metadata_store.get_table_version(table) == 3
    assert await metadata_store.get_op(table, 3) == SetOpDeleteAndAdd(([1], [3]))

    async with build_table(table, metadata_store, s3) as ctx:
        df = ctx.sql("SELECT * FROM users ORDER BY id asc")
        df = df.to_polars()

        active_mps = []
        async for p in await metadata_store.micropartitions(table, s3):
            active_mps.append(p.id)
        assert len(df) == 4
        assert [1, 2, 4, 5] == df["id"].to_list()

    await update(
        table,
        s3,
        metadata_store,
        pl.DataFrame([{"id": 1, "name": "New Name", "email": "new.email@example.com"}]),
    )

    assert await metadata_store.get_table_version(table) == 4
    assert await metadata_store.get_op(table, 4) == SetOpReplace([(3, 4)])

    async with build_table(table, metadata_store, s3) as ctx:
        df = ctx.sql("SELECT * FROM users ORDER BY id asc")
        df = df.to_polars()

        assert len(df) == 4
        first = df.to_dicts()[0]
        assert first["name"] == "New Name"
        assert first["email"] == "new.email@example.com"

    # print("\n\nDone ops. Dumping a `select *` for each version:")
    # versions = metadata_store.get_table_version(table)
    # for v in list(range(1, versions)) + [None]:
    #     with build_table(table, metadata_store, s3, version=v) as ctx:
    #         df = ctx.sql("SELECT * FROM users ORDER BY id asc")
    #         df = df.to_polars()

    # Then test a delete and replace op
    replacements = pl.DataFrame(
        [
            {"id": 6, "name": "6", "email": "6@gmail.com"},
            {"id": 7, "name": "7", "email": "7@gmail.com"},
        ]
    )
    await delete_and_add(table, s3, metadata_store, [2], replacements)
    ids = set((await metadata_store.all(table, s3))["id"].to_list())
    assert ids == {1, 2, 6, 7}

    assert await metadata_store.get_table_version(table) == 5
    assert await metadata_store.get_op(table, 5) == SetOpDeleteAndAdd(([2], [5]))


async def test_simple_insert_fake():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()
    await simple_insert(metadata_store, s3)


async def test_simple_insert_sqlite():
    metadata_store = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    await simple_insert(metadata_store, s3)


async def check_statistics(metadata: MetadataStore, s3: S3Like):
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="users",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="name", type="String"),
            ColumnDefinitions(name="email", type="String"),
        ],
    )

    users = [
        {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"},
        {"id": 3, "name": "John Smith", "email": "john.smith@example.com"},
    ]
    df = pl.DataFrame(users)

    await insert(table, s3, metadata, df)

    async for mp in await metadata.micropartitions(table, s3):
        if mp.id == 0:
            continue
        stats = mp.stats
        assert stats.rows == len(users)
        assert stats.filesize > 0
        id_col = stats.columns[0]
        assert id_col.min == 1
        assert id_col.max == 3
        assert id_col.null_count == 0
        assert id_col.unique_count == 3


async def test_statistics_fake():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()
    await check_statistics(metadata_store, s3)


async def test_statistics_sqlite():
    metadata_store = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    await check_statistics(metadata_store, s3)


async def check_empty_mps_are_deleted(metadata: MetadataStore, s3: S3Like):
    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="users",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="name", type="String"),
            ColumnDefinitions(name="email", type="String"),
        ],
    )

    users = [
        {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"},
        {"id": 3, "name": "John Smith", "email": "john.smith@example.com"},
    ]
    df = pl.DataFrame(users)

    await insert(table, s3, metadata, df)

    users = [
        {"id": 4, "name": "Bill Doe", "email": "bill.doe@example.com"},
        {"id": 5, "name": "Bill Smith", "email": "bill.smith@example.com"},
    ]
    df = pl.DataFrame(users)

    await insert(table, s3, metadata, df)

    await delete(table, s3, metadata, [1, 2, 3])

    ids = set()
    async for mp in await metadata.micropartitions(table, s3):
        ids.add(mp.id)

    assert ids == {2}


async def test_empty_mps_are_deleted_fake():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()
    await check_empty_mps_are_deleted(metadata_store, s3)


async def test_empty_mps_are_deleted_sqlite():
    metadata_store = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    await check_empty_mps_are_deleted(metadata_store, s3)


@pytest.mark.skip(reason="Takes too long to run")
async def test_stress():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()

    table = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="perf",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="date", type="Date"),
            ColumnDefinitions(name="clicks", type="Int64"),
            ColumnDefinitions(name="impressions", type="Int64"),
            ColumnDefinitions(name="sales", type="Int64"),
            ColumnDefinitions(name="spend", type="Int64"),
            ColumnDefinitions(name="orders", type="Int64"),
        ],
    )

    print("\n")

    def rnd(id) -> dict:
        today = datetime.now()
        v = {
            "id": id,
            "campaign_id": random.randint(1, 10),
            "date": (today - timedelta(days=random.randint(0, 30))),
            "clicks": random.randint(1, 10),
            "impressions": random.randint(1, 1000),
            "sales": random.randint(1, 10000),
        }
        id += 1
        return v

    start = perf_counter()
    items = [rnd(i) for i in range(1_000)]
    end = perf_counter()
    print(f"Generate: {end - start} seconds")

    df = pl.DataFrame(items)
    df = df.with_columns(pl.col("date").cast(pl.Date))

    start = perf_counter()
    await insert(table, s3, metadata_store, df)
    end = perf_counter()
    print(f"Insert: {end - start} seconds")

    cnt = len(metadata_store.current_micro_partitions[table.name])
    print(f"Micro partitions: {cnt}")

    async with build_table(table, metadata_store, s3, table_name="perf") as ctx:
        start = perf_counter()
        df = ctx.sql("SELECT sum(clicks) as clicks FROM perf")
        end = perf_counter()
        print(f"Query: {end - start} seconds")
        df = df.to_polars()

        print(df)
        assert df.to_dicts()[0]["clicks"] == sum(i["clicks"] for i in items)


async def test_build_table_only_includes_active_micropartitions():
    """
    Test that build_table only includes active micropartitions and ignores
    old/deleted micropartition files that remain on disk.

    This test reproduces the bug where build_table was including ALL parquet
    files in the directory instead of only the active ones tracked by metadata.
    """

    # Use a temporary directory for this test
    with tempfile.TemporaryDirectory() as temp_dir:
        # Set up environment
        os.environ["DATA_DIR"] = temp_dir

        # Create the directory structure
        table_dir = os.path.join(temp_dir, "test-table", "mps", "bucket")
        os.makedirs(table_dir, exist_ok=True)

        # Use LocalS3 and SqliteMetadata
        metadata_store = SqliteMetadata(f"sqlite:///{temp_dir}/db.db")
        s3 = LocalS3(f"{temp_dir}/test-table/mps")

        # Create the database and schema in metadata
        database = await metadata_store.create_database(Database(id=0, name="test_db"))
        schema = await metadata_store.create_schema(
            Schema(id=0, name="default", database_id=database.id)
        )

        # Create test table
        table = Table(
            id=0,
            schema_id=schema.id,
            database_id=database.id,
            name="test-table",
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="name", type="String"),
                ColumnDefinitions(name="partition_key", type="String"),
            ],
            partition_keys=[
                "partition_key"
            ],  # Use partitioning to trigger append strategy
            sort_keys=["id"],
        )
        table = await metadata_store.create_table(table)

        # Insert initial data - all in same partition to trigger replacement
        initial_data = pl.DataFrame(
            [
                {"id": 1, "name": "Alice", "partition_key": "A"},
                {"id": 2, "name": "Bob", "partition_key": "A"},
            ]
        )
        await insert(table, s3, metadata_store, initial_data)

        # Verify initial state
        async with build_table(
            table, metadata_store, s3, table_name="test-table"
        ) as ctx:
            result = ctx.sql("SELECT COUNT(*) as count FROM 'test-table'").to_polars()
            initial_count = result.to_dicts()[0]["count"]
            assert initial_count == 2, f"Expected 2 rows initially, got {initial_count}"

        # Insert more data in the SAME partition - this will trigger replacement
        # of the old micropartition, leaving the old file on disk but inactive
        additional_data = pl.DataFrame(
            [
                {"id": 3, "name": "Charlie", "partition_key": "A"},
                {"id": 4, "name": "David", "partition_key": "A"},
            ]
        )
        await insert(table, s3, metadata_store, additional_data)

        # At this point, we should have old micropartition files on disk
        # but only the new/active ones should be included in queries

        # Count all parquet files on disk
        all_files = []
        for root, _, files in os.walk(table_dir):
            for file in files:
                if file.endswith(".parquet"):
                    all_files.append(file)

        # Get active micropartition IDs from metadata
        active_ids = await metadata_store._get_ids(table)

        # Verify we have more files on disk than active micropartitions
        # (this confirms old files are still on disk)
        assert len(all_files) > len(active_ids), (
            f"Expected more files on disk ({len(all_files)}) than active MPs ({len(active_ids)}). "
            "This suggests the test setup didn't create the expected scenario."
        )

        # The key test: build_table should only return data from active micropartitions
        async with build_table(
            table, metadata_store, s3, table_name="test-table"
        ) as ctx:
            result = ctx.sql("SELECT COUNT(*) as count FROM 'test-table'").to_polars()
            final_count = result.to_dicts()[0]["count"]

            # Should have exactly 4 rows (2 initial + 2 additional)
            # If the bug existed, this would be higher due to including old files
            assert final_count == 4, (
                f"Expected exactly 4 rows, got {final_count}. "
                f"Active MPs: {len(active_ids)}, Files on disk: {len(all_files)}. "
                "This suggests build_table is including inactive micropartition files."
            )

            # Verify the actual data is correct
            all_data = ctx.sql("SELECT * FROM 'test-table' ORDER BY id").to_polars()
            expected_names = ["Alice", "Bob", "Charlie", "David"]
            actual_names = all_data["name"].to_list()
            assert actual_names == expected_names, (
                f"Expected names {expected_names}, got {actual_names}"
            )
