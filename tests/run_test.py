import io
import os
import random
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta
from time import perf_counter

import polars as pl
import pyarrow.dataset as ds
import pytest
from datafusion import SessionContext

from ..classes import ColumnDefinitions, Header, MicroPartition, Statistics, Table
from ..compress import compress
from ..local_s3 import LocalS3
from ..metadata import FakeMetadataStore, MetadataStore
from ..s3 import FakeS3, S3Like
from ..set_ops import SetOpAdd, SetOpDeleteAndAdd, SetOpReplace
from ..sqlite_metadata import SqliteMetadata


class Metadata:
    pass


def insert(
    table: Table, s3: S3Like, metadata_store: MetadataStore, items: pl.DataFrame
):
    # TODO: validate schema

    # Get the current table version number
    current_version = metadata_store.get_table_version(table)

    # Build the parquet file
    buffer = io.BytesIO()
    items.write_parquet(buffer)
    buffer.seek(0)

    # Create a new micro partition
    print("Compressing...")
    parts = compress(items)
    print(f"Creating micro partitions with {len(parts)} parts...")
    micro_partitions = []
    reserved_ids = metadata_store.reserve_micropartition_ids(table, len(parts))

    for i, part in enumerate(parts):
        id = reserved_ids[i]
        part.seek(0)
        stats = Statistics.from_bytes(part)
        part.seek(0)

        df = pl.read_parquet(io.BytesIO(part.getvalue()))
        print(f"# rows for {id}: stats: {stats.rows}, actual: {df.height}")
        stats.id = id
        micro_partition = MicroPartition(
            id=id,
            header=Header(table_id=table.id),
            data=None,
            stats=stats,
        )

        # Try saving to S3
        part.seek(0)
        s3.put_object("bucket", str(id), part.getvalue())

        micro_partitions.append(micro_partition)

    # Update metadata
    metadata_store.add_micro_partitions(table, current_version, micro_partitions)


def delete(table: Table, s3: S3Like, metadata_store: MetadataStore, pks: list[int]):
    # print(f"[mutation] Deleting {len(pks)} rows from {table.name}")
    # TODO: validate schema
    # TODO: error if pk is not found?

    # Get the current table version number
    current_version = metadata_store.get_table_version(table)

    # Load the parquet file
    replacements: dict[int, MicroPartition] = {}
    updated_ids = 0
    new_mps: list[(int, io.BytesIO)] = []
    for p in metadata_store.micropartitions(table, s3):
        df = p.dump()
        before_cnt = len(df)
        df = df.filter(~pl.col("id").is_in(pks))
        after_cnt = len(df)

        # Skip if no rows were deleted from this micro partition
        if after_cnt == before_cnt:
            continue

        updated_ids += 1

        buffer = io.BytesIO()
        df.write_parquet(buffer)
        new_mps.append((p.id, buffer))

    reserved_ids = metadata_store.reserve_micropartition_ids(table, len(new_mps))
    for i, (old_id, buffer) in enumerate(new_mps):
        # Create a new micro partition
        id = reserved_ids[i]
        stats = Statistics.from_bytes(buffer)
        stats.id = id
        micro_partition = MicroPartition(
            id=id,
            header=Header(table_id=table.id),
            data=None,
            stats=stats,
        )

        # Try saving to S3
        s3.put_object("bucket", str(id), buffer.getvalue())

        replacements[old_id] = micro_partition

    # Update metadata
    metadata_store.replace_micro_partitions(table, current_version, replacements)


def delete_and_add(
    table: Table,
    s3: S3Like,
    metadata_store: MetadataStore,
    delete_ids: list[int],
    new_df: pl.DataFrame,
):
    current_version = metadata_store.get_table_version(table)

    mps = []

    dfs = compress(new_df)
    reserved_ids = metadata_store.reserve_micropartition_ids(table, len(dfs))
    for i, buffer in enumerate(dfs):
        id = reserved_ids[i]

        # Create a new micro partition
        stats = Statistics.from_bytes(buffer)
        stats.id = id
        micro_partition = MicroPartition(
            id=id,
            header=Header(table_id=table.id),
            data=buffer.getvalue(),
            stats=stats,
        )
        mps.append(micro_partition)

        # Try saving to S3
        s3.put_object("bucket", str(id), buffer.getvalue())

    metadata_store.delete_and_add_micro_partitions(
        table, current_version, delete_ids, mps
    )


def update(
    table: Table, s3: S3Like, metadata_store: MetadataStore, items: pl.DataFrame
):
    # print(f"[mutation] Updating {len(items)} rows in {table.name}")
    # TODO: validate schema
    # TODO: error if pk is not found?

    # Get the current table version number
    current_version = metadata_store.get_table_version(table)

    # Load the parquet file
    replacements: dict[int, MicroPartition] = {}
    reserved_ids = metadata_store.reserve_micropartition_ids(table, len(items))
    i = 0
    existing_ids = set()
    for p in metadata_store.micropartitions(table, s3):
        df = p.dump()
        updated_items = items.filter(pl.col("id").is_in(df["id"]))
        existing_ids.add(p.id)

        # TODO: only flag this for an update if something actually changed

        # Skip if no rows were changed from this micro partition
        if len(updated_items) == 0:
            continue

        # Merge the new items
        df = df.vstack(updated_items).unique(subset=["id"], keep="last")

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
        )
        print("id is", id, reserved_ids)

        # Try saving to S3
        s3.put_object("bucket", str(id), buffer.getvalue())

        replacements[p.id] = micro_partition
        i += 1
    print(existing_ids)

    # Update metadata
    metadata_store.replace_micro_partitions(table, current_version, replacements)


@contextmanager
def build_table(
    table: Table,
    metadata_store: MetadataStore,
    s3: S3Like,
    version: int | None = None,
    table_name: str = "users",
    with_data: bool = True,
    included_mp_ids: set[int] | None = None,
):
    ctx = SessionContext()

    match s3:
        case FakeS3():
            with tempfile.TemporaryDirectory() as tmpdir:
                for p in metadata_store.micropartitions(
                    table, s3, version=version, with_data=with_data
                ):
                    if included_mp_ids is not None and p.id not in included_mp_ids:
                        continue
                    path = os.path.join(tmpdir, f"{p.id}.parquet")
                    p.dump().write_parquet(path)

                dataset = ds.dataset(tmpdir, format="parquet")
                ctx.register_dataset(table_name, dataset)
                yield ctx
        case LocalS3():
            wanted_ids = []
            s = perf_counter()

            ids = metadata_store._get_ids(table, version)
            for id in ids:
                if included_mp_ids is not None and id not in included_mp_ids:
                    continue
                wanted_ids.append(id)

            e = perf_counter()
            print(f"Time to get {len(wanted_ids)} wanted ids: {(e - s) * 1000} ms")

            if table_name == "sp-traffic":
                base_dir = "ams_scratch/mps/bucket"
            else:
                base_dir = "scratch/audit_log_items/mps/bucket"
            paths = [f"{base_dir}/{i}.parquet" for i in wanted_ids]
            dataset = ds.dataset(paths, format="parquet")
            ctx.register_dataset(table_name, dataset)
            yield ctx
        case _:
            raise ValueError(f"Unsupported S3 type: {type(s3)}")

    # allowed = {169, 170, 171}
    # with tempfile.TemporaryDirectory() as tmpdir:
    # for p in metadata_store.micropartitions(table, s3, version=version):
    #     path = os.path.join(tmpdir, f"{p.id}.parquet")
    #     p.dump().write_parquet(path)

    # wanted_ids = []
    # for p in metadata_store.micropartitions(table, s3, version=version):
    #     # path = os.path.join(tmpdir, f"{p.id}.parquet")
    #     # p.dump().write_parquet(path)
    #     wanted_ids.append(p.id)

    # base_dir = "ams_scratch/mps/bucket"
    # paths = [f"{base_dir}/{i}.parquet" for i in wanted_ids]
    # dataset = ds.dataset(paths, format="parquet")
    # ctx.register_dataset(table_name, dataset)

    # # ctx.register_parquet(table_name, tmpdir)
    # yield ctx


def simple_insert(metadata_store: MetadataStore, s3: S3Like):
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

    insert(table, s3, metadata_store, df)

    # Expect the table version to be incremented
    assert metadata_store.get_table_version(table) == 1
    assert metadata_store.get_op(table, 1) == SetOpAdd([1])

    for p in metadata_store.micropartitions(table, s3):
        assert p.dump().to_dicts() == users

    users.append({"id": 4, "name": "Bill Doe", "email": "bill.doe@example.com"})
    users.append({"id": 5, "name": "Bill Smith", "email": "bill.smith@example.com"})
    df = pl.DataFrame(users[3:])
    insert(table, s3, metadata_store, df)

    assert metadata_store.get_table_version(table) == 2
    assert metadata_store.get_op(table, 2) == SetOpAdd([2])

    for i, p in enumerate(metadata_store.micropartitions(table, s3)):
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

    assert metadata_store.all(table, s3).to_dicts() == users

    delete(table, s3, metadata_store, [3])
    assert metadata_store.get_table_version(table) == 3
    assert metadata_store.get_op(table, 3) == SetOpReplace([(1, 3)])

    with build_table(table, metadata_store, s3) as ctx:
        df = ctx.sql("SELECT * FROM users ORDER BY id asc")
        df = df.to_polars()

        active_mps = []
        for p in metadata_store.micropartitions(table, s3):
            active_mps.append(p.id)
        assert len(df) == 4
        assert [1, 2, 4, 5] == df["id"].to_list()

    update(
        table,
        s3,
        metadata_store,
        pl.DataFrame([{"id": 1, "name": "New Name", "email": "new.email@example.com"}]),
    )

    assert metadata_store.get_table_version(table) == 4
    assert metadata_store.get_op(table, 4) == SetOpReplace([(3, 4)])

    with build_table(table, metadata_store, s3) as ctx:
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
    delete_and_add(table, s3, metadata_store, [2], replacements)
    ids = set(metadata_store.all(table, s3)["id"].to_list())
    assert ids == {1, 2, 6, 7}

    assert metadata_store.get_table_version(table) == 5
    assert metadata_store.get_op(table, 5) == SetOpDeleteAndAdd(([2], [5]))


def test_simple_insert_fake():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()
    simple_insert(metadata_store, s3)


def test_simple_insert_sqlite():
    metadata_store = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    simple_insert(metadata_store, s3)


def check_statistics(metadata: MetadataStore, s3: S3Like):
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

    insert(table, s3, metadata, df)

    for mp in metadata.micropartitions(table, s3):
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


def test_statistics_fake():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()
    check_statistics(metadata_store, s3)


def test_statistics_sqlite():
    metadata_store = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    check_statistics(metadata_store, s3)


@pytest.mark.skip(reason="Takes too long to run")
def test_stress():
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
    insert(table, s3, metadata_store, df)
    end = perf_counter()
    print(f"Insert: {end - start} seconds")

    cnt = len(metadata_store.current_micro_partitions[table.name])
    print(f"Micro partitions: {cnt}")

    with build_table(table, metadata_store, s3, table_name="perf") as ctx:
        start = perf_counter()
        df = ctx.sql("SELECT sum(clicks) as clicks FROM perf")
        end = perf_counter()
        print(f"Query: {end - start} seconds")
        df = df.to_polars()

        print(df)
        assert df.to_dicts()[0]["clicks"] == sum(i["clicks"] for i in items)
