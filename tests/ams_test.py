import base64
import io
import json
import logging
import os
from time import perf_counter
import polars as pl
import pytest

from ..compress import compress

from ..sweep import find_ids_with_most_overlap

from ..classes import ColumnDefinitions, Table, Database, Schema
from ..local_s3 import LocalS3
from ..metadata import FakeMetadataStore, MetadataStore
from .run_test import build_table, delete_and_add, insert
from ..s3 import FakeS3
from ..sqlite_metadata import SqliteMetadata


def get_parquet_files(path: str) -> list[str]:
    out = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                out.append(os.path.join(root, file))
    return out


def get_table() -> Table:
    return Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="sp-traffic",
        columns=[
            ColumnDefinitions(name="idempotency_id", type="String"),
            ColumnDefinitions(name="dataset_id", type="String"),
            ColumnDefinitions(name="marketplace_id", type="String"),
            ColumnDefinitions(name="currency", type="String"),
            ColumnDefinitions(name="advertiser_id", type="String"),
            ColumnDefinitions(name="campaign_id", type="String"),
            ColumnDefinitions(name="ad_group_id", type="String"),
            ColumnDefinitions(name="ad_id", type="String"),
            ColumnDefinitions(name="keyword_id", type="String"),
            ColumnDefinitions(name="keyword_text", type="String"),
            ColumnDefinitions(name="match_type", type="String"),
            ColumnDefinitions(name="placement", type="String"),
            ColumnDefinitions(name="time_window_start", type="String"),
            ColumnDefinitions(name="clicks", type="Int64"),
            ColumnDefinitions(name="impressions", type="Int64"),
            ColumnDefinitions(name="cost", type="Float64"),
        ],
    )


def create_table_if_needed(metadata: MetadataStore) -> Table:
    database = metadata.get_database("ams")
    if database is None:
        database = metadata.create_database(Database(id=0, name="ams"))

    schema = metadata.get_schema("default")
    if schema is None:
        schema = metadata.create_schema(
            Schema(id=0, name="default", database_id=database.id)
        )

    table = metadata.get_table("sp-traffic")
    if table is None:
        table = get_table()
        table.schema_id = schema.id
        table.database_id = database.id
        metadata.create_table(table)

    table = get_table()
    if metadata.get_table(table.name) is None:
        metadata.create_table(table)

    return table


def cleanup():
    # Delete all *.parquet files in ams_scratch/mps/bucket
    for root, dirs, files in os.walk("ams_scratch/mps/bucket"):
        for file in files:
            if file.endswith(".parquet"):
                os.remove(os.path.join(root, file))

    # Delete sqlite db
    os.remove("ams_scratch/ams.db")


# @pytest.mark.skip(reason="Skipping ams test")
def test_query_time():
    start = perf_counter()
    metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    table = create_table_if_needed(metadata)
    s3 = LocalS3("ams_scratch/mps")

    # 626, 829, 831

    # for mp in metadata.micropartitions(table, s3):
    #     stats = mp.statistics()
    #     for col in stats.columns:
    #         if col.name == "advertiser_id":
    #             print(f"{mp.id}: {col.min} - {col.max}")
    #             break

    # return

    # 24ms - raw
    # 19ms, 17s - after one
    # 15ms after 2
    # 18ms
    # 15ms
    # 13ms
    # 14ms
    # 15ms
    # 12ms

    print(f"Starting to build table: {(perf_counter() - start) * 1000:.0f}ms")
    with build_table(table, metadata, s3, table_name="sp-traffic") as ctx:
        print(f"Done building table: {(perf_counter() - start) * 1000:.0f}ms")
        s = perf_counter()
        df = ctx.sql("SELECT count(*) FROM 'sp-traffic'")
        df = df.to_polars()
        e = perf_counter()

        print(f"Time: {(e - s) * 1000:.0f}ms")
        print(df)

        s = perf_counter()
        df = ctx.sql(
            "SELECT advertiser_id, count(*) as cnt FROM 'sp-traffic' GROUP BY advertiser_id ORDER BY cnt DESC"
        )
        df = df.to_polars()
        e = perf_counter()

        print(f"Time: {(e - s) * 1000:.0f}ms")
        print(df)

        s = perf_counter()
        df = ctx.sql(
            """
            SELECT sum(clicks), sum(impressions), sum(cost), to_date(time_window_start) as date
            FROM 'sp-traffic' 
            WHERE advertiser_id = 'ENTITY2IMWE41VQFHYI'
            GROUP BY date
            """
        )
        # print(df.explain(verbose=True, analyze=True))
        df = df.to_polars()
        e = perf_counter()

        print(f"Time: {(e - s) * 1000:.0f}ms")
        print(df)

        logging.basicConfig(level=logging.DEBUG)

        # plan = ctx.sql(
        #     "EXPLAIN FORMAT TREE SELECT sum(clicks), sum(impressions), sum(cost) FROM 'sp-traffic' WHERE advertiser_id = 'ENTITY2IMWE41VQFHYI'"
        # )
        # print(plan)


@pytest.mark.skip(reason="Skipping ams test")
def test_clustering():
    metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    table = create_table_if_needed(metadata)
    s3 = LocalS3("ams_scratch/mps")

    table = metadata.get_table("sp-traffic")
    if table is None:
        return {"error": "Table not found"}

    stats = {}
    for mp in metadata.micropartitions(table, s3):
        stats[mp.id] = mp.statistics()

    index = 0
    for stat in stats.values():
        for col in stat.columns:
            if col.name == "advertiser_id":
                index = col.index
                break

    to_sweep: list[tuple[str, str, int]] = []
    for stat in stats.values():
        col = stat.columns[index]
        to_sweep.append((col.min, col.max, stat.id))

    overlap = find_ids_with_most_overlap(to_sweep)
    overlaps = [stats[id] for id in overlap]
    overlaps = sorted(overlaps, key=lambda x: x.id)

    # Only allow up to 64mb of overlapping MPs
    total_size = 0
    for i, overlap in enumerate(overlaps):
        total_size += overlap.filesize
        if total_size > 16 * 64 * 1024 * 1024:
            break

    print(f"Found a total of{len(overlaps)} overlapping MPs")
    overlaps = overlaps[:i]

    print(f"Found {len(overlaps)} overlapping MPs")

    # Load each and vstack info a single df
    df: pl.DataFrame | None = None
    rows = 0
    for overlap in overlaps:
        mp = s3.get_object("bucket", f"{overlap.id}")
        if mp is None:
            raise Exception(f"Micropartition {overlap.id} not found")

        mp = json.loads(mp)["data"]
        buffer = io.BytesIO(mp)

        # Reset buffer position and read the dataframe
        buffer.seek(0)
        new_df = pl.read_parquet(buffer)

        rows += new_df.height
        if df is None:
            df = new_df
        else:
            df = df.vstack(new_df)

    print(f"Loaded a total of {rows} rows. New df has {df.height} rows")
    if df.height != rows:
        raise Exception("Rows mismatch")

    df = df.sort(["advertiser_id", "time_window_start"])
    delete_ids = [overlap.id for overlap in overlaps]
    delete_and_add(table, s3, metadata, delete_ids, df)

    print(metadata.get_ops(table)[-5:])


@pytest.mark.skip(reason="Skipping ams test")
def test_ams():
    cleanup()
    files = get_parquet_files("./ams")
    print("Found", len(files), "parquet files")

    # Load the first one and print the schema
    df = pl.read_parquet(files[0])
    for k, v in df.schema.items():
        print(k, v)

    table = get_table()

    metadata_store = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    s3 = LocalS3("ams_scratch/mps")
    create_table_if_needed(metadata_store)
    # s3 = FakeS3()

    for i, file in enumerate(files):
        print(f"Processing {file} ({i + 1}/{len(files)})")
        df = pl.read_parquet(file)
        # if i > 115:
        #     break

        df = pl.read_parquet(file)
        insert(table, s3, metadata_store, df)
        print(f"    Inserted {df.height} rows")

    stats = {}
    for mp in metadata_store.micropartitions(table, s3):
        stats[mp.id] = mp.statistics()

    for id, stat in stats.items():
        print(f"Micropartition {id}:")
        stat.dump()
        # break

    with build_table(table, metadata_store, s3, table_name="sp-traffic") as ctx:
        s = perf_counter()
        df = ctx.sql("SELECT count(*) FROM 'sp-traffic'")
        df = df.to_polars()
        e = perf_counter()
        print(f"Time: {e - s} seconds")

        print(df)
