import json
import os
from time import perf_counter
import polars as pl
import pytest

from ..classes import ColumnDefinitions, Table
from ..local_s3 import LocalS3
from ..metadata import FakeMetadataStore
from .run_test import build_table, insert
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


def test_queries():
    metadata_store = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    s3 = LocalS3("ams_scratch/mps")
    table = get_table()

    stats = metadata_store.get_stats(table, s3, None)
    for s in stats:
        print(s.id, s.rows, s.filesize, s.filesize / s.rows)
    out = [m.model_dump() for m in stats]
    # d = json.dumps(out)
    # print(d)

    # with build_table(table, metadata_store, s3, table_name="sp-traffic") as ctx:
    #     df = ctx.sql("SELECT count(*) FROM 'sp-traffic'")
    #     df = df.to_polars()


@pytest.mark.skip(reason="Skipping ams test")
def test_ams():
    files = get_parquet_files("./ams")
    print("Found", len(files), "parquet files")

    # Load the first one and print the schema
    df = pl.read_parquet(files[0])
    for k, v in df.schema.items():
        print(k, v)

    table = get_table()

    metadata_store = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    s3 = LocalS3("ams_scratch/mps")
    # s3 = FakeS3()

    for i, file in enumerate(files):
        print(f"Processing {file} ({i + 1}/{len(files)})")
        df = pl.read_parquet(file)
        if i > 8:
            break

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
