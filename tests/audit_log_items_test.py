import re
from time import perf_counter

import polars as pl
import pytest

from classes import ColumnDefinitions, Database, Schema, Table
from local_s3 import LocalS3
from metadata import MetadataStore
from sqlite_metadata import SqliteMetadata
from tests.ams_test import cleanup, get_parquet_files
from tests.run_test import build_table, insert
from util import timer


def get_table() -> Table:
    return Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="audit_log_items",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="audit_log_id", type="Int64"),
            ColumnDefinitions(name="target_id", type="Int64"),
            ColumnDefinitions(name="target_type_id", type="Int16"),
            ColumnDefinitions(name="meta", type="String"),
            ColumnDefinitions(name="created_at", type="Timestamp"),
            ColumnDefinitions(name="updated_at", type="Timestamp"),
        ],
    )


def create_table_if_needed(metadata: MetadataStore) -> Table:
    database = metadata.get_database("db")
    if database is None:
        database = metadata.create_database(Database(id=0, name="db"))

    schema = metadata.get_schema("default")
    if schema is None:
        schema = metadata.create_schema(
            Schema(id=0, name="default", database_id=database.id)
        )

    table = metadata.get_table("audit_log_items")
    if table is None:
        table = get_table()
        table.schema_id = schema.id
        table.database_id = database.id
        metadata.create_table(table)

    table = get_table()
    if metadata.get_table(table.name) is None:
        metadata.create_table(table)

    return table


@pytest.mark.skip(reason="Skipping audit_log_items test")
def test_audit_log_items_query():
    table = get_table()
    metadata_store = SqliteMetadata("sqlite:///scratch/audit_log_items/db.db")
    s3 = LocalS3("scratch/audit_log_items/mps")
    create_table_if_needed(metadata_store)

    # Pre-prune
    # wanted_ids = set()
    # for mp in metadata_store.micropartitions(table, s3, with_data=False):
    #     stats = mp.stats

    with build_table(
        table, metadata_store, s3, table_name="audit_log_items", with_data=False
    ) as ctx:
        with timer("Total items in `audit_log_items`"):
            df = ctx.sql("SELECT count(*) FROM 'audit_log_items'")
            df = list(df.to_polars().to_dicts()[0].values())[0]
            print(f"Total items in `audit_log_items`: {df}")

        with timer("Total items in `audit_log_items` by audit_log_id"):
            df = ctx.sql(
                "SELECT count(*) as cnt, audit_log_id FROM 'audit_log_items' GROUP BY audit_log_id ORDER BY cnt desc"
            ).to_polars()
            print(df)
            print(f"Total items in `audit_log_items`: {df}")

        with timer("Select * biggest"):
            df = ctx.sql("SELECT * FROM 'audit_log_items' WHERE audit_log_id = 72850")
            print(df.to_polars())


@pytest.mark.skip(reason="Skipping audit_log_items test")
def test_audit_log_items():
    cleanup(
        mp_path="scratch/audit_log_items/mps", db_path="scratch/audit_log_items/db.db"
    )
    files = get_parquet_files("./data/audit_log_items")
    print("Found", len(files), "parquet files")

    # Sort in numerical order, not lexicographical
    def extract_number(filename):
        match = re.search(r"audit_log_items_(\d+)-", filename)
        if match:
            return int(match.group(1))
        return 0  # fallback

    files.sort(key=extract_number)

    table = get_table()
    metadata_store = SqliteMetadata("sqlite:///scratch/audit_log_items/db.db")
    s3 = LocalS3("scratch/audit_log_items/mps")
    create_table_if_needed(metadata_store)
    # s3 = FakeS3()

    for i, file in enumerate(files):
        # if i > 5:
        #     break
        print(f"Processing {file} ({i + 1}/{len(files)})")

        df = pl.read_parquet(file)
        df = df.sort(["audit_log_id", "id"])

        insert(table, s3, metadata_store, df)

    stats = {}
    for mp in metadata_store.micropartitions(table, s3):
        stats[mp.id] = mp.stats

    # for id, stat in stats.items():
    #     print(f"Micropartition {id}:")
    #     stat.dump()
    # break

    with build_table(table, metadata_store, s3, table_name="audit_log_items") as ctx:
        s = perf_counter()
        df = ctx.sql("SELECT count(*) FROM 'audit_log_items'")
        df = df.to_polars()
        e = perf_counter()
        print(f"Time: {e - s} seconds")

        print(df)
