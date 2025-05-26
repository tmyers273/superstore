import io
import os
from time import perf_counter

import polars as pl
import pytest

from classes import ColumnDefinitions, Database, Schema, Statistics, Table
from local_s3 import LocalS3
from metadata import MetadataStore
from ops.insert import insert
from sqlite_metadata import SqliteMetadata
from sweep import find_ids_with_most_overlap
from tests.run_test import build_table, cluster, delete_and_add
from util import timer


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
            ColumnDefinitions(name="date", type="Date"),
        ],
    )


async def create_table_if_needed(metadata: MetadataStore) -> Table:
    database = await metadata.get_database("ams")
    if database is None:
        database = await metadata.create_database(Database(id=0, name="ams"))

    schema = await metadata.get_schema("default")
    if schema is None:
        schema = await metadata.create_schema(
            Schema(id=0, name="default", database_id=database.id)
        )

    table = metadata.get_table("sp-traffic")
    if table is None:
        table = get_table()
        table.schema_id = schema.id
        table.database_id = database.id
        table.partition_keys = ["advertiser_id"]
        table.sort_keys = ["time_window_start"]
        table = await metadata.create_table(table)

    return table


def cleanup(
    mp_path: str = "ams_scratch/mps/sp-traffic/bucket",
    db_path: str = "ams_scratch/ams.db",
):
    # Delete all *.parquet files in ams_scratch/mps/bucket
    for root, dirs, files in os.walk(mp_path):
        for file in files:
            if file.endswith(".parquet"):
                os.remove(os.path.join(root, file))

    # Delete sqlite db
    os.remove(db_path)


@pytest.mark.skip(reason="Skipping ams test")
def test_query_time():
    os.environ["DATA_DIR"] = "ams_scratch"

    start = perf_counter()
    metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    table = create_table_if_needed(metadata)
    s3 = LocalS3("ams_scratch/sp-traffic/mps")

    # 626, 829, 831

    cnt = 0
    found = 0
    search = "ENTITY2IMWE41VQFHYI"
    included_ids = []
    paths = []
    data_dir = os.getenv("DATA_DIR")
    for mp in metadata.micropartitions(
        table, s3, with_data=False, prefix=f"advertiser_id={search}"
    ):
        cnt += 1

        advertiser_id = mp.stats.get("advertiser_id")
        if advertiser_id is None:
            continue

        time_window_start = mp.stats.get("time_window_start")
        if time_window_start is None:
            continue

        if (
            search >= advertiser_id.min
            and search <= advertiser_id.max
            and "2025-05-01 00:00" >= time_window_start.min
            and "2025-05-04 00:00" < time_window_start.max
        ):
            found += 1
            included_ids.append(mp.id)
            paths.append(
                os.path.join(
                    data_dir,
                    table.name,
                    "mps",
                    "bucket",
                    mp.key_prefix or "",
                    f"{mp.id}.parquet",
                )
            )

    print(f"{search} found in {found}/{cnt} MPs")

    start = perf_counter()
    times = {}
    with build_table(
        table,
        metadata,
        s3,
        table_name="sp-traffic",
        with_data=False,
        # included_mp_ids=set(included_ids),
        paths=paths,
    ) as ctx:
        # print(f"Done building table: {(perf_counter() - start) * 1000:.0f}ms")
        # with timer("Time to get count") as t:
        #     df = ctx.sql("SELECT count(*) FROM 'sp-traffic'")
        #     df = df.to_polars()
        # times["total_count"] = t.duration_ms
        # print(df)

        # with timer("Time to get counts by advertiser_id") as t:
        #     df = ctx.sql(
        #         "SELECT advertiser_id, count(*) as cnt FROM 'sp-traffic' GROUP BY advertiser_id ORDER BY cnt DESC"
        #     )
        #     df = df.to_polars()
        # times["count_by_advertiser"] = t.duration_ms
        # print(df)

        with timer("Time to get sum of clicks, impressions, cost by date") as t:
            out = ctx.sql(
                """
                SELECT cast(campaign_id as bigint) as campaign_id, cast(ad_group_id as bigint) as ad_group_id, cast(ad_id as bigint) as ad_id, sum(clicks), sum(impressions), sum(cost), date
                FROM 'sp-traffic' 
                WHERE advertiser_id = 'ENTITY2IMWE41VQFHYI'  and 
                (
                    (time_window_start >= '2025-05-01 00:00' and time_window_start < '2025-05-02 00:00') or 
                    (time_window_start >= '2025-05-02 00:00' and time_window_start < '2025-05-03 00:00') or 
                    (time_window_start >= '2025-05-03 00:00' and time_window_start < '2025-05-04 00:00') 
                ) 
                GROUP BY date, campaign_id, ad_group_id, ad_id
            """
            )
            df = out.to_polars()
        print(out.explain(verbose=False))

        times["certain_advertiser_specific_dates"] = t.duration_ms

        version = metadata.get_table_version(table)
        print(f"Version: {version}")
        print(df)
        print(times)

        # with open("times.csv", "a") as f:
        #     for query, time in times.items():
        #         f.write(f"{version},{query},{time}\n")

        # With no compaction
        # 651966 MPs
        #     Time to get 1755 wanted ids: 2810.659958049655 ms
        #     Time to create dataset: 23218.510708073154 ms
        #     Time to register dataset: 3.441500011831522 ms
        # Done building table: 26562ms
        # Time to get count: 61519 ms
        # Time to get counts by advertiser_id: 90711 ms
        # Time to get sum of clicks, impressions, cost by date: 906 ms
        #
        # Partitoned by advertiser_id, then compacted
        # 21906 MPs
        #     Time to get 63 wanted ids: 4570.850042044185 ms
        #     Time to create dataset: 27384.887292049825 ms
        #     Time to register dataset: 3.147540963254869 ms
        # Done building table: 32423ms
        # Time to get count: 56705 ms
        # Time to get counts by advertiser_id: 88207 ms
        # Time to get sum of clicks, impressions, cost by date: 1531 ms


@pytest.mark.skip(reason="Skipping ams test")
def test_clustering3() -> None:
    os.environ["DATA_DIR"] = "ams_scratch"

    metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    table = create_table_if_needed(metadata)
    s3 = LocalS3("ams_scratch/sp-traffic/mps")

    cluster(metadata, s3, table)


@pytest.mark.skip(reason="Skipping ams test")
def test_clustering2() -> None:
    metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    create_table_if_needed(metadata)
    s3 = LocalS3("ams_scratch/sp-traffic/mps")
    table = metadata.get_table("sp-traffic")
    if table is None:
        raise Exception("Table not found")

    stats: dict[int, Statistics] = {}
    cnt = 0
    found = 0
    search = "ENTITY2IMWE41VQFHYI"
    index: int | None = None
    for mp in metadata.micropartitions(table, s3):
        if index is None:
            for col in mp.stats.columns:
                if col.name == "advertiser_id":
                    index = col.index
                    break
        if index is None:
            raise Exception("advertiser_id not found")

        # stats = mp.statistics()
        col = mp.stats.columns[index]
        cnt += 1
        if search >= col.min and search <= col.max:
            found += 1
            if col.unique_count > 1:
                stats[mp.id] = mp.stats

    if index is None:
        raise Exception("advertiser_id not found")

    print(f"{search} found in {found}/{cnt} MPs")
    stats_list: list[Statistics] = list(stats.values())
    stats_list = sorted(
        stats_list, key=lambda x: x.columns[index].unique_count, reverse=True
    )

    # for stat in stats_list:
    #     print(stat.columns[index].unique_count)

    # Cap the total filesize of the parquet files
    # We expect the ram usage of the df to be some
    # multiple (2-5x?) of the parquet filesize.
    #
    # If we want to cap the total ram usage to 512mb,
    # then we can cap the total parquet filesize to 512mb / 5 = ~100mb
    # or 512mb / 2 = 256mb
    max_filesize = 128 * 1024 * 1024  # 128mb
    total_size = 0
    last = 0
    for i, stat in enumerate(stats_list):
        total_size += stat.filesize
        if total_size > max_filesize:
            last = i
            break
    if last == 0:
        last = len(stats_list)

    print(f"Found a total of {len(stats_list)} overlapping MPs")
    stats_list = stats_list[:last]

    print(
        f"Found {len(stats_list)} overlapping MPs collectively under {max_filesize / 1024 / 1024:.2f}mb"
    )

    # Load each and vstack info a single df
    df: pl.DataFrame | None = None
    rows = 0
    for stat in stats_list:
        raw = s3.get_object("bucket", f"{stat.id}")
        if raw is None:
            raise Exception(f"Micropartition {stat.id} not found")

        buffer = io.BytesIO(raw)

        # Reset buffer position and read the dataframe
        buffer.seek(0)
        new_df = pl.read_parquet(buffer)

        rows += new_df.height
        if df is None:
            df = new_df
        else:
            df = df.vstack(new_df)
    if df is None:
        raise Exception("No data found")

    print(f"Loaded a total of {rows} rows. New df has {df.height} rows")
    if df.height != rows:
        raise Exception("Rows mismatch")

    df = df.sort(["advertiser_id", "time_window_start"])
    delete_ids = [stat.id for stat in stats_list]
    delete_and_add(table, s3, metadata, delete_ids, df)


@pytest.mark.skip(reason="Skipping ams test")
def test_clustering() -> None:
    metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    create_table_if_needed(metadata)
    s3 = LocalS3("ams_scratch/mps")
    table = metadata.get_table("sp-traffic")
    if table is None:
        raise Exception("Table not found")

    stats: dict[int, Statistics] = {}
    print("Loading stats")
    for mp in metadata.micropartitions(table, s3, with_data=False):
        stats[mp.id] = mp.stats

    index = 0
    for stat in stats.values():
        for col in stat.columns:
            if col.name == "advertiser_id":
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
    overlaps = sorted(overlaps, key=lambda x: x.id)

    if len(overlaps) <= 1:
        print("No overlaps found, trying to combine small MPs")

        # Try to combine small MPs
        SMALL_CUTOFF = 8 * 1024 * 1024  # 8mb
        for stat in stats.values():
            if stat.filesize < SMALL_CUTOFF:
                overlaps.append(stat)
        overlaps = sorted(overlaps, key=lambda x: x.id)

    if len(overlaps) <= 0:
        print("No overlaps found, exiting")
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
        mp = s3.get_object("bucket", f"{overlap.id}")
        if mp is None:
            raise Exception(f"Micropartition {overlap.id} not found")

        buffer = io.BytesIO(mp)

        # Reset buffer position and read the dataframe
        buffer.seek(0)
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

    df = df.sort(["advertiser_id", "time_window_start"])
    delete_ids = [overlap.id for overlap in overlaps]
    delete_and_add(table, s3, metadata, delete_ids, df)


@pytest.mark.skip(reason="Skipping ams test")
def test_ams():
    os.environ["DATA_DIR"] = "ams_scratch"
    cleanup(mp_path="ams_scratch/sp-traffic/mps/bucket")
    files = get_parquet_files("./ams")
    print("Found", len(files), "parquet files")

    table = get_table()

    metadata_store = SqliteMetadata("sqlite:///ams_scratch/ams.db")
    s3 = LocalS3("ams_scratch/sp-traffic/mps")
    table = create_table_if_needed(metadata_store)
    print("Table", table)
    # s3 = FakeS3()

    with build_table(table, metadata_store, s3, table_name="sp-traffic") as ctx:
        s = perf_counter()
        df = ctx.sql("SELECT count(*) FROM 'sp-traffic'")
        df = df.to_polars()
        e = perf_counter()
        print(f"Time: {e - s} seconds")

        print(df)

    total_dur = 0
    total_count = 0
    for i, file in enumerate(files):
        s = perf_counter()
        print(f"Processing {file} ({i + 1}/{len(files)})")
        df = pl.read_parquet(file)

        df = df.with_columns(
            pl.col("time_window_start").str.to_datetime().alias("date").cast(pl.Date)
        )

        if i > 40:
            break

        insert(table, s3, metadata_store, df)

        dur = perf_counter() - s
        total_count += df.height
        total_dur += dur
        rate = df.height / dur / 1000
        total_rate = total_count / total_dur / 1000
        print(
            f"    Inserted {df.height} rows at {rate:.2f}k rows/s (total rate: {total_rate:.2f}k rows/s)"
        )

    stats = {}
    for mp in metadata_store.micropartitions(table, s3):
        stats[mp.id] = mp.stats

    print(f"Total count from parquet files: {total_count}")

    with build_table(table, metadata_store, s3, table_name="sp-traffic") as ctx:
        s = perf_counter()
        df = ctx.sql("SELECT count(*) as count FROM 'sp-traffic'")
        df = df.to_polars()
        e = perf_counter()
        print(f"Time: {e - s} seconds")

        print(df)
        assert df.to_dicts()[0]["count"] == total_count
