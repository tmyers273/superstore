import polars as pl

from classes import Database, Schema, Table
from metadata import FakeMetadataStore, MetadataStore
from ops.insert import insert
from s3 import FakeS3, S3Like
from sqlite_metadata import SqliteMetadata
from tests.run_test import delete, update


async def check_partition_keys(metadata: MetadataStore, s3: S3Like):
    db = await metadata.create_database(Database(id=0, name="test_db"))
    schema = await metadata.create_schema(
        Schema(id=0, name="test_schema", database_id=db.id)
    )
    table = await metadata.create_table(
        Table(
            id=0,
            name="test_table",
            schema_id=schema.id,
            database_id=db.id,
            columns=[],
            partition_keys=["user_id"],
        )
    )

    # Insert some data
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "user_id": [1, 2, 3, 3],
            "clicks": [10, 20, 30, 40],
            "clicks2": [10, 20, 30, 40],
        }
    )

    await insert(table, s3, metadata, df)

    # Expect 3 MPs, one for each unique user_id
    assert metadata.micropartition_count(table, s3) == 3

    # Check that objects exist for each partition key, but don't assume specific MP IDs
    # Collect all micropartitions and their keys
    mp_keys = []
    for mp in metadata.micropartitions(table, s3, with_data=False):
        key = f"{mp.key_prefix or ''}{mp.id}"
        mp_keys.append(key)
        # Verify the object exists in S3
        assert s3.get_object("bucket", key) is not None

    # Verify we have the expected partition prefixes
    partition_prefixes = set()
    for key in mp_keys:
        prefix = key.split("/")[0]  # Extract "user_id=X" part
        partition_prefixes.add(prefix)

    expected_prefixes = {"user_id=1", "user_id=2", "user_id=3"}
    assert partition_prefixes == expected_prefixes

    # Update an item
    df = pl.DataFrame(
        {
            "id": [4],
            "user_id": [3],
            "clicks": [50],
            "clicks2": [50],
        }
    )
    await update(table, s3, metadata, df)

    # Expect 3 MPs, one for each unique user_id
    assert metadata.micropartition_count(table, s3) == 3

    # Check that objects exist for each partition key after update
    mp_keys_after_update = []
    for mp in metadata.micropartitions(table, s3, with_data=False):
        key = f"{mp.key_prefix or ''}{mp.id}"
        mp_keys_after_update.append(key)
        # Verify the object exists in S3
        assert s3.get_object("bucket", key) is not None

    # Verify we still have the expected partition prefixes
    partition_prefixes_after_update = set()
    for key in mp_keys_after_update:
        prefix = key.split("/")[0]  # Extract "user_id=X" part
        partition_prefixes_after_update.add(prefix)

    assert partition_prefixes_after_update == expected_prefixes

    # Delete an item from a MP that has multiple items
    await delete(table, s3, metadata, [4])

    # Expect 3 MPs, one for each unique user_id
    assert metadata.micropartition_count(table, s3) == 3

    # Check that objects exist for each partition key after delete
    mp_keys_after_delete = []
    for mp in metadata.micropartitions(table, s3, with_data=False):
        key = f"{mp.key_prefix or ''}{mp.id}"
        mp_keys_after_delete.append(key)
        # Verify the object exists in S3
        assert s3.get_object("bucket", key) is not None

    # Verify we still have the expected partition prefixes
    partition_prefixes_after_delete = set()
    for key in mp_keys_after_delete:
        prefix = key.split("/")[0]  # Extract "user_id=X" part
        partition_prefixes_after_delete.add(prefix)

    assert partition_prefixes_after_delete == expected_prefixes

    # Delete an object from an MP with a single item, causing the MP
    # itself to be deleted
    await delete(table, s3, metadata, [3])

    assert metadata.micropartition_count(table, s3) == 2

    # Check that objects exist for remaining partition keys after final delete
    mp_keys_final = []
    for mp in metadata.micropartitions(table, s3, with_data=False):
        key = f"{mp.key_prefix or ''}{mp.id}"
        mp_keys_final.append(key)
        # Verify the object exists in S3
        assert s3.get_object("bucket", key) is not None

    # Verify we have the expected partition prefixes (user_id=3 should be gone)
    partition_prefixes_final = set()
    for key in mp_keys_final:
        prefix = key.split("/")[0]  # Extract "user_id=X" part
        partition_prefixes_final.add(prefix)

    expected_prefixes_final = {"user_id=1", "user_id=2"}
    assert partition_prefixes_final == expected_prefixes_final


def test_partition_keys_fake():
    metadata = FakeMetadataStore()
    s3 = FakeS3()
    check_partition_keys(metadata, s3)


def test_partition_keys_sqlite():
    metadata = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    check_partition_keys(metadata, s3)


# TODO test an update that changes the partition key
