import polars as pl

from classes import Database, Schema, Table
from metadata import FakeMetadataStore, MetadataStore
from s3 import FakeS3, S3Like
from sqlite_metadata import SqliteMetadata
from tests.run_test import delete, insert, update


def check_partition_keys(metadata: MetadataStore, s3: S3Like):
    db = metadata.create_database(Database(id=0, name="test_db"))
    schema = metadata.create_schema(Schema(id=0, name="test_schema", database_id=db.id))
    table = metadata.create_table(
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

    insert(table, s3, metadata, df)

    cnt = 0
    for mp in metadata.micropartitions(table, s3):
        cnt += 1

    # Expect 3 MPs, one for each unique user_id
    assert cnt == 3

    # Expect objects to be at "user_id=1/1"
    assert s3.get_object("bucket", "user_id=1/1") is not None
    assert s3.get_object("bucket", "user_id=2/2") is not None
    assert s3.get_object("bucket", "user_id=3/3") is not None

    # Update an item
    df = pl.DataFrame(
        {
            "id": [4],
            "user_id": [3],
            "clicks": [50],
            "clicks2": [50],
        }
    )
    update(table, s3, metadata, df)

    cnt = 0
    for mp in metadata.micropartitions(table, s3):
        print(mp.id, mp.key_prefix)
        cnt += 1

    # Expect 3 MPs, one for each unique user_id
    assert cnt == 3

    # Expect objects to be at "user_id=1/1"
    assert s3.get_object("bucket", "user_id=1/1") is not None
    assert s3.get_object("bucket", "user_id=2/2") is not None
    assert s3.get_object("bucket", "user_id=3/4") is not None

    # Delete an item from a MP that has multiple items
    delete(table, s3, metadata, [4])

    cnt = 0
    for mp in metadata.micropartitions(table, s3):
        cnt += 1

    # Expect 3 MPs, one for each unique user_id
    assert cnt == 3

    assert s3.get_object("bucket", "user_id=1/1") is not None
    assert s3.get_object("bucket", "user_id=2/2") is not None
    assert s3.get_object("bucket", "user_id=3/4") is not None

    # Delete an object from an MP with a single item, causing the MP
    # itself to be deleted
    delete(table, s3, metadata, [3])

    cnt = 0
    for mp in metadata.micropartitions(table, s3):
        cnt += 1

    assert cnt == 2

    assert s3.get_object("bucket", "user_id=1/1") is not None
    assert s3.get_object("bucket", "user_id=2/2") is not None


def test_partition_keys_fake():
    metadata = FakeMetadataStore()
    s3 = FakeS3()
    check_partition_keys(metadata, s3)


def test_partition_keys_sqlite():
    metadata = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    check_partition_keys(metadata, s3)


# TODO test an update that changes the partition key
