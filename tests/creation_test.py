from classes import ColumnDefinitions, Database, Schema, Table
from metadata import FakeMetadataStore, MetadataStore
from s3 import FakeS3, S3Like
from sqlite_metadata import SqliteMetadata


async def table_creation(metadata_store: MetadataStore, s3: S3Like):
    database = Database(id=0, name="my_db")
    schema = Schema(id=0, name="default", database_id=1)
    table = Table(
        id=0,
        schema_id=1,
        database_id=1,
        name="users",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="name", type="String"),
            ColumnDefinitions(name="email", type="String"),
        ],
    )

    db = await metadata_store.create_database(database)
    schema.database_id = db.id
    schema = await metadata_store.create_schema(schema)
    table.schema_id = schema.id
    table = await metadata_store.create_table(table)

    databases = await metadata_store.get_databases()
    assert len(databases) == 1
    assert databases[0].id == 1
    assert databases[0].name == "my_db"
    db = await metadata_store.get_database("my_db")
    assert db is not None
    assert db.id == 1
    assert db.name == "my_db"

    schemas = await metadata_store.get_schemas()
    assert len(schemas) == 1
    assert schemas[0].id == 1
    assert schemas[0].name == "default"
    schema_obj = await metadata_store.get_schema("default")
    assert schema_obj is not None
    assert schema_obj.id == 1
    assert schema_obj.name == "default"

    tables = await metadata_store.get_tables()
    assert len(tables) == 1
    assert tables[0].id == 1
    assert tables[0].name == "users"
    table_obj = metadata_store.get_table("users")
    assert table_obj is not None
    assert table_obj.id == 1
    assert table_obj.name == "users"


async def test_table_creation_fake():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()
    await table_creation(metadata_store, s3)


async def test_table_creation_sqlite():
    metadata_store = SqliteMetadata("sqlite:///:memory:")
    s3 = FakeS3()
    await table_creation(metadata_store, s3)
