"""
Test script for the new register_datasets functionality.
Tests registering multiple datasets and querying them with joins.
"""

import polars as pl
from datafusion import SessionContext

from classes import ColumnDefinitions, Database, Schema, Table
from metadata import FakeMetadataStore
from ops.insert import insert
from s3 import FakeS3, TableRegistration


async def test_single_table_registration():
    """Test registering a single table to ensure basic functionality works."""

    metadata_store = FakeMetadataStore()
    s3 = FakeS3()

    # Create database and schema
    database = await metadata_store.create_database(Database(id=0, name="test_db"))
    schema = await metadata_store.create_schema(
        Schema(id=0, name="test_schema", database_id=database.id)
    )

    # Create users table
    users_table = await metadata_store.create_table(
        Table(
            id=0,
            name="users",
            schema_id=schema.id,
            database_id=database.id,
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="name", type="String"),
                ColumnDefinitions(name="email", type="String"),
            ],
        )
    )

    # Insert test data
    users_data = pl.DataFrame(
        [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
    )
    await insert(users_table, s3, metadata_store, users_data)

    # Test single registration
    ctx = SessionContext()
    await s3.register_dataset(ctx, "users", users_table, metadata_store)

    result = ctx.sql("SELECT * FROM users ORDER BY id").to_polars()

    assert result.columns == ["id", "name", "email"]
    assert len(result) == 2
    assert result.to_dicts()[0]["name"] == "Alice"


async def test_register_datasets_separately():
    """Test registering two tables separately to see if they interfere."""

    metadata_store = FakeMetadataStore()
    s3 = FakeS3()

    # Create database and schema
    database = await metadata_store.create_database(Database(id=0, name="test_db"))
    schema = await metadata_store.create_schema(
        Schema(id=0, name="test_schema", database_id=database.id)
    )

    # Create users table
    users_table = await metadata_store.create_table(
        Table(
            id=0,
            name="users",
            schema_id=schema.id,
            database_id=database.id,
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="name", type="String"),
                ColumnDefinitions(name="email", type="String"),
            ],
        )
    )

    # Create orders table
    orders_table = await metadata_store.create_table(
        Table(
            id=0,
            name="orders",
            schema_id=schema.id,
            database_id=database.id,
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="user_id", type="Int64"),
                ColumnDefinitions(name="product", type="String"),
                ColumnDefinitions(name="amount", type="Float64"),
            ],
        )
    )

    # Insert test data
    users_data = pl.DataFrame(
        [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
    )
    await insert(users_table, s3, metadata_store, users_data)

    orders_data = pl.DataFrame(
        [
            {"id": 101, "user_id": 1, "product": "Laptop", "amount": 999.99},
            {"id": 102, "user_id": 2, "product": "Mouse", "amount": 29.99},
        ]
    )
    await insert(orders_table, s3, metadata_store, orders_data)

    # Register tables separately
    ctx = SessionContext()
    await s3.register_dataset(ctx, "users", users_table, metadata_store)
    await s3.register_dataset(ctx, "orders", orders_table, metadata_store)

    # Check each table individually
    users_result = ctx.sql("SELECT * FROM users ORDER BY id").to_polars()
    orders_result = ctx.sql("SELECT * FROM orders ORDER BY id").to_polars()

    assert users_result.columns == ["id", "name", "email"]
    assert orders_result.columns == ["id", "user_id", "product", "amount"]


async def test_register_datasets_with_join():
    """Test registering multiple datasets and performing a join query."""

    # Set up metadata store and S3
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()

    # Create database and schema
    database = await metadata_store.create_database(Database(id=0, name="test_db"))
    schema = await metadata_store.create_schema(
        Schema(id=0, name="test_schema", database_id=database.id)
    )

    # Create users table
    users_table = await metadata_store.create_table(
        Table(
            id=0,
            name="users",
            schema_id=schema.id,
            database_id=database.id,
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="name", type="String"),
                ColumnDefinitions(name="email", type="String"),
            ],
        )
    )

    # Create orders table
    orders_table = await metadata_store.create_table(
        Table(
            id=0,
            name="orders",
            schema_id=schema.id,
            database_id=database.id,
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="user_id", type="Int64"),
                ColumnDefinitions(name="product", type="String"),
                ColumnDefinitions(name="amount", type="Float64"),
            ],
        )
    )

    # Insert test data into users table
    users_data = pl.DataFrame(
        [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
        ]
    )
    await insert(users_table, s3, metadata_store, users_data)

    # Insert test data into orders table
    orders_data = pl.DataFrame(
        [
            {"id": 101, "user_id": 1, "product": "Laptop", "amount": 999.99},
            {"id": 102, "user_id": 2, "product": "Mouse", "amount": 29.99},
            {"id": 103, "user_id": 1, "product": "Keyboard", "amount": 79.99},
            {"id": 104, "user_id": 3, "product": "Monitor", "amount": 299.99},
            {"id": 105, "user_id": 2, "product": "Headphones", "amount": 149.99},
        ]
    )
    await insert(orders_table, s3, metadata_store, orders_data)

    # Create registrations for both tables
    registrations = [
        TableRegistration(
            table=users_table,
            table_name="users",
        ),
        TableRegistration(
            table=orders_table,
            table_name="orders",
        ),
    ]

    # Register both datasets using the new method
    ctx = SessionContext()
    registered_names = await s3.register_datasets(ctx, registrations, metadata_store)

    # Verify the registration mapping
    assert registered_names == {"users": "users", "orders": "orders"}

    # Test the join query
    query = """
    SELECT 
        u.name,
        u.email,
        o.product,
        o.amount
    FROM orders o
    LEFT JOIN users u ON u.id = o.user_id
    ORDER BY o.id
    """

    result = ctx.sql(query).to_polars()

    # Verify the results
    expected_results = [
        {
            "name": "Alice",
            "email": "alice@example.com",
            "product": "Laptop",
            "amount": 999.99,
        },
        {
            "name": "Bob",
            "email": "bob@example.com",
            "product": "Mouse",
            "amount": 29.99,
        },
        {
            "name": "Alice",
            "email": "alice@example.com",
            "product": "Keyboard",
            "amount": 79.99,
        },
        {
            "name": "Charlie",
            "email": "charlie@example.com",
            "product": "Monitor",
            "amount": 299.99,
        },
        {
            "name": "Bob",
            "email": "bob@example.com",
            "product": "Headphones",
            "amount": 149.99,
        },
    ]

    actual_results = result.to_dicts()
    assert len(actual_results) == 5

    for i, expected in enumerate(expected_results):
        actual = actual_results[i]
        assert actual["name"] == expected["name"]
        assert actual["email"] == expected["email"]
        assert actual["product"] == expected["product"]
        assert abs(actual["amount"] - expected["amount"]) < 0.01  # Float comparison


async def test_register_datasets_with_custom_names():
    """Test registering datasets with custom table names."""

    metadata_store = FakeMetadataStore()
    s3 = FakeS3()

    # Create database and schema
    database = await metadata_store.create_database(Database(id=0, name="test_db"))
    schema = await metadata_store.create_schema(
        Schema(id=0, name="test_schema", database_id=database.id)
    )

    # Create a simple table
    table = await metadata_store.create_table(
        Table(
            id=0,
            name="products",
            schema_id=schema.id,
            database_id=database.id,
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="name", type="String"),
                ColumnDefinitions(name="price", type="Float64"),
            ],
        )
    )

    # Insert test data
    data = pl.DataFrame(
        [
            {"id": 1, "name": "Widget A", "price": 10.99},
            {"id": 2, "name": "Widget B", "price": 15.99},
        ]
    )
    await insert(table, s3, metadata_store, data)

    # Register with custom names
    registrations = [
        TableRegistration(
            table=table,
            table_name="products_v1",
        ),
        TableRegistration(
            table=table,
            table_name="products_v2",
        ),
    ]

    ctx = SessionContext()
    registered_names = await s3.register_datasets(ctx, registrations, metadata_store)

    # Verify both registrations point to the same source table
    assert registered_names == {
        "products": "products_v2"
    }  # Last one wins in the mapping

    # Test that both table names work in queries
    result1 = ctx.sql("SELECT COUNT(*) as count FROM products_v1").to_polars()
    result2 = ctx.sql("SELECT COUNT(*) as count FROM products_v2").to_polars()

    assert result1.to_dicts()[0]["count"] == 2
    assert result2.to_dicts()[0]["count"] == 2
