"""
Fake data generation for development environment.

This module provides functionality to create realistic test data for development
and testing purposes. It only operates when APP_ENV=dev.
"""

import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict

import polars as pl

from classes import ColumnDefinitions, Database, Schema, Table
from local_s3 import LocalS3
from metadata import MetadataStore
from tests.run_test import insert


def generate_fake_users_data(count: int = 1000) -> pl.DataFrame:
    """Generate fake users data"""
    random.seed(42)  # For reproducible data

    first_names = [
        "Alice",
        "Bob",
        "Charlie",
        "Diana",
        "Eve",
        "Frank",
        "Grace",
        "Henry",
        "Ivy",
        "Jack",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
    ]
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "company.com", "example.org"]

    data = []
    for i in range(count):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        data.append(
            {
                "id": i + 1,
                "first_name": first_name,
                "last_name": last_name,
                "email": f"{first_name.lower()}.{last_name.lower()}{i}@{random.choice(domains)}",
                "age": random.randint(18, 80),
                "department": random.choice(
                    ["Engineering", "Marketing", "Sales", "HR", "Finance"]
                ),
                "salary": random.randint(40000, 150000),
                "created_at": datetime.now() - timedelta(days=random.randint(0, 365)),
                "is_active": random.choice([True, False]),
            }
        )

    return pl.DataFrame(data)


def generate_fake_orders_data(count: int = 5000) -> pl.DataFrame:
    """Generate fake orders data"""
    random.seed(43)  # Different seed for variety

    products = [
        "Laptop",
        "Phone",
        "Tablet",
        "Headphones",
        "Monitor",
        "Keyboard",
        "Mouse",
        "Webcam",
    ]
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]

    data = []
    for i in range(count):
        order_date = datetime.now() - timedelta(days=random.randint(0, 180))
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10.0, 2000.0), 2)
        total_amount = round(quantity * unit_price, 2)

        data.append(
            {
                "id": i + 1,
                "user_id": random.randint(1, 1000),  # Reference to users table
                "product_name": random.choice(products),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "order_date": order_date,
                "status": random.choice(statuses),
                "shipping_address": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm'])} St",
            }
        )

    return pl.DataFrame(data)


def generate_fake_analytics_data(count: int = 10000) -> pl.DataFrame:
    """Generate fake analytics/metrics data"""
    random.seed(44)

    data = []
    base_date = datetime.now() - timedelta(days=90)

    for i in range(count):
        event_date = base_date + timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        data.append(
            {
                "id": i + 1,
                "event_date": event_date,
                "user_id": random.randint(1, 1000),
                "page_path": random.choice(
                    [
                        "/home",
                        "/products",
                        "/about",
                        "/contact",
                        "/checkout",
                        "/profile",
                    ]
                ),
                "session_duration": random.randint(30, 3600),  # seconds
                "page_views": random.randint(1, 20),
                "bounce_rate": round(random.uniform(0.1, 0.9), 2),
                "conversion": random.choice([True, False]),
                "device_type": random.choice(["desktop", "mobile", "tablet"]),
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "country": random.choice(["US", "UK", "CA", "DE", "FR", "JP", "AU"]),
            }
        )

    return pl.DataFrame(data)


def generate_fake_products_data(count: int = 500) -> pl.DataFrame:
    """Generate fake products data"""
    random.seed(45)

    categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys"]
    brands = ["TechCorp", "StyleBrand", "BookHouse", "HomePlus", "SportMax", "ToyWorld"]

    data = []
    for i in range(count):
        category = random.choice(categories)
        data.append(
            {
                "id": i + 1,
                "name": f"Product {i + 1}",
                "description": f"High quality {category.lower()} product with excellent features",
                "category": category,
                "brand": random.choice(brands),
                "price": round(random.uniform(5.0, 1000.0), 2),
                "cost": round(random.uniform(2.0, 500.0), 2),
                "stock_quantity": random.randint(0, 1000),
                "weight": round(random.uniform(0.1, 50.0), 2),
                "dimensions": f"{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(5, 50)}",
                "is_featured": random.choice([True, False]),
                "created_at": datetime.now() - timedelta(days=random.randint(0, 730)),
            }
        )

    return pl.DataFrame(data)


def get_fake_tables_config():
    """Get configuration for all fake tables"""
    return [
        {
            "name": "fake_users",
            "columns": [
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="first_name", type="String"),
                ColumnDefinitions(name="last_name", type="String"),
                ColumnDefinitions(name="email", type="String"),
                ColumnDefinitions(name="age", type="Int32"),
                ColumnDefinitions(name="department", type="String"),
                ColumnDefinitions(name="salary", type="Int64"),
                ColumnDefinitions(name="created_at", type="Timestamp"),
                ColumnDefinitions(name="is_active", type="Boolean"),
            ],
            "partition_keys": ["department"],
            "sort_keys": ["id"],
            "data_generator": generate_fake_users_data,
            "data_count": 1000,
        },
        {
            "name": "fake_orders",
            "columns": [
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="user_id", type="Int64"),
                ColumnDefinitions(name="product_name", type="String"),
                ColumnDefinitions(name="quantity", type="Int32"),
                ColumnDefinitions(name="unit_price", type="Float64"),
                ColumnDefinitions(name="total_amount", type="Float64"),
                ColumnDefinitions(name="order_date", type="Timestamp"),
                ColumnDefinitions(name="status", type="String"),
                ColumnDefinitions(name="shipping_address", type="String"),
            ],
            "partition_keys": ["status"],
            "sort_keys": ["order_date", "id"],
            "data_generator": generate_fake_orders_data,
            "data_count": 5000,
        },
        {
            "name": "fake_analytics",
            "columns": [
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="event_date", type="Timestamp"),
                ColumnDefinitions(name="user_id", type="Int64"),
                ColumnDefinitions(name="page_path", type="String"),
                ColumnDefinitions(name="session_duration", type="Int32"),
                ColumnDefinitions(name="page_views", type="Int32"),
                ColumnDefinitions(name="bounce_rate", type="Float64"),
                ColumnDefinitions(name="conversion", type="Boolean"),
                ColumnDefinitions(name="device_type", type="String"),
                ColumnDefinitions(name="browser", type="String"),
                ColumnDefinitions(name="country", type="String"),
            ],
            "partition_keys": ["country", "device_type"],
            "sort_keys": ["event_date"],
            "data_generator": generate_fake_analytics_data,
            "data_count": 10000,
        },
        {
            "name": "fake_products",
            "columns": [
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="name", type="String"),
                ColumnDefinitions(name="description", type="String"),
                ColumnDefinitions(name="category", type="String"),
                ColumnDefinitions(name="brand", type="String"),
                ColumnDefinitions(name="price", type="Float64"),
                ColumnDefinitions(name="cost", type="Float64"),
                ColumnDefinitions(name="stock_quantity", type="Int32"),
                ColumnDefinitions(name="weight", type="Float64"),
                ColumnDefinitions(name="dimensions", type="String"),
                ColumnDefinitions(name="is_featured", type="Boolean"),
                ColumnDefinitions(name="created_at", type="Timestamp"),
            ],
            "partition_keys": ["category"],
            "sort_keys": ["id"],
            "data_generator": generate_fake_products_data,
            "data_count": 500,
        },
    ]


def get_fake_table_names():
    """Get list of all fake table names"""
    return [config["name"] for config in get_fake_tables_config()]


def check_fake_tables_exist(metadata: MetadataStore) -> bool:
    """Check if any fake tables already exist"""
    fake_table_names = get_fake_table_names()
    for table_name in fake_table_names:
        if metadata.get_table(table_name) is not None:
            return True
    return False


def create_fake_tables_and_data(metadata: MetadataStore, data_dir: str):
    """Create fake tables and data for development environment"""
    if os.getenv("APP_ENV") != "dev":
        print("Not in dev environment, skipping fake data creation")
        return

    # Check if fake tables already exist
    if check_fake_tables_exist(metadata):
        print("Fake tables already exist, skipping fake data creation")
        return

    print("Creating fake tables and data for development...")

    # Create development database and schema
    dev_db = metadata.get_database("development")
    if dev_db is None:
        dev_db = metadata.create_database(Database(id=0, name="development"))

    dev_schema = metadata.get_schema("dev_schema")
    if dev_schema is None:
        dev_schema = metadata.create_schema(
            Schema(id=0, name="dev_schema", database_id=dev_db.id)
        )

    # Get fake tables configuration
    fake_tables_config = get_fake_tables_config()

    # Create tables and insert data
    for table_config in fake_tables_config:
        # Check if table already exists (double-check)
        existing_table = metadata.get_table(table_config["name"])
        if existing_table is not None:
            print(f"Table {table_config['name']} already exists, skipping...")
            continue

        # Create table
        table = Table(
            id=0,
            name=table_config["name"],
            schema_id=dev_schema.id,
            database_id=dev_db.id,
            columns=table_config["columns"],
            partition_keys=table_config.get("partition_keys", []),
            sort_keys=table_config.get("sort_keys", []),
        )

        created_table = metadata.create_table(table)
        print(f"Created table: {created_table.name}")

        # Generate and insert fake data
        print(
            f"Generating {table_config['data_count']} rows of fake data for {table_config['name']}..."
        )
        fake_data = table_config["data_generator"](table_config["data_count"])

        # Create S3 path for this table
        table_s3_path = f"{data_dir}/{table_config['name']}/mps"
        table_s3 = LocalS3(table_s3_path)

        # Insert the data
        insert(created_table, table_s3, metadata, fake_data)
        print(f"Inserted {len(fake_data)} rows into {table_config['name']}")

    print("Fake tables and data creation completed!")


def reset_fake_data(metadata: MetadataStore, data_dir: str):
    """Reset fake data by dropping and recreating tables"""
    if os.getenv("APP_ENV") != "dev":
        raise ValueError("This function is only available in development environment")

    fake_table_names = get_fake_table_names()

    dropped_tables = []
    for table_name in fake_table_names:
        table = metadata.get_table(table_name)
        if table is not None:
            metadata.drop_table(table)
            dropped_tables.append(table_name)
            print(f"Dropped table: {table_name}")

    # Recreate the fake data
    create_fake_tables_and_data(metadata, data_dir)

    return {"dropped_tables": dropped_tables, "recreated_tables": fake_table_names}


def get_fake_data_status(metadata: MetadataStore, data_dir: str) -> Dict[str, Any]:
    """Get status of all fake data tables"""
    if os.getenv("APP_ENV") != "dev":
        raise ValueError("This function is only available in development environment")

    fake_table_names = get_fake_table_names()

    status: Dict[str, Any] = {"app_env": os.getenv("APP_ENV"), "tables": {}}

    for table_name in fake_table_names:
        table = metadata.get_table(table_name)
        if table is not None:
            # Get table statistics
            try:
                table_s3_path = f"{data_dir}/{table_name}/mps"
                table_s3 = LocalS3(table_s3_path)

                total_rows = 0
                total_filesize = 0
                mp_count = 0

                for mp in metadata.micropartitions(table, table_s3, with_data=False):
                    mp_count += 1
                    total_rows += mp.stats.rows
                    total_filesize += mp.stats.filesize

                status["tables"][table_name] = {
                    "exists": True,
                    "table_id": table.id,
                    "schema_id": table.schema_id,
                    "database_id": table.database_id,
                    "status": table.status.value,
                    "partition_keys": table.partition_keys,
                    "sort_keys": table.sort_keys,
                    "total_rows": total_rows,
                    "total_filesize": total_filesize,
                    "micropartitions": mp_count,
                    "columns": len(table.columns),
                }
            except Exception as e:
                status["tables"][table_name] = {
                    "exists": True,
                    "error": f"Failed to get statistics: {str(e)}",
                }
        else:
            status["tables"][table_name] = {"exists": False}

    return status
