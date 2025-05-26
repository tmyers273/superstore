#!/usr/bin/env python3
"""
Test script for fake data generation functionality.
This script can be used to test the fake data generation without running the full server.
"""

import os
import tempfile

# Set up test environment
os.environ["APP_ENV"] = "dev"
os.environ["DATA_DIR"] = tempfile.mkdtemp()
os.environ["USER_PASSWORD"] = "test123"

print(f"Test DATA_DIR: {os.environ['DATA_DIR']}")

# Import after setting environment variables
from fake_data import (
    create_fake_tables_and_data,
    generate_fake_analytics_data,
    generate_fake_orders_data,
    generate_fake_products_data,
    generate_fake_users_data,
)
from local_s3 import LocalS3
from sqlite_metadata import SqliteMetadata


def test_data_generators():
    """Test individual data generators"""
    print("Testing data generators...")

    # Test users data
    users_df = generate_fake_users_data(10)
    print(f"Generated {len(users_df)} users")
    print("Users columns:", users_df.columns)
    print("Sample user:", users_df.to_dicts()[0])
    print()

    # Test orders data
    orders_df = generate_fake_orders_data(20)
    print(f"Generated {len(orders_df)} orders")
    print("Orders columns:", orders_df.columns)
    print("Sample order:", orders_df.to_dicts()[0])
    print()

    # Test analytics data
    analytics_df = generate_fake_analytics_data(15)
    print(f"Generated {len(analytics_df)} analytics events")
    print("Analytics columns:", analytics_df.columns)
    print("Sample analytics:", analytics_df.to_dicts()[0])
    print()

    # Test products data
    products_df = generate_fake_products_data(5)
    print(f"Generated {len(products_df)} products")
    print("Products columns:", products_df.columns)
    print("Sample product:", products_df.to_dicts()[0])
    print()


def test_table_creation():
    """Test full table creation and data insertion"""
    print("Testing table creation and data insertion...")

    # Initialize metadata store
    data_dir = os.environ["DATA_DIR"]
    db_path = f"sqlite:///{data_dir}/test.db"

    metadata = SqliteMetadata(db_path)

    try:
        create_fake_tables_and_data(metadata, data_dir)
        print("✅ Fake tables and data created successfully!")

        # Verify tables were created
        tables = metadata.get_tables()
        print(f"Total tables created: {len(tables)}")

        for table in tables:
            if table.name.startswith("fake_"):
                print(
                    f"  - {table.name} (ID: {table.id}, Columns: {len(table.columns)})"
                )

                # Try to get some statistics
                try:
                    table_s3_path = f"{data_dir}/{table.name}/mps"
                    table_s3 = LocalS3(table_s3_path)

                    total_rows = 0
                    mp_count = 0
                    for mp in metadata.micropartitions(
                        table, table_s3, with_data=False
                    ):
                        mp_count += 1
                        total_rows += mp.stats.rows

                    print(f"    Rows: {total_rows}, Micropartitions: {mp_count}")
                except Exception as e:
                    print(f"    Error getting stats: {e}")

    except Exception as e:
        print(f"❌ Error creating fake data: {e}")
        import traceback

        traceback.print_exc()


def main():
    """Run all tests"""
    print("🧪 Testing Fake Data Generation")
    print("=" * 50)

    test_data_generators()
    print("-" * 50)
    test_table_creation()

    print("\n✨ Test completed!")
    print(f"Test data directory: {os.environ['DATA_DIR']}")
    print("You can inspect the generated files in the above directory.")


if __name__ == "__main__":
    main()
