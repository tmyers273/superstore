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
)
from local_s3 import LocalS3
from sqlite_metadata import SqliteMetadata


async def test_table_creation():
    """Test full table creation and data insertion"""
    print("Testing table creation and data insertion...")

    # Initialize metadata store
    data_dir = os.environ["DATA_DIR"]
    db_path = f"sqlite:///{data_dir}/db.db"

    metadata = SqliteMetadata(db_path)

    try:
        create_fake_tables_and_data(metadata, data_dir)
        print("✅ Fake tables and data created successfully!")

        # Verify tables were created
        tables = await metadata.get_tables()
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
                    async for mp in await metadata.micropartitions(
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
