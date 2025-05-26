#!/usr/bin/env python3
"""
Script to migrate database schema and create fake data tables.
This ensures the database schema is up to date before creating fake data.
"""

import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import server configuration
from sqlalchemy import create_engine, text

import server
from db import Base
from fake_data import create_fake_tables_and_data, get_fake_tables_config


def migrate_database():
    """Ensure database schema is up to date"""
    print("🔄 Migrating database schema...")

    # Create synchronous engine for schema operations
    data_dir = server.data_dir
    sync_db_path = f"sqlite:///{data_dir}/db.db"
    sync_engine = create_engine(sync_db_path)

    # Create all tables (this will add missing columns)
    Base.metadata.create_all(sync_engine)

    # Check if status column exists and add it if missing
    with sync_engine.connect() as conn:
        try:
            # Try to query the status column
            result = conn.execute(text("SELECT status FROM tables LIMIT 1"))
            print("✅ Status column exists")
        except Exception as e:
            if "no such column: status" in str(e):
                print("⚠️  Status column missing, adding it...")
                try:
                    # Add the status column with default value
                    conn.execute(
                        text(
                            "ALTER TABLE tables ADD COLUMN status TEXT DEFAULT 'active'"
                        )
                    )
                    conn.commit()
                    print("✅ Status column added successfully")
                except Exception as add_error:
                    print(f"❌ Error adding status column: {add_error}")
                    return False
            else:
                print(f"❌ Error checking status column: {e}")
                return False

    print("✅ Database schema migration completed")
    return True


def main():
    """Migrate database and create fake data tables"""
    print("🚀 Migrating database and creating fake data...")
    print("=" * 60)

    # Check environment
    app_env = os.getenv("APP_ENV")
    print(f"APP_ENV: {app_env}")

    if app_env != "dev":
        print("❌ Error: APP_ENV must be set to 'dev' to run this script")
        print("Set APP_ENV=dev and try again")
        return

    # Use server configuration
    data_dir = server.data_dir
    metadata = server.metadata

    print(f"Data directory: {data_dir}")
    print(f"Database path: {server.db_path}")
    print()

    if data_dir is None:
        print("❌ Error: DATA_DIR is not configured in server")
        return

    # First, migrate the database schema
    if not migrate_database():
        print("❌ Database migration failed, aborting")
        return

    print()

    # Show what will be created
    fake_tables_config = get_fake_tables_config()
    print("Will create the following tables:")
    total_expected_rows = 0
    for config in fake_tables_config:
        print(f"  - {config['name']}: {config['data_count']:,} rows")
        total_expected_rows += config["data_count"]
    print(f"Total expected rows: {total_expected_rows:,}")
    print()

    print("🔄 Creating fake data...")

    try:
        # Create the fake data
        create_fake_tables_and_data(metadata, data_dir)

        print("✅ Fake data creation completed!")
        print("Fake data is ready for development!")

    except Exception as e:
        print(f"❌ Error creating fake data: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
