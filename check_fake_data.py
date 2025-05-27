#!/usr/bin/env python3
"""
Script to check the status of fake data tables.
"""

import asyncio
import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import server configuration
import server
from fake_data import get_fake_data_status


async def main():
    """Check fake data status"""
    print("📊 Checking fake data status...")
    print("=" * 50)

    # Check environment
    app_env = os.getenv("APP_ENV")
    print(f"APP_ENV: {app_env}")

    if app_env != "dev":
        print("❌ Error: APP_ENV must be set to 'dev' to run this script")
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

    try:
        status = await get_fake_data_status(metadata, data_dir)

        print("📋 Fake Data Status Report:")
        print(f"Environment: {status['app_env']}")
        print()

        total_rows = 0
        total_mps = 0
        total_filesize = 0

        for table_name, table_info in status["tables"].items():
            print(f"🗂️  {table_name}:")
            if table_info.get("exists", False):
                if "error" in table_info:
                    print(f"   ❌ Error: {table_info['error']}")
                else:
                    rows = table_info.get("total_rows", 0)
                    mps = table_info.get("micropartitions", 0)
                    filesize = table_info.get("total_filesize", 0)
                    columns = table_info.get("columns", 0)

                    total_rows += rows
                    total_mps += mps
                    total_filesize += filesize

                    print(f"   ✅ Rows: {rows:,}")
                    print(f"   📁 Micropartitions: {mps}")
                    print(f"   💾 File size: {filesize:,} bytes")
                    print(f"   🏛️  Columns: {columns}")
                    print(
                        f"   🔑 Partition keys: {table_info.get('partition_keys', [])}"
                    )
                    print(f"   📊 Sort keys: {table_info.get('sort_keys', [])}")
            else:
                print("   ⚪ Does not exist")
            print()

        print("📈 Summary:")
        print(f"   Total rows: {total_rows:,}")
        print(f"   Total micropartitions: {total_mps}")
        print(
            f"   Total file size: {total_filesize:,} bytes ({total_filesize / 1024 / 1024:.2f} MB)"
        )
        print()
        print("🎉 Fake data is ready for development!")

    except Exception as e:
        print(f"❌ Error getting fake data status: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
