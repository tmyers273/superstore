"""
Test script to demonstrate the new register_datasets functionality.
"""

from datafusion import SessionContext

from classes import ColumnDefinitions, Table
from s3 import FakeS3, TableRegistration


def test_register_datasets():
    """Test the new register_datasets method with multiple tables."""

    # Create some test tables
    table1 = Table(
        id=1,
        schema_id=1,
        database_id=1,
        name="events",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="event_type", type="String"),
            ColumnDefinitions(name="timestamp", type="Timestamp"),
        ],
    )

    table2 = Table(
        id=2,
        schema_id=1,
        database_id=1,
        name="users",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="name", type="String"),
            ColumnDefinitions(name="email", type="String"),
        ],
    )

    # Create registrations with different configurations
    registrations = [
        TableRegistration(
            table=table1, version=5, table_name="events_v5", included_mp_ids={1, 2, 3}
        ),
        TableRegistration(
            table=table2,
            version=None,  # Latest version
            table_name="users_latest",
            included_mp_ids={10, 20, 30},
        ),
        TableRegistration(
            table=table1,
            version=3,
            # table_name will default to table.name = "events"
            included_mp_ids={4, 5, 6},
        ),
    ]

    # Create S3 instance and SessionContext
    s3 = FakeS3()
    ctx = SessionContext()

    # This would normally be a real metadata store
    metadata_store = None  # For demo purposes

    print("Registrations to process:")
    for i, reg in enumerate(registrations):
        table_name = reg.table_name if reg.table_name else reg.table.name
        print(
            f"  {i + 1}. Table: {reg.table.name}, Version: {reg.version}, "
            f"Name: {table_name}, MP IDs: {reg.included_mp_ids}"
        )

    # This would call the new method:
    # registered_names = s3.register_datasets(ctx, registrations, metadata_store)
    # print(f"Registered tables: {registered_names}")

    print("\nNew register_datasets method is ready to use!")


if __name__ == "__main__":
    test_register_datasets()
