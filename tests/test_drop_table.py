import os
import tempfile

import pytest

from classes import ColumnDefinitions, Database, Schema, Table, TableStatus
from metadata import FakeMetadataStore
from sqlite_metadata import SqliteMetadata


class TestDropTable:
    """Test suite for table drop functionality."""

    async def test_drop_table_sqlite(self):
        """Test dropping a table with SqliteMetadata"""
        # Create a temporary SQLite database
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as temp_db:
            temp_db.close()
            connection_string = f"sqlite:///{temp_db.name}"

            try:
                metadata = SqliteMetadata(connection_string)

                # Create test database, schema, and table
                database = await metadata.create_database(
                    Database(id=0, name="test_db")
                )
                schema = await metadata.create_schema(
                    Schema(id=0, name="test_schema", database_id=database.id)
                )
                table = metadata.create_table(
                    Table(
                        id=0,
                        name="test_table",
                        schema_id=schema.id,
                        database_id=database.id,
                        columns=[ColumnDefinitions(name="id", type="Int64")],
                    )
                )

                # Verify table is active and visible
                assert table.status == TableStatus.ACTIVE
                tables = metadata.get_tables()
                assert len(tables) == 1
                assert tables[0].name == "test_table"
                assert tables[0].status == TableStatus.ACTIVE

                found_table = metadata.get_table("test_table")
                assert found_table is not None
                assert found_table.status == TableStatus.ACTIVE

                # Drop the table
                dropped_table = metadata.drop_table(table)
                assert dropped_table.status == TableStatus.DROPPED

                # Verify table is no longer visible in normal queries
                tables = metadata.get_tables()
                assert len(tables) == 0

                found_table = metadata.get_table("test_table")
                assert found_table is None

                # Verify table is still visible when including dropped tables
                tables_with_dropped = metadata.get_tables(include_dropped=True)
                assert len(tables_with_dropped) == 1
                assert tables_with_dropped[0].status == TableStatus.DROPPED

                found_dropped_table = metadata.get_table(
                    "test_table", include_dropped=True
                )
                assert found_dropped_table is not None
                assert found_dropped_table.status == TableStatus.DROPPED

            finally:
                # Clean up
                os.unlink(temp_db.name)

    async def test_drop_table_fake(self):
        """Test dropping a table with FakeMetadataStore"""
        metadata = FakeMetadataStore()

        # Create test database, schema, and table
        database = await metadata.create_database(Database(id=0, name="test_db"))
        schema = await metadata.create_schema(
            Schema(id=0, name="test_schema", database_id=database.id)
        )
        table = metadata.create_table(
            Table(
                id=0,
                name="test_table",
                schema_id=schema.id,
                database_id=database.id,
                columns=[ColumnDefinitions(name="id", type="Int64")],
            )
        )

        # Verify table is active and visible
        assert table.status == TableStatus.ACTIVE
        tables = metadata.get_tables()
        assert len(tables) == 1
        assert tables[0].name == "test_table"
        assert tables[0].status == TableStatus.ACTIVE

        found_table = metadata.get_table("test_table")
        assert found_table is not None
        assert found_table.status == TableStatus.ACTIVE

        # Drop the table
        dropped_table = metadata.drop_table(table)
        assert dropped_table.status == TableStatus.DROPPED

        # Verify table is no longer visible in normal queries
        tables = metadata.get_tables()
        assert len(tables) == 0

        found_table = metadata.get_table("test_table")
        assert found_table is None

        # Verify table is still visible when including dropped tables
        tables_with_dropped = metadata.get_tables(include_dropped=True)
        assert len(tables_with_dropped) == 1
        assert tables_with_dropped[0].status == TableStatus.DROPPED

        found_dropped_table = metadata.get_table("test_table", include_dropped=True)
        assert found_dropped_table is not None
        assert found_dropped_table.status == TableStatus.DROPPED

    def test_drop_nonexistent_table_sqlite(self):
        """Test dropping a non-existent table raises an error"""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as temp_db:
            temp_db.close()
            connection_string = f"sqlite:///{temp_db.name}"

            try:
                metadata = SqliteMetadata(connection_string)

                # Create a table object that doesn't exist in the database
                fake_table = Table(
                    id=999,
                    name="nonexistent_table",
                    schema_id=1,
                    database_id=1,
                    columns=[],
                )

                # Dropping should raise an error
                with pytest.raises(ValueError, match="Table with id 999 not found"):
                    metadata.drop_table(fake_table)

            finally:
                os.unlink(temp_db.name)

    def test_drop_nonexistent_table_fake(self):
        """Test dropping a non-existent table raises an error with FakeMetadataStore"""
        metadata = FakeMetadataStore()

        # Create a table object that doesn't exist in the store
        fake_table = Table(
            id=999,
            name="nonexistent_table",
            schema_id=1,
            database_id=1,
            columns=[],
        )

        # Dropping should raise an error
        with pytest.raises(ValueError, match="Table nonexistent_table not found"):
            metadata.drop_table(fake_table)

    async def test_multiple_tables_drop_behavior(self):
        """Test behavior with multiple tables where some are dropped"""
        metadata = FakeMetadataStore()

        # Create test database and schema
        database = await metadata.create_database(Database(id=0, name="test_db"))
        schema = await metadata.create_schema(
            Schema(id=0, name="test_schema", database_id=database.id)
        )

        # Create multiple tables
        tables = []
        for i in range(3):
            table = metadata.create_table(
                Table(
                    id=0,
                    name=f"table_{i + 1}",
                    schema_id=schema.id,
                    database_id=database.id,
                    columns=[ColumnDefinitions(name="id", type="Int64")],
                )
            )
            tables.append(table)

        # Verify all tables are active
        active_tables = metadata.get_tables()
        assert len(active_tables) == 3
        for table in active_tables:
            assert table.status == TableStatus.ACTIVE

        # Drop the middle table
        metadata.drop_table(tables[1])

        # Verify only 2 tables are active
        active_tables = metadata.get_tables()
        assert len(active_tables) == 2
        active_names = {t.name for t in active_tables}
        assert active_names == {"table_1", "table_3"}

        # Verify all 3 tables exist when including dropped
        all_tables = metadata.get_tables(include_dropped=True)
        assert len(all_tables) == 3

        # Verify the dropped table has correct status
        dropped_table = metadata.get_table("table_2", include_dropped=True)
        assert dropped_table is not None
        assert dropped_table.status == TableStatus.DROPPED

    def test_table_status_enum_values(self):
        """Test that the TableStatus enum has the expected values"""
        assert TableStatus.ACTIVE == "active"
        assert TableStatus.DROPPED == "dropped"

        # Test that we can create tables with explicit status
        table = Table(
            id=1,
            name="test",
            schema_id=1,
            database_id=1,
            columns=[],
            status=TableStatus.ACTIVE,
        )
        assert table.status == TableStatus.ACTIVE

        # Test that default status is ACTIVE
        table_default = Table(
            id=1,
            name="test",
            schema_id=1,
            database_id=1,
            columns=[],
        )
        assert table_default.status == TableStatus.ACTIVE
