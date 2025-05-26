import asyncio
import os
import tempfile
import unittest

from sqlalchemy import text
from sqlalchemy.orm import Session

from classes import Database, Schema, Table
from sqlite_metadata import SqliteMetadata


class TestSqliteMetadata(unittest.TestCase):
    def setUp(self):
        # Create a temporary SQLite database
        self.temp_db_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.temp_db_file.close()
        self.connection_string = f"sqlite:///{self.temp_db_file.name}"

        # Initialize metadata store
        self.metadata = SqliteMetadata(self.connection_string)

        # Create test database, schema, and table
        self.database = asyncio.run(
            self.metadata.create_database(Database(id=0, name="test_db"))
        )
        self.schema = asyncio.run(
            self.metadata.create_schema(
                Schema(id=0, name="test_schema", database_id=self.database.id)
            )
        )
        self.table = await self.metadata.create_table(
            Table(
                id=0,
                name="test_table",
                schema_id=self.schema.id,
                database_id=self.database.id,
                columns=[],
            )
        )

    def tearDown(self):
        # Clean up the temporary database file
        if os.path.exists(self.temp_db_file.name):
            os.unlink(self.temp_db_file.name)

    def test_reserve_micropartition_ids(self):
        # Test initial reservation when no IDs exist
        ids_1 = self.metadata.reserve_micropartition_ids(self.table, 5)
        self.assertEqual(len(ids_1), 5)
        self.assertEqual(ids_1, [1, 2, 3, 4, 5])

        # Test subsequent reservation
        ids_2 = self.metadata.reserve_micropartition_ids(self.table, 3)
        self.assertEqual(len(ids_2), 3)
        self.assertEqual(ids_2, [6, 7, 8])

        # Test reserving a single ID
        ids_3 = self.metadata.reserve_micropartition_ids(self.table, 1)
        self.assertEqual(len(ids_3), 1)
        self.assertEqual(ids_3, [9])

        # Test reserving a larger batch
        ids_4 = self.metadata.reserve_micropartition_ids(self.table, 10)
        self.assertEqual(len(ids_4), 10)
        self.assertEqual(ids_4, list(range(10, 20)))

        # Verify no actual rows were created in the micropartitions table
        with Session(self.metadata.engine) as session:
            result = session.execute(
                text("SELECT COUNT(*) FROM micro_partitions")
            ).scalar()
            self.assertEqual(result, 0)

            # Verify the sequence was properly updated
            result = session.execute(
                text("SELECT seq FROM sqlite_sequence WHERE name = 'micro_partitions'")
            ).scalar()
            self.assertEqual(result, 19)


if __name__ == "__main__":
    unittest.main()
