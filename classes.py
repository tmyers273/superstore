from pydantic import BaseModel
import polars as pl
import base64
import io
import pyarrow.parquet as pq


class Header(BaseModel):
    table_id: int


class Column(BaseModel):
    data: bytes


class MicroPartition(BaseModel):
    id: int
    header: Header
    data: bytes

    def statistics(self) -> list[dict]:
        data = base64.b64decode(self.data)
        buffer = io.BytesIO(data)

        # Read the parquet file metadata
        parquet_file = pq.ParquetFile(buffer)
        metadata = parquet_file.metadata

        # Print metadata and statistics
        print("\nParquet Metadata:")
        print(f"Number of rows: {metadata.num_rows}")
        print(f"Number of row groups: {metadata.num_row_groups}")
        print(f"Created by: {metadata.created_by}")
        print(f"Schema: {metadata.schema}")

        # Print column statistics for each row group
        for i in range(metadata.num_row_groups):
            print(f"\nRow Group {i} Statistics:")
            row_group_metadata = metadata.row_group(i)
            for j in range(row_group_metadata.num_columns):
                col_metadata = row_group_metadata.column(j)
                stats = col_metadata.statistics
                if stats:
                    print(f"\nColumn: {col_metadata.path_in_schema}")
                    print(f"  Min: {stats.min}")
                    print(f"  Max: {stats.max}")
                    print(f"  Null count: {stats.null_count}")

        return []

    def dump(self) -> pl.DataFrame:
        data = base64.b64decode(self.data)
        buffer = io.BytesIO(data)

        # Reset buffer position and read the dataframe
        buffer.seek(0)
        return pl.read_parquet(buffer)


class ColumnDefinitions(BaseModel):
    name: str
    type: str


class Table(BaseModel):
    id: int
    name: str
    columns: list[ColumnDefinitions]
