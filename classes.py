import io

import polars as pl
from pydantic import BaseModel


class Header(BaseModel):
    table_id: int


class Column(BaseModel):
    data: bytes


class ColumnStatistics[T](BaseModel):
    index: int
    name: str
    min: T
    max: T
    null_count: int
    unique_count: int


class Statistics(BaseModel):
    id: int
    rows: int
    filesize: int
    columns: list[ColumnStatistics]

    def dump(self):
        print("Rows: ", self.rows)
        print("Filesize: ", self.filesize)
        for col in self.columns:
            print(f"  {col.name}({col.min} - {col.max}, nulls = {col.null_count})")


class MicroPartition(BaseModel):
    id: int
    header: Header
    data: bytes

    def statistics(self) -> Statistics:
        buffer = io.BytesIO(self.data)
        buffer.seek(0)
        df = pl.read_parquet(buffer)

        # Define the statistics we want to compute for each column
        stats_operations = {
            "cardinality": lambda col: pl.col(col).n_unique(),
            "min": lambda col: pl.col(col).min(),
            "max": lambda col: pl.col(col).max(),
            "null_count": lambda col: pl.col(col).null_count(),
        }

        # Generate all statistics expressions
        stats_exprs = []
        for col in df.columns:
            for stat_name, operation in stats_operations.items():
                stats_exprs.append(operation(col).alias(f"{col}_{stat_name}"))

        # Compute all statistics in a single pass
        stats = df.select(stats_exprs).to_dicts()[0]

        # Create ColumnStatistics objects
        cols = []
        for i, col in enumerate(df.columns):
            cols.append(
                ColumnStatistics(
                    name=col,
                    index=i,
                    min=stats[f"{col}_min"],
                    max=stats[f"{col}_max"],
                    null_count=stats[f"{col}_null_count"],
                    unique_count=stats[f"{col}_cardinality"],
                )
            )

        return Statistics(
            id=self.id,
            rows=df.height,
            filesize=len(self.data),
            columns=cols,
        )

    def dump(self) -> pl.DataFrame:
        buffer = io.BytesIO(self.data)

        # Reset buffer position and read the dataframe
        buffer.seek(0)
        return pl.read_parquet(buffer)


class ColumnDefinitions(BaseModel):
    name: str
    type: str


class Database(BaseModel):
    id: int
    name: str


class Schema(BaseModel):
    id: int
    database_id: int
    name: str


class Table(BaseModel):
    id: int
    schema_id: int
    database_id: int
    name: str
    columns: list[ColumnDefinitions]
