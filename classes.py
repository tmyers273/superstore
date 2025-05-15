import datetime
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

    @classmethod
    def from_bytes(cls, buffer: io.BytesIO | bytes) -> "Statistics":
        match buffer:
            case io.BytesIO():
                buffer.seek(0)
                df = pl.read_parquet(buffer)
                length = buffer.seek(0, io.SEEK_END)
            case bytes():
                df = pl.read_parquet(buffer)
                length = len(buffer)
            case _:
                raise ValueError(f"Invalid buffer type: {type(buffer)}")

        stats = cls.from_df(df)
        stats.filesize = length

        return stats

    @classmethod
    def from_df(cls, df: pl.DataFrame) -> "Statistics":
        # Define the statistics we want to compute for each column
        stats_operations = {
            "cardinality": lambda col: pl.col(col).n_unique(),
            "null_count": lambda col: pl.col(col).null_count(),
        }

        # Generate all statistics expressions
        stats_exprs = []
        for col in df.columns:
            # Add all basic statistics
            for stat_name, operation in stats_operations.items():
                stats_exprs.append(operation(col).alias(f"{col}_{stat_name}"))

            # Handle min/max based on column type
            col_dtype = df.schema[col]
            if str(col_dtype).startswith("struct"):
                # For struct columns, use null values
                stats_exprs.append(pl.lit(None).alias(f"{col}_min"))
                stats_exprs.append(pl.lit(None).alias(f"{col}_max"))
            else:
                # For non-struct columns, use regular min/max
                stats_exprs.append(pl.col(col).min().alias(f"{col}_min"))
                stats_exprs.append(pl.col(col).max().alias(f"{col}_max"))

        # Compute all statistics in a single pass
        stats = df.select(stats_exprs).to_dicts()[0]

        # Create ColumnStatistics objects
        cols = []
        for i, col in enumerate(df.columns):
            min = stats[f"{col}_min"]
            max = stats[f"{col}_max"]
            match min:
                case datetime.date():
                    min = str(min)
                case _:
                    pass
            match max:
                case datetime.date():
                    max = str(max)
                case _:
                    pass

            cols.append(
                ColumnStatistics(
                    name=col,
                    index=i,
                    min=min,
                    max=max,
                    null_count=stats[f"{col}_null_count"],
                    unique_count=stats[f"{col}_cardinality"],
                )
            )

        return Statistics(
            id=0,
            rows=df.height,
            filesize=0,
            columns=cols,
        )

    def dump(self):
        print("Rows: ", self.rows)
        print("Filesize: ", self.filesize)
        for col in self.columns:
            print(f"  {col.name}({col.min} - {col.max}, nulls = {col.null_count})")


class MicroPartition(BaseModel):
    id: int
    header: Header
    data: bytes | None
    stats: Statistics

    def dump(self) -> pl.DataFrame:
        if self.data is None:
            return pl.DataFrame()
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
