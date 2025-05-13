from pydantic import BaseModel
import polars as pl
import base64
import io
import pyarrow.parquet as pq


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

        # Open the parquet file
        buffer.seek(0)
        df = pl.read_parquet(buffer)
        cardinality_by_col = df.select(
            [pl.col(col).n_unique() for col in df.columns]
        ).to_dicts()[0]

        card = [
            pl.col(col).n_unique().alias(f"{col}_cardinality") for col in df.columns
        ]
        min = [pl.col(col).min().alias(f"{col}_min") for col in df.columns]
        max = [pl.col(col).max().alias(f"{col}_max") for col in df.columns]
        null_count = [
            pl.col(col).null_count().alias(f"{col}_null_count") for col in df.columns
        ]

        t = df.select(card + min + max + null_count).to_dicts()[0]

        # Turn the DF into a list of ColumnStatistics
        cols = []
        for i, col in enumerate(df.columns):
            cols.append(
                ColumnStatistics(
                    name=col,
                    index=i,
                    min=t[f"{col}_min"],
                    max=t[f"{col}_max"],
                    null_count=t[f"{col}_null_count"],
                    unique_count=t[f"{col}_cardinality"],
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
