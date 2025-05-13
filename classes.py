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
        data = base64.b64decode(self.data)
        buffer = io.BytesIO(data)

        # Read the parquet file metadata
        parquet_file = pq.ParquetFile(buffer)
        metadata = parquet_file.metadata

        # Print column statistics for each row group
        cols = {}
        for i in range(metadata.num_row_groups):
            row_group_metadata = metadata.row_group(i)
            for j in range(row_group_metadata.num_columns):
                col_metadata = row_group_metadata.column(j)
                name = col_metadata.path_in_schema
                if name not in cols:
                    cols[name] = ColumnStatistics(
                        name=name, index=j, min=None, max=None, null_count=0
                    )

                stats = col_metadata.statistics
                if stats:
                    if cols[name].min is not None:
                        cols[name].min = min(stats.min, cols[name].min)
                    else:
                        cols[name].min = stats.min

                    if cols[name].max is None:
                        cols[name].max = stats.max
                    else:
                        cols[name].max = max(stats.max, cols[name].max)
                    cols[name].null_count += stats.null_count

        cols = list(cols.values())
        cols.sort(key=lambda x: x.index)

        print(
            f"Size for {self.id}: {metadata.serialized_size} vs buffer len of {len(data)} w/ {metadata.num_row_groups} row groups"
        )

        return Statistics(
            id=self.id,
            rows=metadata.num_rows,
            filesize=metadata.serialized_size,
            columns=cols,
        )

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
