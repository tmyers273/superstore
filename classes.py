from pydantic import BaseModel
import polars as pl
import base64
import io


class Header(BaseModel):
    columns: list[str]
    types: list[str]
    byte_ranges: list[tuple[int, int]]


class Column(BaseModel):
    data: bytes


class MicroPartition(BaseModel):
    id: int
    header: Header
    data: bytes

    def dump(self) -> pl.DataFrame:
        data = base64.b64decode(self.data)
        return pl.read_parquet(io.BytesIO(data))


class ColumnDefinitions(BaseModel):
    name: str
    type: str


class Table(BaseModel):
    name: str
    columns: list[ColumnDefinitions]
