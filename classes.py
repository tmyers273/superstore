from pydantic import BaseModel
import polars as pl
import base64
import io


class Header(BaseModel):
    table_id: int


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
    id: int
    name: str
    columns: list[ColumnDefinitions]
