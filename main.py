from typing import Protocol
from pydantic import BaseModel


class S3Like(Protocol):
    def get_object(self, bucket: str, key: str) -> bytes | None:
        raise NotImplementedError

    def put_object(self, bucket: str, key: str, body: bytes):
        raise NotImplementedError


class FakeS3(S3Like):
    def __init__(self):
        self.objects = {}

    def get_object(self, bucket: str, key: str) -> bytes | None:
        if bucket not in self.objects:
            return None

        return self.objects[bucket].get(key)

    def put_object(self, bucket: str, key: str, body: bytes):
        if bucket not in self.objects:
            self.objects[bucket] = {}

        self.objects[bucket][key] = body


class MetadataStore(Protocol):
    pass


class FakeMetadataStore(MetadataStore):
    def __init__(self):
        self.metadata = {}


class Header(BaseModel):
    columns: list[str]
    types: list[str]
    byte_ranges: list[tuple[int, int]]


class Column(BaseModel):
    data: bytes


class MicroPartition(BaseModel):
    header: Header
    columns: list[Column]


class ColumnDefinitions(BaseModel):
    name: str
    type: str


class Table(BaseModel):
    name: str
    columns: list[ColumnDefinitions]


class Metadata:
    pass


class MicroPartitionMetadata(Metadata):
    pass


def insert(table: Table, s3: S3Like, metadata_store: MetadataStore, items: list[dict]):
    cols = {}
    for col in table.columns:
        values = [item[col.name] for item in items]

    # Create a new micro partition
    micro_partition = MicroPartition(
        header=Header(columns=table.columns, types=table.columns, byte_ranges=[]),
        columns=[],
    )


def test_it():
    table = Table(
        name="users",
        columns=[
            ColumnDefinitions(name="id", type="i64"),
            ColumnDefinitions(name="name", type="string"),
            ColumnDefinitions(name="email", type="string"),
        ],
    )

    users = [
        {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"},
        {"id": 3, "name": "John Smith", "email": "john.smith@example.com"},
    ]

    insert(table, FakeS3(), FakeMetadataStore(), users)


print("Hello, World!")
