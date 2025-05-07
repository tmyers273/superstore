import base64
import gzip
import json
from typing import Generator, Protocol
from pydantic import BaseModel
import polars as pl


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

    def dump(self) -> list[dict]:
        values = []
        data = base64.b64decode(self.data)
        for s, e in self.header.byte_ranges:
            v = gzip.decompress(data[s : e + 1])
            values.append(json.loads(v))

        out = []
        for i in range(len(values[0])):
            row = {}
            for j in range(len(self.header.columns)):
                row[self.header.columns[j]] = values[j][i]
            out.append(row)

        return out


class ColumnDefinitions(BaseModel):
    name: str
    type: str
    type: str


class Table(BaseModel):
    name: str
    columns: list[ColumnDefinitions]


class MetadataStore(Protocol):
    def get_table_version(self, table: Table) -> int:
        raise NotImplementedError

    def add_micro_partition(
        self, table: Table, version: int, micro_partition: MicroPartition
    ):
        raise NotImplementedError

    def get_new_micropartition_id(self, table: Table) -> int:
        """
        Returns a new, unused micropartition id.
        """
        raise NotImplementedError


class FakeMetadataStore(MetadataStore):
    def __init__(self) -> None:
        self.table_versions: dict[str, int] = {}
        self.micro_partitions: dict[str, list[dict]] = {}
        self.micropartition_ids: dict[str, int] = {}

    def get_table_version(self, table: Table) -> int:
        if table.name not in self.table_versions:
            self.table_versions[table.name] = 0

        return self.table_versions[table.name]

    def add_micro_partition(
        self, table: Table, current_version: int, micro_partition: MicroPartition
    ):
        if self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        if table.name not in self.micro_partitions:
            self.micro_partitions[table.name] = []

        self.micro_partitions[table.name].append(
            {
                "id": micro_partition.id,
                "header": micro_partition.header,
            }
        )

        self.table_versions[table.name] = current_version + 1

    def get_new_micropartition_id(self, table: Table) -> int:
        if table.name not in self.micropartition_ids:
            self.micropartition_ids[table.name] = 0
            return 0

        self.micropartition_ids[table.name] += 1
        return self.micropartition_ids[table.name]

    def micropartitions(
        self, table: Table, s3: S3Like
    ) -> Generator[MicroPartition, None, None]:
        if table.name not in self.micro_partitions:
            return

        for metadata in self.micro_partitions[table.name]:
            micro_partition_raw = s3.get_object("bucket", f"{metadata['id']}")
            if micro_partition_raw is None:
                raise ValueError(f"Micro partition `{metadata['id']}` not found")
            micro_partition = json.loads(micro_partition_raw)

            yield MicroPartition(
                id=metadata["id"],
                header=metadata["header"],
                data=micro_partition["data"],
            )

    def all(self, table: Table, s3: S3Like) -> list[dict]:
        out: list[dict] = []
        for metadata in self.micropartitions(table, s3):
            out.extend(metadata.dump())

        return out


class Metadata:
    pass


def insert(table: Table, s3: S3Like, metadata_store: MetadataStore, items: list[dict]):
    # Get the current table version number
    current_version = metadata_store.get_table_version(table)

    # Build the data blob and byte ranges
    data = b""
    offset = 0
    ranges = []
    for col in table.columns:
        values = [item[col.name] for item in items]
        value = bytes(json.dumps(values), "utf-8")
        chunk = gzip.compress(value)
        ranges.append((offset, offset + len(chunk) - 1))
        data += chunk
        offset += len(chunk)

    # Create a new micro partition
    id = metadata_store.get_new_micropartition_id(table)
    micro_partition = MicroPartition(
        id=id,
        header=Header(
            columns=[col.name for col in table.columns],
            types=[col.type for col in table.columns],
            byte_ranges=ranges,
        ),
        data=base64.b64encode(data),
    )

    # Try saving to S3
    s3.put_object("bucket", str(id), micro_partition.model_dump_json())

    # Update metadata
    metadata_store.add_micro_partition(table, current_version, micro_partition)


def test_simple_insert():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()

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
    df = pl.DataFrame(users)

    insert(table, s3, metadata_store, users)

    # Expect the table version to be incremented
    assert metadata_store.get_table_version(table) == 1

    for p in metadata_store.micropartitions(table, s3):
        assert p.dump() == users

    users.append({"id": 4, "name": "Bill Doe", "email": "bill.doe@example.com"})
    users.append({"id": 5, "name": "Bill Smith", "email": "bill.smith@example.com"})
    insert(table, s3, metadata_store, users[3:])

    assert metadata_store.get_table_version(table) == 2

    for i, p in enumerate(metadata_store.micropartitions(table, s3)):
        if i == 0:
            expected = users[:3]
        elif i == 1:
            expected = users[3:]
        else:
            raise ValueError("Unexpected micro partition")

        assert p.dump() == expected, (
            f"Mismatch in micro partition #{p.id}\n\nExp: {expected}\n\nGot: {p.dump()}\n\n"
        )

    assert metadata_store.all(table, s3) == users
