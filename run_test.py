import base64
from contextlib import contextmanager
from copy import deepcopy
import gzip
import io
import json
import os
import tempfile
from typing import Generator, Protocol
from datafusion import SessionContext
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

    def dump(self) -> pl.DataFrame:
        data = base64.b64decode(self.data)
        return pl.read_parquet(io.BytesIO(data))


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
        self.old_versions: dict[str, dict[int, list[dict]]] = {}
        self.micropartition_ids: dict[str, int] = {}

    def get_table_version(self, table: Table) -> int:
        if table.name not in self.table_versions:
            self.table_versions[table.name] = 0

        return self.table_versions[table.name]

    def __archive_version(self, table: Table):
        if table.name not in self.old_versions:
            self.old_versions[table.name] = {}

        current_version = self.get_table_version(table)
        if current_version in self.old_versions[table.name]:
            raise ValueError("Version already archived")

        self.old_versions[table.name][current_version] = deepcopy(
            self.micro_partitions.get(table.name, [])
        )

    def add_micro_partition(
        self, table: Table, current_version: int, micro_partition: MicroPartition
    ):
        if self.get_table_version(table) != current_version:
            raise ValueError("Version mismatch")

        self.__archive_version(table)

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
        self, table: Table, s3: S3Like, version: int | None = None
    ) -> Generator[MicroPartition, None, None]:
        """
        A generator that loops through all the micro partitions for a table.
        """

        if version is None:
            micropartitions = self.micro_partitions[table.name]
            if table.name not in self.micro_partitions:
                return
        else:
            if table.name not in self.old_versions:
                raise ValueError(f"Table {table.name} has no archived versions")
            if version not in self.old_versions[table.name]:
                raise ValueError(f"Version {version} not found")

            micropartitions = self.old_versions[table.name][version]

        for metadata in micropartitions:
            micro_partition_raw = s3.get_object("bucket", f"{metadata['id']}")
            if micro_partition_raw is None:
                raise ValueError(f"Micro partition `{metadata['id']}` not found")
            micro_partition = json.loads(micro_partition_raw)

            yield MicroPartition(
                id=metadata["id"],
                header=metadata["header"],
                data=micro_partition["data"],
            )

    def all(self, table: Table, s3: S3Like) -> pl.DataFrame | None:
        """
        Returns a dataframe containing all the data for the table.
        """
        out: pl.DataFrame | None = None
        for metadata in self.micropartitions(table, s3):
            if out is None:
                out = metadata.dump()
            else:
                out = pl.concat([out, metadata.dump()])

        return out


class Metadata:
    pass


def insert(
    table: Table, s3: S3Like, metadata_store: MetadataStore, items: pl.DataFrame
):
    # TODO: validate schema

    # Get the current table version number
    current_version = metadata_store.get_table_version(table)

    # Build the data blob and byte ranges
    buffer = io.BytesIO()
    items.write_parquet(buffer)
    buffer.seek(0)

    # data = b""
    # offset = 0
    # ranges = []
    # for col in table.columns:
    #     values = [item[col.name] for item in items]
    #     value = bytes(json.dumps(values), "utf-8")
    #     chunk = gzip.compress(value)
    #     ranges.append((offset, offset + len(chunk) - 1))
    #     data += chunk
    #     offset += len(chunk)

    # Create a new micro partition
    id = metadata_store.get_new_micropartition_id(table)
    micro_partition = MicroPartition(
        id=id,
        header=Header(
            columns=[col.name for col in table.columns],
            types=[col.type for col in table.columns],
            byte_ranges=[],
        ),
        data=base64.b64encode(buffer.getvalue()),
    )

    # Try saving to S3
    s3.put_object("bucket", str(id), micro_partition.model_dump_json().encode("utf-8"))

    # Update metadata
    metadata_store.add_micro_partition(table, current_version, micro_partition)


@contextmanager
def build_table(
    table: Table, metadata_store: MetadataStore, s3: S3Like, version: int | None = None
):
    ctx = SessionContext()
    with tempfile.TemporaryDirectory() as tmpdir:
        for p in metadata_store.micropartitions(table, s3, version=version):
            path = os.path.join(tmpdir, f"{p.id}.parquet")
            p.dump().write_parquet(path)

        ctx.register_parquet("users", tmpdir)
        yield ctx


def test_simple_insert():
    metadata_store = FakeMetadataStore()
    s3 = FakeS3()

    table = Table(
        name="users",
        columns=[
            ColumnDefinitions(name="id", type="Int64"),
            ColumnDefinitions(name="name", type="String"),
            ColumnDefinitions(name="email", type="String"),
        ],
    )

    users = [
        {"id": 1, "name": "John Doe", "email": "john.doe@example.com"},
        {"id": 2, "name": "Jane Doe", "email": "jane.doe@example.com"},
        {"id": 3, "name": "John Smith", "email": "john.smith@example.com"},
    ]
    df = pl.DataFrame(users)

    insert(table, s3, metadata_store, df)

    # Expect the table version to be incremented
    assert metadata_store.get_table_version(table) == 1

    for p in metadata_store.micropartitions(table, s3):
        assert p.dump().to_dicts() == users

    users.append({"id": 4, "name": "Bill Doe", "email": "bill.doe@example.com"})
    users.append({"id": 5, "name": "Bill Smith", "email": "bill.smith@example.com"})
    df = pl.DataFrame(users[3:])
    insert(table, s3, metadata_store, df)

    assert metadata_store.get_table_version(table) == 2

    for i, p in enumerate(metadata_store.micropartitions(table, s3)):
        if i == 0:
            expected = users[:3]
        elif i == 1:
            expected = users[3:]
        else:
            raise ValueError("Unexpected micro partition")

        dump = p.dump()
        assert dump.to_dicts() == expected, (
            f"Mismatch in micro partition #{p.id}\n\nExp: {expected}\n\nGot: {dump}\n\n"
        )

    assert metadata_store.all(table, s3).to_dicts() == users

    # Dump in mem parquet files to tmp storage
    with build_table(table, metadata_store, s3, version=1) as ctx:
        df = ctx.sql("SELECT sum(id) FROM users")
        print(df.to_polars())
