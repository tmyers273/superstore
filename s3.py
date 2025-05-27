import os
import shutil
import tempfile
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    pass

import pyarrow.dataset as ds
from datafusion import SessionContext


@dataclass
class TableRegistration:
    """Configuration for registering a single table dataset."""

    table: "Table"
    version: int | None = None
    table_name: str | None = None  # If None, use table.name
    included_mp_ids: set[int] | None = None
    paths: list[str] | None = None


class S3Like(Protocol):
    def get_object(self, bucket: str, key: str) -> bytes | None:
        raise NotImplementedError

    def put_object(self, bucket: str, key: str, body: bytes):
        raise NotImplementedError

    async def register_dataset(
        self,
        ctx: SessionContext,
        table_name: str,
        table: "Table",
        metadata_store,
        version: int | None = None,
        with_data: bool = True,
        included_mp_ids: set[int] | None = None,
        paths: list[str] | None = None,
    ):
        """Register a single dataset with the given SessionContext for querying."""
        raise NotImplementedError

    async def register_datasets(
        self,
        ctx: SessionContext,
        registrations: list[TableRegistration],
        metadata_store,
        with_data: bool = True,
    ) -> dict[str, str]:
        """Register multiple datasets with the given SessionContext for querying.

        Args:
            ctx: SessionContext to register datasets with
            registrations: List of TableRegistration objects specifying what to register
            metadata_store: Metadata store for accessing table data
            with_data: Whether to include data in registration (applies to all tables)

        Returns:
            Dictionary mapping table.name -> registered_table_name
        """
        raise NotImplementedError


def _cleanup_temp_dirs(temp_dirs: list[str]) -> None:
    """Clean up temporary directories. This function is called by the finalizer."""
    for tmpdir in temp_dirs:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass  # Ignore cleanup errors


class FakeS3(S3Like):
    def __init__(self):
        self.objects: dict[str, dict[str, bytes]] = {}
        self._temp_dirs: list[str] = []
        # Register a finalizer to clean up temp dirs when this object is garbage collected
        self._finalizer = weakref.finalize(self, _cleanup_temp_dirs, self._temp_dirs)

    def get_object(self, bucket: str, key: str) -> bytes | None:
        if bucket not in self.objects:
            return None

        return self.objects[bucket].get(key)

    def put_object(self, bucket: str, key: str, body: bytes):
        if bucket not in self.objects:
            self.objects[bucket] = {}

        self.objects[bucket][key] = body

    async def register_dataset(
        self,
        ctx: SessionContext,
        table_name: str,
        table: "Table",
        metadata_store: "MetadataStore",
        version: int | None = None,
        with_data: bool = True,
        included_mp_ids: set[int] | None = None,
        paths: list[str] | None = None,
    ):
        """Register a dataset by writing micropartitions to a temporary directory."""
        # Create a temporary directory that will be cleaned up when the context manager exits
        tmpdir = tempfile.mkdtemp()

        async for p in await metadata_store.micropartitions(
            table, self, version=version, with_data=with_data
        ):
            if included_mp_ids is not None and p.id not in included_mp_ids:
                continue
            path = os.path.join(tmpdir, f"{p.id}.parquet")
            p.dump().write_parquet(path)

        dataset = ds.dataset(tmpdir, format="parquet")
        ctx.register_dataset(table_name, dataset)

        # Store the tmpdir so it can be cleaned up automatically by the finalizer
        self._temp_dirs.append(tmpdir)

    async def register_datasets(
        self,
        ctx: SessionContext,
        registrations: list[TableRegistration],
        metadata_store,
        with_data: bool = True,
    ) -> dict[str, str]:
        """Register multiple datasets by calling register_dataset for each."""
        registered_names = {}

        for reg in registrations:
            # Use provided table_name or fall back to table.name
            table_name = (
                reg.table_name if reg.table_name is not None else reg.table.name
            )

            await self.register_dataset(
                ctx=ctx,
                table_name=table_name,
                table=reg.table,
                metadata_store=metadata_store,
                version=reg.version,
                with_data=with_data,
                included_mp_ids=reg.included_mp_ids,
                paths=reg.paths,
            )
            registered_names[reg.table.name] = table_name

        return registered_names
