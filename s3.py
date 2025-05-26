import os
import shutil
import tempfile
import weakref
from typing import Protocol

import pyarrow.dataset as ds
from datafusion import SessionContext


class S3Like(Protocol):
    def get_object(self, bucket: str, key: str) -> bytes | None:
        raise NotImplementedError

    def put_object(self, bucket: str, key: str, body: bytes):
        raise NotImplementedError

    def register_dataset(
        self,
        ctx: SessionContext,
        table_name: str,
        table,
        metadata_store,
        version: int | None = None,
        with_data: bool = True,
        included_mp_ids: set[int] | None = None,
        paths: list[str] | None = None,
    ):
        """Register a dataset with the given SessionContext for querying."""
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

    def register_dataset(
        self,
        ctx: SessionContext,
        table_name: str,
        table,
        metadata_store,
        version: int | None = None,
        with_data: bool = True,
        included_mp_ids: set[int] | None = None,
        paths: list[str] | None = None,
    ):
        """Register a dataset by writing micropartitions to a temporary directory."""
        # Create a temporary directory that will be cleaned up when the context manager exits
        tmpdir = tempfile.mkdtemp()

        for p in metadata_store.micropartitions(
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
