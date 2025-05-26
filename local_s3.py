import os
from time import perf_counter

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext

from s3 import S3Like


class LocalS3(S3Like):
    def __init__(self, path: str):
        self.path = path

    def get_object(self, bucket: str, key: str) -> bytes | None:
        path = os.path.join(self.path, bucket, key + ".parquet")
        return open(path, "rb").read()

    def put_object(self, bucket: str, key: str, data: bytes):
        # Create the full path including the bucket directory
        # keys can come in prefixed with partition keys (ie "advertiser_id=123/1")
        # We need to pull out the path part so we can create any necessary
        # directories.
        parts = key.split("/")
        key_name = parts[-1]
        key_path = "/".join(parts[:-1])
        full_path = os.path.join(self.path, bucket, key_path)

        # Create the directory if it doesn't exist
        os.makedirs(full_path, exist_ok=True)

        # Write the file
        with open(os.path.join(full_path, key_name + ".parquet"), "wb") as f:
            f.write(data)

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
        """Register a dataset by creating a dataset from parquet files on disk."""
        if paths is None:
            wanted_ids = []
            s = perf_counter()

            if included_mp_ids is None:
                ids = metadata_store._get_ids(table, version)
                for id in ids:
                    if included_mp_ids is not None and id not in included_mp_ids:
                        continue
                    wanted_ids.append(id)
            else:
                wanted_ids = list(included_mp_ids)

            e = perf_counter()
            print(f"    Time to get {len(wanted_ids)} wanted ids: {(e - s) * 1000} ms")

            data_dir = os.getenv("DATA_DIR")
            if data_dir is None:
                raise ValueError("DATA_DIR is not set")
            base_dir = os.path.join(data_dir, table.name, "mps/bucket")

            # Only include files that correspond to wanted micropartition IDs
            paths = []
            for root, _, files in os.walk(base_dir):
                for file in files:
                    if file.endswith(".parquet"):
                        # Extract the micropartition ID from the filename
                        # The filename should be {id}.parquet
                        filename_without_ext = os.path.splitext(file)[0]
                        try:
                            mp_id = int(filename_without_ext)
                            if mp_id in wanted_ids:
                                paths.append(os.path.join(root, file))
                        except ValueError:
                            # Skip files that don't have numeric names
                            continue

        s = perf_counter()
        dataset = ds.dataset(
            paths,
            format="parquet",
            partitioning=ds.partitioning(
                pa.schema([pa.field("advertiser_id", pa.large_string())]),
                flavor="hive",
            ),
        )
        e = perf_counter()
        print(f"    Time to create dataset: {(e - s) * 1000} ms")
        s = perf_counter()
        ctx.register_dataset(table_name, dataset)
        e = perf_counter()
        print(f"    Time to register dataset: {(e - s) * 1000} ms")
