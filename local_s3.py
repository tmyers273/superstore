import os

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
