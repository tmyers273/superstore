import os

from s3 import S3Like


class LocalS3(S3Like):
    def __init__(self, path: str):
        self.path = path

    def get_object(self, bucket: str, key: str) -> bytes | None:
        return open(os.path.join(self.path, bucket, key + ".parquet"), "rb").read()

    def put_object(self, bucket: str, key: str, data: bytes):
        # Create the full path including the bucket directory
        full_path = os.path.join(self.path, bucket)
        # Create the directory if it doesn't exist
        os.makedirs(full_path, exist_ok=True)
        # Write the file
        with open(os.path.join(full_path, key + ".parquet"), "wb") as f:
            f.write(data)
