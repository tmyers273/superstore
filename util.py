import os
from time import perf_counter


class timer:
    def __init__(self, msg: str | None = None):
        self.msg = msg
        self.duration_ms = 0

    def __enter__(self):
        self.s = perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        e = perf_counter()
        self.duration_ms = int(round((e - self.s) * 1000, 0))
        print(f"{self.msg or 'Time taken'}: {self.duration_ms} ms")


def env(key: str) -> str:
    """
    Keys the specified key from the env, throwing an exception
    if it does not exist.
    """
    val = os.getenv(key)
    if val is None:
        raise Exception(f"Env variable `{key}` is not set")
    return val
