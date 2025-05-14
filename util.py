from contextlib import contextmanager
from time import perf_counter


@contextmanager
def timer(msg: str | None = None):
    s = perf_counter()
    yield
    e = perf_counter()
    print(f"{msg or 'Time taken'}: {(e - s) * 1000} ms")
