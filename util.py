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
