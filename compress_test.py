import io
import json
from time import perf_counter
import polars as pl


def compress(
    df: pl.DataFrame, max_file_size: int = 16 * 1024 * 1024, tolerance: float = 0.10
) -> list[io.BytesIO]:
    """
    Splits the given dataframe into one or more parquet files,
    aiming to keep the size of each file as close to `max_file_size`
    (in bytes) as possible, without going over.

    The passed `tolerance` is used to determine the lower bound
    to create an acceptable range for the new file.

    Defaults:
    - max_file_size: 16MB
    - tolerance: 10%, giving a default range of 14.4 to 16MB
    """
    parts = []

    min = int(round(max_file_size * (1 - tolerance), 0))
    max = max_file_size
    start: int | None = None

    while len(df) > 0:
        start_time = perf_counter()
        buffer, rows = guess_and_check(df, min, max, start)
        start = rows
        end_time = perf_counter()
        print(f"Compressed {rows} rows in {(end_time - start_time) * 1000:.0f}ms")
        df = df.slice(rows)

        parts.append(buffer)

    return parts


def guess_and_check(
    df: pl.DataFrame, min: int, max: int, start: int | None = None
) -> tuple[io.BytesIO, int]:
    """
    Guesses the size of the dataframe and checks if it's within the given tolerance.

    Runs a binary search to find an output that is within the given min and max.

    Returns a tuple containing:
    - BytesIO buffer with the compressed data
    - Number of rows used from the original dataframe

    If even a single row exceeds max size, returns an empty buffer with 0 rows.
    """
    low = 1
    high = len(df)

    # If start is passed, we want the mid to be that
    if start is not None:
        low = (start * 2) - high
    print(
        f"BEFORE STARTING: low={low}, high={high}, start={start}, mid={(low + high) // 2}"
    )

    buffer = io.BytesIO()

    # Check if even a single row exceeds max
    if high > 0:
        test_buffer = compress_part(df.slice(0, 1))
        if test_buffer.tell() > max:
            # If a single row is too big, return empty buffer with 0 rows
            return io.BytesIO(), 0

    while low <= high:
        mid = (low + high) // 2
        print(f"    Mid is {mid}, start is {start}, low is {low}, high is {high}")
        test_slice = df.slice(0, mid)
        compress_part(test_slice, buffer)
        size = buffer.tell()

        if size <= max:
            # This slice works, but might not be optimal
            if size >= min:
                # Size is within our tolerance range, return early
                print(f"        good size ({min} <= {size} <= {max}), returning")
                return buffer, mid

            # Try a larger slice
            low = mid + 1
            print(f"        too small ({size} < {min}), trying larger")
        else:
            # This slice is too big, try a smaller one
            high = mid - 1
            print(f"        too big ({size} > {max}), trying smaller")

    # If we couldn't find a size in the acceptable range, return the largest slice under min
    if high > 0:
        print("===========")
        buffer = compress_part(df.slice(0, high))
        return buffer, high

    # Couldn't fit anything
    return io.BytesIO(), 0


def compress_part(df: pl.DataFrame, buffer: io.BytesIO | None = None) -> io.BytesIO:
    """
    Compresses the given dataframe into a parquet file.
    """
    # start = perf_counter()
    if buffer is None:
        buffer = io.BytesIO()
    else:
        buffer.seek(0)
        buffer.truncate(0)

    df.write_parquet(buffer)
    # end = perf_counter()
    # print(f"Compressed {len(df)} rows in {(end - start) * 1000:.0f}ms")
    return buffer


def test_compress():
    items = []
    for i in range(1000_000):
        items.append(
            {
                "id": i,
                "name": f"name_{i}",
                "email": f"email_{i}@example.com",
            }
        )

    df = pl.DataFrame(items)

    json_buf = io.BytesIO()
    json_buf.write(json.dumps(items).encode())
    print(f"\nFull JSON size: {json_buf.tell()} bytes")

    buf = io.BytesIO()
    df.write_parquet(buf)
    print(f"Full Parquet size: {buf.tell()} bytes")

    max_file_size = 200_000
    start = perf_counter()
    parts = compress(df, max_file_size)
    end = perf_counter()
    print(
        f"Compressed into {len(parts)} parts, max size {max_file_size} bytes. Took {(end - start) * 1000:.0f}ms"
    )
    for i, part in enumerate(parts):
        print(f"    Part {i + 1} size: {part.tell()} bytes")


test_compress()
