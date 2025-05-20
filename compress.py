import io

import polars as pl


def compress(
    df: pl.DataFrame, max_file_size: int = 16 * 1024 * 1024, tolerance: float = 0.20
) -> list[io.BytesIO]:
    """
    Splits the given dataframe into one or more parquet files,
    aiming to keep the size of each file as close to `max_file_size`
    (in bytes) as possible, without going over.

    The passed `tolerance` is used to determine the lower bound
    to create an acceptable range for the new file.

    Defaults:
    - max_file_size: 16MB
    - tolerance: 20%, giving a default range of 12.8 to 16MB
    """
    parts = []

    min = int(round(max_file_size * (1 - tolerance), 0))
    max = max_file_size

    # Use the first 10k as a guess
    test_df = df.slice(0, 10_000)
    test_buffer = _compress_part(test_df)

    records: list[tuple[int, int]] = [
        (test_buffer.tell(), test_df.height),
    ]

    while len(df) > 0:
        avg_bytes_per_row = (
            sum(r[0] for r in records) / sum(r[1] for r in records) if records else None
        )

        buffer, rows = _ratio_based_compress(df, min, max, avg_bytes_per_row)
        parts.append(buffer)
        df = df.slice(rows)

        records.append((buffer.tell(), rows))
        # print(f"    {buffer.tell() / 1024 / 1024:.2f}MB, {rows:,} rows")

    # print(f"Build {len(parts)} parts from {len(df)} rows")
    # for r in records:
    #     print(f"    {r[0] / 1024 / 1024:.2f}MB, {r[1]:,} rows")

    return parts


def _ratio_based_compress(
    df: pl.DataFrame, min_size: int, max_size: int, bytes_per_row: float | None = None
) -> tuple[io.BytesIO, int]:
    """
    Compresses the dataframe using compression ratio estimation.

    Returns a tuple containing:
    - BytesIO buffer with the compressed data
    - Number of rows used from the original dataframe
    - Updated bytes per row estimate

    If even a single row exceeds max size, returns an empty buffer with 0 rows.
    """
    buffer = io.BytesIO()

    # Check if even a single row exceeds max
    if len(df) == 0:
        raise ValueError("No rows to compress")

    if bytes_per_row is None:
        test_buffer = _compress_part(df.slice(0, 1))
        single_row_size = test_buffer.tell()
        bytes_per_row = single_row_size

    # Make an initial estimate based on bytes_per_row
    estimated_rows = min(len(df), int(max_size / bytes_per_row))

    # We'll need at least one row
    estimated_rows = max(1, estimated_rows)

    # Start with our estimate, then adjust if needed
    rows = estimated_rows
    i = 0
    max_iterations = 20  # Add a maximum iteration count
    while i < max_iterations:  # Add a termination condition
        test_slice = df.slice(0, rows)
        _compress_part(test_slice, buffer)
        size = buffer.tell()

        # Update our bytes_per_row estimate
        new_bytes_per_row = size / rows if rows > 0 else 0

        if size <= max_size:
            # This slice works
            if size >= min_size or rows == len(df):
                # Size is within our tolerance range or we've used all rows
                return buffer, rows

            # Try to add more rows based on our refined estimate
            additional_rows = int((max_size - size) / new_bytes_per_row * 1.2)
            if additional_rows == 0:
                # Can't add more rows within tolerance
                return buffer, rows

            rows = min(len(df), rows + additional_rows)
        else:
            # Too big, scale back
            rows_to_remove = int((size - max_size) / new_bytes_per_row * 1.2) + 1
            rows = max(1, rows - rows_to_remove)

            # If we're only using one row and it's still too big, something's wrong
            if rows == 1:
                return buffer, 1

        i += 1

    # If we exit the loop due to max iterations, return the current buffer
    return buffer, rows


def _compress_part(df: pl.DataFrame, buffer: io.BytesIO | None = None) -> io.BytesIO:
    """
    Compresses the given dataframe into a parquet file.
    """
    if buffer is None:
        buffer = io.BytesIO()
    else:
        buffer.seek(0)
        buffer.truncate(0)

    df.write_parquet(buffer)
    return buffer


# def test_compress():
#     items = []
#     for i in range(1_000_000):
#         items.append(
#             {
#                 "id": i,
#                 "name": f"name_{i}",
#                 "email": f"email_{i}@example.com",
#             }
#         )

#     df = pl.DataFrame(items)

#     json_buf = io.BytesIO()
#     json_buf.write(json.dumps(items).encode())
#     # print(f"\nFull JSON size:   {json_buf.tell() / 1024 / 1024:.2f}MB")

#     buf = io.BytesIO()
#     df.write_parquet(buf)
#     # print(f"Full Parquet size: {buf.tell() / 1024 / 1024:.2f}MB")

#     max_file_size = 200_000
#     start = perf_counter()
#     parts = compress(df, max_file_size)
#     end = perf_counter()
#     # print(
#     #     f"Compressed into {len(parts)} parts, max size {max_file_size} bytes. Took {(end - start) * 1000:.0f}ms"
#     # )
#     # for i, part in enumerate(parts):
#     #     print(f"    Part {i + 1} size: {part.tell()} bytes")


# test_compress()
