import json
import os
import re
import shutil

import polars as pl
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from classes import ColumnDefinitions, Database, Schema, Table
from local_s3 import LocalS3
from metadata import MetadataStore
from sqlite_metadata import SqliteMetadata
from tests.ams_test import get_parquet_files
from tests.audit_log_items_test import create_table_if_needed, get_table
from tests.run_test import build_table, insert

load_dotenv()
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# db_path = "sqlite:///ams_scratch/ams.db"
# s3_path = "ams_scratch/mps"
# table_name = "sp-traffic"

data_dir = os.getenv("DATA_DIR")
db_path = f"sqlite:///{data_dir}/db.db"
s3_path = f"{data_dir}/audit_log_items/mps"
table_name = "audit_log_items"
print(f"data_dir: {data_dir}")
print(f"S3 Path: {s3_path}")

metadata = SqliteMetadata(db_path)
s3 = LocalS3(s3_path)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/table/{table_id}")
async def table(table_id: int):
    print(metadata.get_tables())
    table = metadata.get_table(table_name)
    if table is None:
        return {"error": "Table not found"}

    mps: list[dict] = []
    total_rows = 0
    total_filesize = 0
    micropartitions = 0
    for mp in metadata.micropartitions(table, s3, with_data=False):
        raw = mp.model_dump()
        del raw["data"]
        stats = mp.stats
        total_rows += stats.rows
        total_filesize += stats.filesize
        raw["stats"] = stats.model_dump()
        micropartitions += 1

        if len(mps) < 30:
            mps.append(raw)

    out = table.model_dump()
    out["mps"] = mps
    out["total_rows"] = total_rows
    out["total_filesize"] = total_filesize
    out["micropartitions"] = micropartitions
    return out


@app.get("/ingest/audit-log-items")
async def ingest_audit_log_items(limit: int | None = None):
    files = get_parquet_files("/ingest/audit_log_items")
    print("Found", len(files), "parquet files")

    # Sort in numerical order, not lexicographical
    def extract_number(filename):
        match = re.search(r"audit_log_items_(\d+)-", filename)
        if match:
            return int(match.group(1))
        return 0  # fallback

    files.sort(key=extract_number)

    table = get_table()
    create_table_if_needed(metadata)

    for i, file in enumerate(files):
        if limit is not None and i >= limit:
            break

        print(f"Processing {file} ({i + 1}/{len(files)})")

        df = pl.read_parquet(file)
        df = df.sort(["audit_log_id", "id"])

        insert(table, s3, metadata, df)
        print(f"Inserted {file} with {df.height} rows")
        shutil.move(file, f"/done{file}")

    return {"message": "Ingested audit log items"}


@app.get("/create-table-if-needed/audit-log-items")
async def create_table_if_needed_audit_log_items():
    def get_table() -> Table:
        return Table(
            id=1,
            schema_id=1,
            database_id=1,
            name="audit_log_items",
            columns=[
                ColumnDefinitions(name="id", type="Int64"),
                ColumnDefinitions(name="audit_log_id", type="Int64"),
                ColumnDefinitions(name="target_id", type="Int64"),
                ColumnDefinitions(name="target_type_id", type="Int16"),
                ColumnDefinitions(name="meta", type="String"),
                ColumnDefinitions(name="created_at", type="Timestamp"),
                ColumnDefinitions(name="updated_at", type="Timestamp"),
            ],
        )

    def create_table_if_needed(metadata: MetadataStore) -> Table:
        database = metadata.get_database("db")
        if database is None:
            database = metadata.create_database(Database(id=0, name="db"))

        schema = metadata.get_schema("default")
        if schema is None:
            schema = metadata.create_schema(
                Schema(id=0, name="default", database_id=database.id)
            )

        table = metadata.get_table("audit_log_items")
        if table is None:
            table = get_table()
            table.schema_id = schema.id
            table.database_id = database.id
            metadata.create_table(table)

        table = get_table()
        if metadata.get_table(table.name) is None:
            metadata.create_table(table)

        return table

    create_table_if_needed(metadata)
    return {"message": "Table created"}


@app.get("/audit-log-items")
async def audit_log_items(audit_log_id: int, page: int = 1, per_page: int = 15):
    table = metadata.get_table(table_name)
    if table is None:
        return {"error": "Table not found"}

    with build_table(
        table, metadata, s3, table_name="audit_log_items", with_data=False
    ) as ctx:
        total = ctx.sql(
            f"""
            SELECT count(*) FROM 'audit_log_items'
            WHERE audit_log_id = {audit_log_id}
        """
        ).to_polars()
        total = list(total.to_dicts()[0].values())[0]

        df = ctx.sql(
            f"""
            SELECT * FROM 'audit_log_items'
            WHERE audit_log_id = {audit_log_id}
            ORDER BY id
            LIMIT {per_page}
            OFFSET {(page - 1) * per_page}
        """
        )
        df = df.to_polars()

    data = df.to_dicts()
    for row in data:
        row["meta"] = json.loads(row["meta"])

    return {
        "data": data,
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@app.get("/databases")
async def databases():
    databases = metadata.get_databases()
    schemas = metadata.get_schemas()
    tables = metadata.get_tables()

    # Create a nested structure using list comprehensions
    dbs = [
        {
            **db.model_dump(),
            "schemas": [
                {
                    **schema.model_dump(),
                    "tables": [
                        table.model_dump()
                        for table in tables
                        if table.schema_id == schema.id and table.database_id == db.id
                    ],
                }
                for schema in schemas
                if schema.database_id == db.id
            ],
        }
        for db in databases
    ]

    return {"databases": dbs}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
