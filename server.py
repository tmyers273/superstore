import json
import math
import os
import re
import shutil
from contextlib import asynccontextmanager
from time import perf_counter

import polars as pl
import uvicorn
from datafusion import SessionContext
from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import select

from classes import ColumnDefinitions, Database, Schema, Table
from create_user import create_user
from db import Base, User, engine
from fake_data import create_fake_tables_and_data, get_fake_data_status, reset_fake_data
from local_s3 import LocalS3
from metadata import MetadataStore
from s3 import TableRegistration
from sqlite_metadata import SqliteMetadata
from tests import ams_test
from tests.ams_test import get_parquet_files
from tests.audit_log_items_test import create_table_if_needed, get_table
from tests.run_test import build_table, insert
from users import auth_backend, current_active_user, fastapi_users
from util import env


async def create_db_and_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        r = await conn.execute(select(User).where(User.email == "tmyers273@gmail.com"))
        user = r.fetchall()
        if user:
            print("User already exists")
            print(user)
        else:
            print("User does not exist, creating user")
            await create_user("tmyers273@gmail.com", env("USER_PASSWORD"))

    # Create fake data if in development environment and tables don't exist
    if data_dir is not None:
        create_fake_tables_and_data(metadata, data_dir)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_db_and_tables()
    yield


load_dotenv()
app = FastAPI(lifespan=lifespan)

app.include_router(
    fastapi_users.get_auth_router(auth_backend), prefix="/auth/jwt", tags=["auth"]
)


# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# db_path = "sqlite:///ams_scratch/ams.db"
# s3_path = "ams_scratch/mps"
# table_name = "sp-traffic"

data_dir = os.getenv("DATA_DIR")
db_path = f"sqlite:///{data_dir}/db.db"
# db_path = f"sqlite:///{data_dir}/db.db"
s3_path = f"{data_dir}/audit_log_items/mps"
table_name = "audit_log_items"
print(f"data_dir: {data_dir}")
print(f"S3 Path: {s3_path}")

metadata = SqliteMetadata(db_path)
s3 = LocalS3(s3_path)


@app.get("/")
async def root():
    return {"message": "Hello World"}


class LoginRequest(BaseModel):
    email: str
    password: str


@app.get("/auth/me")
async def authenticated_route(user: User = Depends(current_active_user)):
    # DO NOT DELETE
    user.name = "Demo User"
    return user


@app.post("/auth/login")
async def login(request: LoginRequest):
    print(request)
    return {"user": {"id": 1, "name": "Demo User", "email": request.email}}


@app.delete("/table/{table_id}")
async def drop_table(table_id: int, user: User = Depends(current_active_user)):
    table = metadata.get_table_by_id(table_id)
    if table is None:
        return {"error": "Table not found"}

    try:
        dropped_table = metadata.drop_table(table)
        return {
            "message": f"Table '{table.name}' has been dropped",
            "table": dropped_table.model_dump(),
        }
    except ValueError as e:
        return {"error": str(e)}


def sort_column_to_key(sortColumn: str):
    if sortColumn == "id":
        return lambda x: x["id"]
    elif sortColumn == "rows":
        return lambda x: x["stats"]["rows"]
    elif sortColumn == "filesize":
        return lambda x: x["stats"]["filesize"]
    else:
        raise ValueError(f"Invalid sort column: {sortColumn}")


@app.get("/table/{table_id}")
async def table(
    table_id: int,
    page: int = 1,
    pageSize: int = 30,
    sortColumn: str = "id",
    sortDirection: str = "asc",
    user: User = Depends(current_active_user),
):
    print(metadata.get_tables())
    table = metadata.get_table_by_id(table_id)
    if table is None:
        return {"error": "Table not found"}

    # Create the correct S3 path for this specific table
    table_s3_path = f"{data_dir}/{table.name}/mps"
    table_s3 = LocalS3(table_s3_path)

    total_rows = 0
    total_filesize = 0
    all_mps: list[dict] = []
    for i, mp in enumerate(metadata.micropartitions(table, table_s3, with_data=False)):
        raw = mp.model_dump()
        del raw["data"]
        stats = mp.stats
        total_rows += stats.rows
        total_filesize += stats.filesize
        raw["stats"] = stats.model_dump()
        all_mps.append(raw)

    all_mps.sort(key=sort_column_to_key(sortColumn), reverse=sortDirection == "desc")
    skip = (page - 1) * pageSize
    mps = all_mps[skip : skip + pageSize]

    out = table.model_dump()
    out["mps"] = mps
    out["total_rows"] = total_rows
    out["total_filesize"] = total_filesize
    out["micropartitions"] = len(all_mps)

    out["total_pages"] = math.ceil(len(all_mps) / pageSize)
    out["current_page"] = page

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


@app.get("/max-audit-log-items")
async def audit_log_items_max():
    table = metadata.get_table(table_name)
    if table is None:
        return {"error": "Table not found"}

    with build_table(
        table, metadata, s3, table_name="audit_log_items", with_data=False
    ) as ctx:
        max_audit_log_id = ctx.sql(
            "SELECT max(audit_log_id) FROM 'audit_log_items'"
        ).to_polars()
        max_audit_log_id = list(max_audit_log_id.to_dicts()[0].values())[0]

    return {"max_audit_log_id": max_audit_log_id}


@app.get("/total-audit-log-items")
async def audit_log_items_total():
    table = metadata.get_table(table_name)
    if table is None:
        return {"error": "Table not found"}

    with build_table(
        table, metadata, s3, table_name="audit_log_items", with_data=False
    ) as ctx:
        total = ctx.sql("SELECT count(*) FROM 'audit_log_items'").to_polars()
        total = list(total.to_dicts()[0].values())[0]

    return {"total": total}


class ExecuteRequest(BaseModel):
    table_names: list[str] | str
    """A list of table names or a single table name string. If string with commas, will be split."""
    query: str
    page: int = 1
    per_page: int = 30


@app.post("/execute")
async def execute(request: ExecuteRequest, user: User = Depends(current_active_user)):
    s = perf_counter()

    # Parse table names - handle both string and list formats
    if isinstance(request.table_names, str):
        # Split by comma if it's a string
        table_names = [name.strip() for name in request.table_names.split(",")]
    else:
        table_names = request.table_names

    if not table_names:
        return {"error": "No table names provided"}

    # Get all requested tables and validate they exist
    tables = []
    for table_name in table_names:
        table = metadata.get_table(table_name)
        if table is None:
            return {"error": f"Table '{table_name}' not found"}
        tables.append(table)

    # Create registrations for all tables
    registrations = []
    for table in tables:
        registrations.append(
            TableRegistration(
                table=table,
                table_name=table.name,  # Use the table's actual name
                # Don't pass paths - let register_dataset find the files automatically
            )
        )

    # Create a base S3 instance - the register_datasets method will handle
    # creating the correct LocalS3 instances for each table's path
    base_s3_path = f"{data_dir}/base/mps"  # This won't be used directly
    s3_instance = LocalS3(base_s3_path)

    # Register all datasets and execute the query
    ctx = SessionContext()

    try:
        registered_names = s3_instance.register_datasets(
            ctx, registrations, metadata, with_data=False
        )

        # Execute the query
        results = ctx.sql(request.query).to_polars()

        skip = (request.page - 1) * request.per_page
        out = results.slice(skip, request.per_page).to_dicts()

    except Exception as e:
        return {"error": f"Query execution failed: {str(e)}"}

    dur = perf_counter() - s

    return {
        "data": {
            "rows": out,
            "columns": results.columns,
            "query": request.query,
            "registered_tables": registered_names,
        },
        "total": results.height,
        "page": request.page,
        "per_page": request.per_page,
        "from": skip + 1,
        "to": skip + request.per_page,
        "last_page": math.ceil(results.height / request.per_page),
        "stats": {
            "execution_time": dur,
            "rows_processed": 0,
            "bytes_scanned": 0,
        },
    }


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
        "current_page": page,
        "from": (page - 1) * per_page + 1,
        "to": (page - 1) * per_page + per_page,
        "last_page": math.ceil(total / per_page),
    }


@app.get("/databases")
async def databases(user: User = Depends(current_active_user)):
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


@app.get("/create-sp-traffic-table")
async def create_sp_traffic_table(schema_name: str = "na"):
    db = metadata.get_database("db")
    if db is None:
        db = metadata.create_database(Database(id=0, name="db"))

    schema = metadata.get_schema(schema_name)
    if schema is None:
        schema = metadata.create_schema(
            Schema(id=0, name=schema_name, database_id=db.id)
        )

    table = metadata.get_table("sp_traffic")
    if table is None:
        table = ams_test.get_table()
        table.id = 0
        table.name = "sp_traffic"
        table.schema_id = schema.id
        table.database_id = db.id
        metadata.create_table(table)

    return {"message": "Table created"}


@app.get("/ingest-na-sp-traffic")
async def ingest_na_sp_traffic(limit: int = 5):
    await create_sp_traffic_table(schema_name="na")

    table = metadata.get_table("sp_traffic")
    if table is None:
        raise Exception("Table not found")

    s3_path = f"{data_dir}/sp_traffic/mps"
    s3 = LocalS3(s3_path)

    files = get_parquet_files("/ingest/ams/na/sp-traffic")
    print("Found", len(files), "parquet files")

    total_dur = 0.0
    total_count = 0
    for i, file in enumerate(files):
        s = perf_counter()
        print(f"Processing {file} ({i + 1}/{len(files)})")
        df = pl.read_parquet(file)

        if i > limit:
            break

        insert(table, s3, metadata, df)

        dur = perf_counter() - s
        total_count += df.height
        total_dur += dur
        rate = df.height / dur / 1000
        total_rate = total_count / total_dur / 1000
        print(
            f"    Inserted {df.height} rows at {rate:.2f}k rows/s (total rate: {total_rate:.2f}k rows/s)"
        )

        dst = f"/done{file}"
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.move(file, dst)

    return {"message": "Table created"}


@app.post("/dev/create-fake-data")
async def create_fake_data_endpoint(user: User = Depends(current_active_user)):
    """Manually trigger fake data creation (dev environment only)"""
    if os.getenv("APP_ENV") != "dev":
        return {"error": "This endpoint is only available in development environment"}

    if data_dir is None:
        return {"error": "DATA_DIR is not configured"}

    try:
        create_fake_tables_and_data(metadata, data_dir)
        return {"message": "Fake tables and data created successfully"}
    except Exception as e:
        return {"error": f"Failed to create fake data: {str(e)}"}


@app.delete("/dev/reset-fake-data")
async def reset_fake_data_endpoint(user: User = Depends(current_active_user)):
    """Reset fake data by dropping and recreating tables (dev environment only)"""
    if os.getenv("APP_ENV") != "dev":
        return {"error": "This endpoint is only available in development environment"}

    if data_dir is None:
        return {"error": "DATA_DIR is not configured"}

    try:
        result = reset_fake_data(metadata, data_dir)
        return {"message": "Fake data reset successfully", **result}
    except Exception as e:
        return {"error": f"Failed to reset fake data: {str(e)}"}


@app.get("/dev/fake-data-status")
async def fake_data_status_endpoint(user: User = Depends(current_active_user)):
    """Check the status of fake data tables (dev environment only)"""
    if os.getenv("APP_ENV") != "dev":
        return {"error": "This endpoint is only available in development environment"}

    if data_dir is None:
        return {"error": "DATA_DIR is not configured"}

    try:
        return get_fake_data_status(metadata, data_dir)
    except Exception as e:
        return {"error": f"Failed to get fake data status: {str(e)}"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
