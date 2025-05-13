from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from .classes import ColumnDefinitions, Database, Schema, Table

from .local_s3 import LocalS3
from .sqlite_metadata import SqliteMetadata

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
s3 = LocalS3("ams_scratch/mps")


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/table/{table_id}")
async def table(table_id: int):
    print(metadata.get_tables())
    table = metadata.get_table("sp-traffic")
    if table is None:
        return {"error": "Table not found"}

    mps = []
    total_rows = 0
    total_filesize = 0
    for mp in metadata.micropartitions(table, s3):
        raw = mp.model_dump()
        del raw["data"]
        stats = mp.statistics()
        total_rows += stats.rows
        total_filesize += stats.filesize
        raw["stats"] = stats.model_dump()
        mps.append(raw)

    out = table.model_dump()
    out["mps"] = mps
    out["total_rows"] = total_rows
    out["total_filesize"] = total_filesize
    return out


@app.get("/databases")
async def databases():
    # metadata.create_database(Database(id=0, name="ams"))
    # metadata.create_schema(Schema(id=0, name="default", database_id=1))
    # metadata.create_table(
    #     Table(
    #         id=0,
    #         name="sp-traffic",
    #         schema_id=1,
    #         database_id=1,
    #         columns=[
    #             ColumnDefinitions(name="idempotency_id", type="String"),
    #             ColumnDefinitions(name="dataset_id", type="String"),
    #             ColumnDefinitions(name="marketplace_id", type="String"),
    #             ColumnDefinitions(name="currency", type="String"),
    #             ColumnDefinitions(name="advertiser_id", type="String"),
    #             ColumnDefinitions(name="campaign_id", type="String"),
    #             ColumnDefinitions(name="ad_group_id", type="String"),
    #             ColumnDefinitions(name="ad_id", type="String"),
    #             ColumnDefinitions(name="keyword_id", type="String"),
    #             ColumnDefinitions(name="keyword_text", type="String"),
    #             ColumnDefinitions(name="match_type", type="String"),
    #             ColumnDefinitions(name="placement", type="String"),
    #             ColumnDefinitions(name="time_window_start", type="String"),
    #             ColumnDefinitions(name="clicks", type="Int64"),
    #             ColumnDefinitions(name="impressions", type="Int64"),
    #             ColumnDefinitions(name="cost", type="Float64"),
    #         ],
    #     )
    # )
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
