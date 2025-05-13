from fastapi import FastAPI
import uvicorn

from .classes import ColumnDefinitions, Database, Schema, Table

from .local_s3 import LocalS3
from .sqlite_metadata import SqliteMetadata

app = FastAPI()

metadata = SqliteMetadata("sqlite:///ams_scratch/ams.db")
s3 = LocalS3("ams_scratch/mps")


@app.get("/")
async def root():
    return {"message": "Hello World"}


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

    # Nest the schemas under the database
    dbs = {db.id: db.model_dump() for db in databases}
    for schema in schemas:
        if schema.database_id not in dbs:
            raise ValueError(
                f"Schema {schema.name} has database id {schema.database_id} but no database with that id"
            )
        if "schemas" not in dbs[schema.database_id]:
            dbs[schema.database_id]["schemas"] = {}
        dbs[schema.database_id]["schemas"][schema.id] = schema.model_dump()

    print(dbs)
    for table in tables:
        if table.schema_id not in dbs[table.database_id]["schemas"]:
            raise ValueError(
                f"Table {table.name} has schema id {table.schema_id} but no schema with that id"
            )
        if "tables" not in dbs[table.database_id]["schemas"][table.schema_id]:
            dbs[table.database_id]["schemas"][table.schema_id]["tables"] = {}

        dbs[table.database_id]["schemas"][table.schema_id]["tables"][table.id] = (
            table.model_dump()
        )

    return dbs


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
