# This file contains differential testing, comparing the results from sqlite to
# the results from our database.
#
# This is intended to serve as a way of surfacing bugs via fuzzing.
#
# The basic idea is that we run our database and sqlite side by side, performing
# the same operations on both. If there is ever any difference, then we flag that
# as a bug.
import io
import random
from time import perf_counter
from typing import Generator, Protocol

import polars as pl
from sqlalchemy import Column, Integer, String, create_engine, select
from sqlalchemy.orm import Session, declarative_base

from classes import Database, Schema, Table
from metadata import MetadataStore
from s3 import FakeS3
from sqlite_metadata import SqliteMetadata
from tests.run_test import delete, insert, update


class DifferentialOp:
    pass


class InsertOp(DifferentialOp):
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def __repr__(self):
        return f"InsertOp({self.df})"


class UpdateOp(DifferentialOp):
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def __repr__(self):
        return f"UpdateOp({self.df})"


class DeleteOp(DifferentialOp):
    def __init__(self, ids: list[int]):
        self.ids = ids

    def __repr__(self):
        return f"DeleteOp({self.ids})"


class DifferentialRunner(Protocol):
    def apply(self, op: DifferentialOp):
        raise NotImplementedError

    def check_invariants(self):
        raise NotImplementedError


class DifferentialRunnerSqlite(DifferentialRunner):
    def __init__(self):
        Base = declarative_base()

        class Users(Base):
            __tablename__ = "table_versions"
            __table_args__ = {"sqlite_autoincrement": True}

            id = Column(Integer, primary_key=True, autoincrement=True)
            account_id = Column(Integer, nullable=False)
            name = Column(String, nullable=False)
            email = Column(String, nullable=False)
            count = Column(Integer, nullable=False)

        self.users = Users
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)

    def apply(self, op: DifferentialOp):
        match op:
            case InsertOp():
                with Session(self.engine) as session:
                    for row in op.df.to_dicts():
                        session.add(self.users(**row))
                    session.commit()
            case UpdateOp():
                with Session(self.engine) as session:
                    for row in op.df.to_dicts():
                        session.query(self.users).filter(
                            self.users.id == row["id"]
                        ).update(row)
                    session.commit()
            case DeleteOp():
                with Session(self.engine) as session:
                    for id in op.ids:
                        session.query(self.users).filter(self.users.id == id).delete()
                    session.commit()
            case _:
                raise ValueError(f"Unknown operation: {op}")

    def check_invariants(self):
        pass

    def all(self) -> list[dict]:
        with Session(self.engine) as session:
            stmt = select(self.users).order_by(self.users.id)
            items = session.execute(stmt).scalars().all()
            dict_items = [item.__dict__ for item in items]
            dict_items = [
                {k: v for k, v in item.items() if not k.startswith("_")}
                for item in dict_items
            ]
            return dict_items


class DifferentialRunnerSuperstore(DifferentialRunner):
    def __init__(
        self, metadata: MetadataStore, partition_keys: list[str] | None = None
    ):
        self.metadata = metadata
        self.s3 = FakeS3()

        self.db = self.metadata.create_database(Database(id=0, name="test"))
        self.schema = self.metadata.create_schema(
            Schema(id=0, name="test", database_id=self.db.id)
        )
        self.table = self.metadata.create_table(
            Table(
                id=0,
                name="test",
                database_id=self.db.id,
                schema_id=self.schema.id,
                columns=[],
                partition_keys=partition_keys,
            )
        )

    def apply(self, op: DifferentialOp):
        match op:
            case InsertOp():
                # with timer("fake insert op"):
                insert(self.table, self.s3, self.metadata, op.df)
            case UpdateOp():
                # with timer("fake update op"):
                update(self.table, self.s3, self.metadata, op.df)
            case DeleteOp():
                # with timer("fake delete op"):
                delete(self.table, self.s3, self.metadata, op.ids)
            case _:
                raise ValueError(f"Unknown operation: {op}")

    def check_invariants(self):
        # TODO: if there are partition keys, make sure that all the MPs don't violate
        # TODO: if there are partition keys, make sure all the MPs have the correct prefix
        if self.table.partition_keys:
            for mp in self.metadata.micropartitions(
                self.table, self.s3, with_data=False
            ):
                # Make sure the MP has a key prefix
                if mp.key_prefix is None:
                    raise ValueError(f"Micropartition {mp.id} has no key prefix")

                # Make sure the MP prefix contains all the partition keys
                parts = mp.key_prefix.split("/")[:-1]
                parts = [part.split("=")[0] for part in parts]
                assert parts == self.table.partition_keys, (
                    f"Micropartition {mp.id} has incorrect key prefix: {mp.key_prefix}"
                )

                # Make sure the MP exists in S3
                key = f"{mp.key_prefix}{mp.id}"
                raw = self.s3.get_object("bucket", key)
                assert raw is not None, f"Micropartition {key} not found in s3"
                part = io.BytesIO(raw)
                df = pl.read_parquet(part)

                # Finally, make sure the MP contains only one tuple per partition key
                #
                # If the partition key is "account_id", then there should only be
                # one unique account_id in the MP
                #
                # If there are multiple partition keys, then there should only be
                # one unique tuple per partition key
                #
                # ie if the partition keys are ["account_id", "region"], then there
                # should only be one unique tuple in the MP
                cnt = df.group_by(self.table.partition_keys).n_unique().height
                assert 1 == cnt, f"Micropartition {key} has duplicate rows\n\n{df}"

    def all(self) -> list[dict]:
        df = self.metadata.all(self.table, self.s3)
        df = df.sort("id")
        return df.to_dicts()


rng = random.Random(1)


class OpGenerator:
    def __init__(self, seed: int = 1):
        self.rng = random.Random(seed)
        self.used_ids = set()
        self.number_of_ops = 3
        self.ops = []

    def _op_code(self) -> int:
        return self.rng.randint(0, self.number_of_ops - 1)

    def _gen_insert(self) -> InsertOp:
        count = self.rng.randint(0, 10)
        ids = []
        account_ids = []
        name = []
        email = []

        for _ in range(count):
            # Spin until we get a new, unused id
            id = self.rng.randint(0, 1000000000)
            while id in self.used_ids:
                id = self.rng.randint(0, 1000000000)

            ids.append(id)
            name.append(f"{id}")
            email.append(f"{id}@{id}.com")
            account_ids.append(id % 20)
            self.used_ids.add(id)

        assert len(ids) == len(name) == len(email)

        return InsertOp(
            pl.DataFrame(
                {
                    "id": ids,
                    "name": name,
                    "email": email,
                    "account_id": account_ids,
                    "count": [len(self.ops)] * len(ids),
                }
            )
        )

    def _gen_update(self) -> UpdateOp:
        count = min(len(self.used_ids), self.rng.randint(0, 10))
        ids = self.rng.sample(list(self.used_ids), count)
        names = []
        emails = []
        account_ids = []
        for id in ids:
            names.append(f"{id}")
            emails.append(f"{id}@{id}.com")
            account_ids.append(id % 20)

        df = pl.DataFrame(
            {
                "id": ids,
                "name": names,
                "email": emails,
                "account_id": account_ids,
                "count": [len(self.ops)] * count,
            }
        )
        return UpdateOp(df)

    def _gen_delete(self) -> DeleteOp:
        count = min(len(self.used_ids), self.rng.randint(0, 10))
        ids = self.rng.sample(list(self.used_ids), count)
        return DeleteOp(ids)

    def __call__(self) -> Generator[DifferentialOp, None, None]:
        while True:
            op_code = self._op_code()
            match op_code:
                case 0:
                    op = self._gen_insert()
                    self.ops.append(op)
                    yield op
                case 1:
                    op = self._gen_update()
                    self.ops.append(op)
                    yield op
                case 2:
                    op = self._gen_delete()
                    self.ops.append(op)
                    yield op
                case _:
                    raise ValueError(f"Unknown operation: {op_code}")


# @pytest.mark.skip(reason="Takes too long to run")
def test_differential_testing():
    metadata = SqliteMetadata("sqlite:///:memory:")
    sqlite = DifferentialRunnerSqlite()
    fake = DifferentialRunnerSuperstore(metadata, partition_keys=["account_id"])
    gen = OpGenerator()

    fake_apply_times = 0
    sqlite_apply_times = 0
    fake_all_times = 0
    sqlite_all_times = 0

    for i in range(5_000):
        op = next(gen())

        start = perf_counter()
        fake.apply(op)
        fake_apply_times += perf_counter() - start
        fake.check_invariants()

        start = perf_counter()
        sqlite.apply(op)
        sqlite_apply_times += perf_counter() - start
        sqlite.check_invariants()

        start = perf_counter()
        fake_res = fake.all()
        fake_all_times += perf_counter() - start

        start = perf_counter()
        sqlite_res = sqlite.all()
        sqlite_all_times += perf_counter() - start

        assert fake_res == sqlite_res

        if i % 100 == 0:
            print(
                f"Completed {i} ops w/ {len(sqlite_res)} rows and {fake.metadata.micropartition_count(fake.table, fake.s3)} MPs. fake_apply_time={fake_apply_times:.2f}, sqlite_apply_time={sqlite_apply_times:.2f}, fake_all_time={fake_all_times:.2f}, sqlite_all_time={sqlite_all_times:.2f}"
            )
