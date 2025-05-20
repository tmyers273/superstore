# This file contains differential testing, comparing the results from sqlite to
# the results from our database.
#
# This is intended to serve as a way of surfacing bugs via fuzzing.
#
# The basic idea is that we run our database and sqlite side by side, performing
# the same operations on both. If there is ever any difference, then we flag that
# as a bug.
import random
from typing import Generator, Protocol

import polars as pl
from sqlalchemy import Column, Integer, String, create_engine, select
from sqlalchemy.orm import Session, declarative_base

from classes import Database, Schema, Table
from metadata import FakeMetadataStore
from s3 import FakeS3
from tests.run_test import delete, insert, update


class DifferentialOp:
    pass


class InsertOp(DifferentialOp):
    def __init__(self, df: pl.DataFrame):
        self.df = df


class UpdateOp(DifferentialOp):
    def __init__(self, df: pl.DataFrame):
        self.df = df


class DeleteOp(DifferentialOp):
    def __init__(self, ids: list[int]):
        self.ids = ids


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


class DifferentialRunnerFake(DifferentialRunner):
    def __init__(self):
        self.metadata = FakeMetadataStore()
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
            )
        )

    def apply(self, op: DifferentialOp):
        match op:
            case InsertOp():
                insert(self.table, self.s3, self.metadata, op.df)
            case UpdateOp():
                update(self.table, self.s3, self.metadata, op.df)
            case DeleteOp():
                delete(self.table, self.s3, self.metadata, op.ids)
            case _:
                raise ValueError(f"Unknown operation: {op}")

    def check_invariants(self):
        # TODO: if there are partition keys, make sure that all the MPs don't violate
        # TODO: if there are partition keys, make sure all the MPs have the correct prefix
        pass

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
            self.used_ids.add(id)

        assert len(ids) == len(name) == len(email)

        return InsertOp(
            pl.DataFrame(
                {
                    "id": ids,
                    "name": name,
                    "email": email,
                    "count": [len(self.ops)] * len(ids),
                }
            )
        )

    def _gen_update(self) -> UpdateOp:
        count = min(len(self.used_ids), self.rng.randint(0, 10))
        ids = self.rng.sample(list(self.used_ids), count)
        names = []
        emails = []

        for id in ids:
            names.append(f"{id}")
            emails.append(f"{id}@{id}.com")

        df = pl.DataFrame(
            {
                "id": ids,
                "name": names,
                "email": emails,
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


def test_differential_testing():
    sqlite = DifferentialRunnerSqlite()
    fake = DifferentialRunnerFake()
    gen = OpGenerator()

    for _ in range(5_000):
        op = next(gen())

        fake.apply(op)
        sqlite.apply(op)

        fake_res = fake.all()
        sqlite_res = sqlite.all()

        assert fake_res == sqlite_res
