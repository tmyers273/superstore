from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from classes import Table
from db import Base
from repositories import (
    FakeVersionRepository,
    SqliteVersionRepository,
    VersionRepository,
)
from repositories.version_repository import SessionT
from set.set_ops import apply
from tests.set_ops_test import generate_random_ops


def check_version_repository(repo: VersionRepository[SessionT], session: SessionT):
    ops = generate_random_ops(10)

    repo.CHECKPOINT_FREQUENCY = 3

    expected = set()
    table = Table(
        id=0,
        name="test",
        schema_id=1,
        database_id=1,
        columns=[],
    )

    for version, op in enumerate(ops):
        repo.add(table, version, op, session)
        expected = apply(expected, op)
        assert repo.get_hams(table, version, session) == expected


def test_fake_version_repository():
    check_version_repository(FakeVersionRepository(), None)


def test_sqlite_version_repository():
    # Use in-memory SQLite database for testing
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    repo = SqliteVersionRepository(engine)
    with Session(repo.engine) as session:
        check_version_repository(repo, session)
