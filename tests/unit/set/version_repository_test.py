from classes import Table
from repositories import (
    FakeVersionRepository,
    SqliteVersionRepository,
    VersionRepository,
)
from set.set_ops import apply
from tests.set_ops_test import generate_random_ops


def check_version_repository(repo: VersionRepository):
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
        repo.add(table, version, op, None)
        expected = apply(expected, op)
        assert repo.get_hams(table, version, None) == expected


def test_fake_version_repository():
    check_version_repository(FakeVersionRepository())


def test_sqlite_version_repository():
    # Use in-memory SQLite database for testing
    repo = SqliteVersionRepository("sqlite:///:memory:")
    check_version_repository(repo)
