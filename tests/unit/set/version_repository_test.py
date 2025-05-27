from sqlalchemy.ext.asyncio import AsyncSession

from classes import Table
from db import Base, create_async_engine
from repositories import (
    FakeVersionRepository,
    SqliteVersionRepository,
    VersionRepository,
)
from repositories.version_repository import SessionT
from set.set_ops import apply
from tests.set_ops_test import generate_random_ops


async def check_version_repository(
    repo: VersionRepository[SessionT], session: SessionT
):
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
        await repo.add(table, version, op, session)
        expected = apply(expected, op)
        assert await repo.get_hams(table, version, session) == expected


async def test_fake_version_repository():
    await check_version_repository(FakeVersionRepository(), None)


async def test_sqlite_version_repository():
    # Use in-memory SQLite database for testing
    db_path = "sqlite+aiosqlite:///:memory:"
    engine = create_async_engine(db_path)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    repo = SqliteVersionRepository(engine)
    async with AsyncSession(repo.engine) as session:
        await check_version_repository(repo, session)
