# Snapshots are performance optimization.
#
# To reconstruct the database a given version, we need to determine which
# micropartitions were present in that version.
#
# The list of micropartition ids in a given version can be modelled as a set.
#
# We can add, remove, or replace elements in the set. If we keep track of
# every operation we have ever performed, we can reconstruct the set of
# micropartition ids in any given version.
#
# This works, but means that to construct the Nth version, we need to walk
# through all N operations. Fine if N is 100, not so great if N is 1,000,000,000.
#
# To make this more efficient, we can store a snapshot of the set every X ops.
# Then, to reconstruct the current set:
#   - first load the first snapshot with a version number <= the target version
#   - then load all the ops from the _snapshot's_ version number,
#   - then apply them to the working set.
#
# If we snapshot every 1000 items (let's call this B), to get the HAMS at
# version N, we need to:
#   - load the snapshot from `floor(N/B)`
#   - load all the ops >= `floor(N/B)` and <= `N`
#   - apply the ops to the snapshot
#
# This gives a bounded time.


import bisect
from typing import Protocol, Tuple, Union

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from classes import Table
from db import Base, OperationSnapshot, create_async_engine
from set.set_ops import apply
from tests.set_ops_test import generate_random_ops


class Snapshotter(Protocol):
    async def set(self, table: Table, version: int, cams: list[int]):
        raise NotImplementedError

    async def get(
        self, table: Table, version: int
    ) -> Union[None, Tuple[int, list[int]]]:
        """
        Given a version, return the snapshot with the highest
        version <= the given version.

        Returns the snapshot version and the snapshot itself
        if there is a snapshot for the given version, otherwise None.
        """
        raise NotImplementedError


class SqliteSnapshotter(Snapshotter):
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def set(self, table: Table, version: int, cams: list[int]):
        snapshot = OperationSnapshot(
            table_name=table.name,
            version=version,
            data=cams,
        )
        async with AsyncSession(self.engine) as session:
            session.add(snapshot)
            await session.commit()

    async def get(
        self, table: Table, version: int
    ) -> Union[None, Tuple[int, list[int]]]:
        async with AsyncSession(self.engine) as session:
            q = (
                select(OperationSnapshot)
                .filter(OperationSnapshot.table_name == table.name)
                .filter(OperationSnapshot.version <= version)
                .order_by(OperationSnapshot.version.desc())
            )
            snapshot = await session.scalar(q)
        if snapshot is None:
            return None
        return snapshot.version, snapshot.data


class FakeSnapshotter(Snapshotter):
    def __init__(self):
        self.snapshots_data: dict[int, list[int]] = {}
        self.sorted_versions: list[int] = []

    async def set(self, table: Table, version: int, cams: list[int]):
        snapshot_value = cams

        if version not in self.snapshots_data:
            # This is a new version, add it to sorted_versions
            self.snapshots_data[version] = snapshot_value
            bisect.insort(
                self.sorted_versions, version
            )  # Keeps self.sorted_versions sorted
        else:
            # This is an update to an existing version's data
            self.snapshots_data[version] = snapshot_value
            # self.sorted_versions does not need to change as 'version' is already in it.

    async def get(
        self, table: Table, version: int
    ) -> Union[None, Tuple[int, list[int]]]:
        if not self.sorted_versions:
            return None

        # Find the insertion point for 'version' in self.sorted_versions.
        # bisect_right returns an index 'idx' such that all elements in
        # self.sorted_versions[:idx] are <= 'version', and all elements
        # in self.sorted_versions[idx:] are > 'version'.
        idx = bisect.bisect_right(self.sorted_versions, version)

        if idx == 0:
            # No snapshot version is less than or equal to the given version.
            # This means 'version' is smaller than all stored snapshot versions.
            return None
        else:
            # The snapshot version we're looking for is at index idx-1.
            found_version = self.sorted_versions[idx - 1]
            return found_version, self.snapshots_data[found_version]


async def check_snapshotter(snapshotter: Snapshotter):
    table = Table(
        id=0,
        name="test",
        schema_id=1,
        database_id=1,
        columns=[],
        partition_keys=[],
        sort_keys=[],
    )
    await snapshotter.set(table, 1, [])
    await snapshotter.set(table, 3, [1, 2])
    assert await snapshotter.get(table, 0) is None
    assert await snapshotter.get(table, 1) == (1, [])
    assert await snapshotter.get(table, 2) == (1, [])
    assert await snapshotter.get(table, 3) == (3, [1, 2])
    assert await snapshotter.get(table, 4) == (3, [1, 2])


async def test_fake_snapshot():
    snapshotter = FakeSnapshotter()
    await check_snapshotter(snapshotter)


async def test_sqlite_snapshot():
    db_path = "sqlite+aiosqlite:///:memory:"
    engine = create_async_engine(db_path)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    snapshotter = SqliteSnapshotter(engine)
    await check_snapshotter(snapshotter)


def test_snapshot_ops():
    ops = generate_random_ops(50)
    expected = apply(set(), ops)
    print(f"Expected has {len(expected)} items")
