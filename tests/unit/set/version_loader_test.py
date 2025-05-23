from typing import Protocol

from classes import Table
from set.set_ops import SetOp, apply
from tests.set_ops_test import generate_random_ops


class VersionRepository(Protocol):
    def add(self, table: Table, version: int, op: SetOp):
        raise NotImplementedError

    def get_hams(self, table: Table, version: int) -> set[int]:
        raise NotImplementedError


class FakeVersionRepository(VersionRepository):
    CHECKPOINT_FREQUENCY = 1024

    def __init__(self) -> None:
        self.ops: dict[str, list[SetOp]] = {}
        self.checkpoints: dict[str, list[tuple[int, set[int]]]] = {}

    def add(self, table: Table, version: int, op: SetOp):
        if table.name not in self.ops:
            self.ops[table.name] = []
        self.ops[table.name].append(op)

        if version % self.CHECKPOINT_FREQUENCY == 0:
            self._checkpoint(table, version)

    def _checkpoint(self, table: Table, version: int):
        hams = self.get_hams(table, version)
        if table.name not in self.checkpoints:
            self.checkpoints[table.name] = []
        self.checkpoints[table.name].append((version, hams))

    def _highest_checkpoint(
        self, table: Table, version: int
    ) -> None | tuple[int, set[int]]:
        pass

    def get_hams(self, table: Table, version: int) -> set[int]:
        ops: list[SetOp] = self.ops[table.name]
        if version >= len(ops):
            raise ValueError(f"Version {version} is greater than the number of ops")

        return apply(set(), ops[: version + 1])


def test_fake_version_repository():
    ops = generate_random_ops(10)

    repo = FakeVersionRepository()
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
        repo.add(table, version, op)
        expected = apply(expected, op)
        assert repo.get_hams(table, version) == expected
