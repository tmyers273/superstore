from classes import Table
from set.set_ops import SetOp, apply

from .version_repository import VersionRepository


class FakeVersionRepository(VersionRepository[None]):
    """
    In-memory implementation of VersionRepository for testing purposes.

    This implementation stores operations and checkpoints in memory,
    making it fast and suitable for unit tests.
    """

    CHECKPOINT_FREQUENCY = 1024

    def __init__(self) -> None:
        self.ops: dict[str, list[SetOp]] = {}
        self.checkpoints: dict[str, list[tuple[int, set[int]]]] = {}

    async def add(self, table: Table, version: int, op: SetOp, session: None):
        if table.name not in self.ops:
            self.ops[table.name] = []
        self.ops[table.name].append(op)

        if version % self.CHECKPOINT_FREQUENCY == 0:
            await self._checkpoint(table, version, session)

    async def _checkpoint(self, table: Table, version: int, session: None):
        hams = await self.get_hams(table, version, None)
        if table.name not in self.checkpoints:
            self.checkpoints[table.name] = []
        self.checkpoints[table.name].append((version, hams))

    def _highest_checkpoint(
        self, table: Table, version: int
    ) -> None | tuple[int, set[int]]:
        if table.name not in self.checkpoints:
            return None

        checkpoints = self.checkpoints[table.name]

        # Iterate in reverse to find the highest checkpoint <= version
        for checkpoint_version, hams in reversed(checkpoints):
            if checkpoint_version <= version:
                return (checkpoint_version, hams)

        return None

    async def get_hams(self, table: Table, version: int, session: None) -> set[int]:
        if table.name not in self.ops:
            return set()

        ops: list[SetOp] = self.ops[table.name]
        if version >= len(ops):
            raise ValueError(f"Version {version} is greater than the number of ops")

        checkpoint = self._highest_checkpoint(table, version)
        if checkpoint is not None:
            checkpoint_version, hams = checkpoint
            ops = ops[checkpoint_version : version + 1]
        else:
            hams = set()
            ops = ops[: version + 1]

        return apply(hams, ops)
