from typing import Protocol

from classes import Table
from set.set_ops import SetOp


class VersionRepository(Protocol):
    """
    Protocol defining the interface for version repository implementations.

    A version repository manages versioned operations on tables, with support
    for checkpointing to optimize retrieval performance.
    """

    CHECKPOINT_FREQUENCY: int

    def add(self, table: Table, version: int, op: SetOp):
        """Add a new operation at the specified version for the given table."""
        raise NotImplementedError

    def get_hams(self, table: Table, version: int) -> set[int]:
        """Get the set of hams (items) at the specified version for the given table."""
        raise NotImplementedError
