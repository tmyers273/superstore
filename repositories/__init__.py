"""
Repository layer for data access patterns.

This package contains repository classes that implement data access logic
for various storage backends and versioning systems.
"""

from .fake_version_repository import FakeVersionRepository
from .sqlite_version_repository import SqliteVersionRepository
from .version_repository import VersionRepository

__all__ = ["VersionRepository", "FakeVersionRepository", "SqliteVersionRepository"]
