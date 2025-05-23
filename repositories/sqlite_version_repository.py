from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from classes import Table
from db import Base, Operation, OperationSnapshot
from set.set_ops import SetOp, apply

from .version_repository import VersionRepository


class SqliteVersionRepository(VersionRepository):
    CHECKPOINT_FREQUENCY = 1024

    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        Base.metadata.create_all(self.engine)

    def add(self, table: Table, version: int, op: SetOp):
        with Session(self.engine) as session:
            # Convert SetOp to operation data
            operation_type, data = self._set_op_to_operation_data(op)

            # Create operation record
            operation = Operation(
                table_name=table.name,
                version=version,
                operation_type=operation_type,
                data=data,
            )
            session.add(operation)

            # Create checkpoint if needed
            if version % self.CHECKPOINT_FREQUENCY == 0:
                self._checkpoint(session, table, version)

            session.commit()

    def get_hams(self, table: Table, version: int) -> set[int]:
        with Session(self.engine) as session:
            # Find the highest checkpoint <= version
            checkpoint = self._highest_checkpoint(session, table, version)

            if checkpoint is not None:
                checkpoint_version, hams = checkpoint
                # Get operations from checkpoint to target version
                ops = self._get_ops_range(
                    session, table, checkpoint_version + 1, version
                )
            else:
                hams = set()
                # Get all operations from 0 to target version
                ops = self._get_ops_range(session, table, 0, version)

            return apply(hams, ops)

    def _set_op_to_operation_data(self, op: SetOp) -> tuple[str, Any]:
        """Convert a SetOp to operation_type and data suitable for Operation model"""
        from set.set_ops import SetOpAdd, SetOpDelete, SetOpDeleteAndAdd, SetOpReplace

        if isinstance(op, SetOpAdd):
            return ("add", op.items)
        elif isinstance(op, SetOpDelete):
            return ("delete", op.items)
        elif isinstance(op, SetOpReplace):
            return ("replace", op.items)
        elif isinstance(op, SetOpDeleteAndAdd):
            return ("delete_and_add", list(op.items))
        else:
            raise ValueError(f"Unknown SetOp type: {type(op)}")

    def _checkpoint(self, session: Session, table: Table, version: int):
        """Create a checkpoint at the given version"""
        hams = self._calculate_hams_at_version(session, table, version)

        snapshot = OperationSnapshot(
            table_name=table.name,
            version=version,
            data=list(hams),  # Convert set to list for JSON storage
        )
        session.add(snapshot)

    def _calculate_hams_at_version(
        self, session: Session, table: Table, version: int
    ) -> set[int]:
        """Calculate the hams at a specific version by applying all operations up to that point"""
        ops = self._get_ops_range(session, table, 0, version)
        return apply(set(), ops)

    def _highest_checkpoint(
        self, session: Session, table: Table, version: int
    ) -> None | tuple[int, set[int]]:
        """Find the highest checkpoint <= version"""
        stmt = (
            select(OperationSnapshot)
            .where(OperationSnapshot.table_name == table.name)
            .where(OperationSnapshot.version <= version)
            .order_by(OperationSnapshot.version.desc())
            .limit(1)
        )

        snapshot = session.execute(stmt).scalars().one_or_none()
        if snapshot is None:
            return None

        return (int(snapshot.version), set(snapshot.data))

    def _get_ops_range(
        self, session: Session, table: Table, start_version: int, end_version: int
    ) -> list[SetOp]:
        """Get operations in version range [start_version, end_version] inclusive"""
        stmt = (
            select(Operation)
            .where(Operation.table_name == table.name)
            .where(Operation.version >= start_version)
            .where(Operation.version <= end_version)
            .order_by(Operation.version)
        )

        operations = session.execute(stmt).scalars().all()
        return [op.to_set_op() for op in operations]
