from enum import IntEnum


class SetOpType(IntEnum):
    ADD = 0
    REPLACE = 1
    DELETE = 2


class SetOp:
    type: SetOpType


class SetOpAdd(SetOp):
    type: SetOpType = SetOpType.ADD
    items: list[int]

    def __init__(self, items: list[int]):
        self.items = items


class SetOpReplace(SetOp):
    type: SetOpType = SetOpType.REPLACE
    items: list[tuple[int, int]]

    def __init__(self, items: list[tuple[int, int]]):
        self.items = items


class SetOpDelete(SetOp):
    type: SetOpType = SetOpType.DELETE
    items: list[int]

    def __init__(self, items: list[int]):
        self.items = items


def apply(set: set[int], op: SetOp | list[SetOp]) -> set[int]:
    if isinstance(op, list):
        for o in op:
            set = apply(set, o)
        return set

    match op:
        case SetOpAdd():
            return set.union(op.items)
        case SetOpReplace():
            remove = [i[0] for i in op.items]
            add = [i[1] for i in op.items]

            set = set.difference(remove)
            set = set.union(add)
            return set
        case SetOpDelete():
            return set.difference(op.items)
        case _:
            raise ValueError(f"Unknown set operation type: {op.type}")
