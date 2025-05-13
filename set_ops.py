from enum import IntEnum


class SetOpType(IntEnum):
    ADD = 0
    REPLACE = 1
    DELETE = 2
    DELETE_AND_ADD = 3


class SetOp:
    type: SetOpType


class SetOpAdd(SetOp):
    type: SetOpType = SetOpType.ADD
    items: list[int]

    def __repr__(self) -> str:
        return f"SetOpAdd({self.items})"

    def __init__(self, items: list[int]):
        self.items = items

    def __eq__(self, other):
        if not isinstance(other, SetOpAdd):
            return False
        return self.items == other.items


class SetOpReplace(SetOp):
    type: SetOpType = SetOpType.REPLACE
    items: list[tuple[int, int]]

    def __repr__(self) -> str:
        return f"SetOpReplace({self.items})"

    def __init__(self, items: list[tuple[int, int]]):
        self.items = items

    def __eq__(self, other):
        if not isinstance(other, SetOpReplace):
            return False
        return self.items == other.items


class SetOpDelete(SetOp):
    type: SetOpType = SetOpType.DELETE
    items: list[int]

    def __repr__(self) -> str:
        return f"SetOpDelete({self.items})"

    def __init__(self, items: list[int]):
        self.items = items

    def __eq__(self, other):
        if not isinstance(other, SetOpDelete):
            return False
        return self.items == other.items


class SetOpDeleteAndAdd(SetOp):
    type: SetOpType = SetOpType.DELETE_AND_ADD
    items: tuple[list[int], list[int]]

    def __repr__(self) -> str:
        return f"SetOpDeleteAndAdd({self.items})"

    def __init__(self, items: tuple[list[int], list[int]]):
        self.items = items

    def __eq__(self, other):
        if not isinstance(other, SetOpDeleteAndAdd):
            return False
        return self.items == other.items


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
        case SetOpDeleteAndAdd():
            remove = op.items[0]
            add = op.items[1]

            set = set.difference(remove)
            set = set.union(add)
            return set
        case _:
            raise ValueError(f"Unknown set operation type: {op.type}")
