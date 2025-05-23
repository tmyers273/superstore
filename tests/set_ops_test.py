import random

from set.set_ops import (
    SetOp,
    SetOpAdd,
    SetOpDelete,
    SetOpDeleteAndAdd,
    SetOpReplace,
    apply,
)


def generate_random_ops(n: int) -> list["SetOp"]:
    working_set = set()
    ops: list["SetOp"] = []
    for _ in range(n):
        op = random.choice(["add", "replace", "delete", "delete_and_add"])
        match op:
            case "add":
                count = random.randint(1, 10)
                ids = random.sample(range(1000), count)

                # Remove any ids that are already in the working set.
                ids = [id for id in ids if id not in working_set]

                ops.append(SetOpAdd(ids))
                working_set.update(ids)
            case "replace":
                count = min(random.randint(1, 10), len(working_set))
                new_ids = random.sample(range(1000), count)

                # Remove any ids that are already in the working set.
                new_ids = [id for id in new_ids if id not in working_set]

                old_ids = list(working_set)[: len(new_ids)]

                ops.append(SetOpReplace(list(zip(old_ids, new_ids))))
                working_set.update(new_ids)
            case "delete":
                count = min(random.randint(1, 10), len(working_set))
                ids = random.sample(list(working_set), count)
                ops.append(SetOpDelete(ids))
                working_set.difference_update(ids)
            case "delete_and_add":
                delete_count = min(random.randint(1, 10), len(working_set))
                add_count = random.randint(1, 10)

                delete_ids = random.sample(list(working_set), delete_count)
                add_ids = random.sample(range(1000), add_count)

                # Remove any ids that are already in the working set.
                add_ids = [id for id in add_ids if id not in working_set]

                ops.append(SetOpDeleteAndAdd((delete_ids, add_ids)))
                working_set.difference_update(delete_ids)
                working_set.update(add_ids)
            case _:
                raise ValueError(f"Unknown operation: {op}")
    return ops


def test_set_ops():
    s = set()

    s = apply(s, SetOpAdd([1, 2, 3]))
    assert s == {1, 2, 3}

    s = apply(s, SetOpReplace([(1, 4), (2, 5)]))
    assert s == {3, 4, 5}

    s = apply(s, SetOpDelete([4, 5]))
    assert s == {3}


def test_set_ops_stack():
    ops = [
        SetOpAdd([1, 2, 3]),
        SetOpReplace([(1, 4), (2, 5)]),
        SetOpDelete([4, 5]),
    ]

    s = apply(set(), ops)

    assert s == {3}
