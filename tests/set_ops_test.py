from ..set_ops import SetOpAdd, SetOpDelete, SetOpReplace, apply


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
