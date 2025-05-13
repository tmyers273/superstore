from datetime import date
from ..sweep import find_ids_with_most_overlap


def test_sweep_with_integers():
    items = [
        (1, 5, 1),  # A
        (2, 6, 2),  # B
        (4, 8, 3),  # C
        (7, 9, 4),  # D
    ]

    result = find_ids_with_most_overlap(items)

    # C should have the most overlaps (with A, B, and D)
    assert len(result) == 3
    assert 1 in result  # A
    assert 2 in result  # B
    assert 4 in result  # D


def test_sweep_with_dates():
    items = [
        (date(2024, 1, 1), date(2024, 1, 5), 1),  # A
        (date(2024, 1, 2), date(2024, 1, 6), 2),  # B
        (date(2024, 1, 4), date(2024, 1, 8), 3),  # C
        (date(2024, 1, 7), date(2024, 1, 9), 4),  # D
    ]

    result = find_ids_with_most_overlap(items)

    # C should have the most overlaps (with A, B, and D)
    assert len(result) == 3
    assert 1 in result  # A
    assert 2 in result  # B
    assert 4 in result  # D
