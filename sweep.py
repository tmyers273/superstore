from bisect import bisect_left, bisect_right
from typing import TypeVar

T = TypeVar("T")  # coordinate type
Id = TypeVar("Id")  # your label type


def find_ids_with_most_overlap(items: list[tuple[T, T, Id]]) -> set[Id]:
    """
    Closed intervals: [start, end] - both endpoints included.
    Returns the set of ids whose interval overlaps the largest number of other intervals.
    Time  O(n log n), space O(n).
    """
    if len(items) <= 1:
        return {items[0][2]} if items else set()

    # Pre-sort endpoints
    starts = sorted(s for s, _, _ in items)  # ascending start points
    ends = sorted(e for _, e, _ in items)  # ascending end points

    # Count overlaps for each interval i = [s_i, e_i]
    counts: list[int] = []
    for s, e, _ in items:
        left = bisect_right(starts, e)  # # {j | start_j ≤ e_i}
        right = bisect_left(ends, s)  # # {j | end_j   < s_i}
        counts.append(left - right - 1)  # subtract self

    max_cnt = max(counts)
    # all indices that tie for the maximum
    champions = [i for i, c in enumerate(counts) if c == max_cnt]

    # 2. collect their neighbours (overlap test is O(1))
    neighbours: set[Id] = set()
    for idx in champions:
        s1, e1, id1 = items[idx]
        for s2, e2, id2 in items:
            if id1 == id2:  # skip the champion itself
                continue
            if not (e1 < s2 or e2 < s1):  # closed-interval overlap test
                neighbours.add(id2)

    return neighbours


# vals = []
# for start, end, id in items:
#     vals.append((start, 0, id))
#     vals.append((end, 1, id))

# # Sort by v.0, then v.1
# print("Sorting overlapped")
# vals.sort(key=lambda x: (x[0], x[1]))

# active = set()
# print("Building overlapped")
# overlapped: dict[Id, set[Id]] = defaultdict(set)
# for i, val in enumerate(vals):
#     id = val[2]
#     is_start = val[1] == 0
#     if is_start:
#         active.add(id)
#     else:
#         active.remove(id)

#     if len(active) > 1:
#         for a in active:
#             for b in active:
#                 if a != b:
#                     overlapped.setdefault(a, set()).add(b)
#                     overlapped.setdefault(b, set()).add(a)
#     if i % 100 == 0:
#         print(f"    Done {i}/{len(vals)}")
# print("Done overlapped")

# # Find the set with the most overlaps
# max_overlaps = 0
# max_set = set()
# for id, overlaps in overlapped.items():
#     if len(overlaps) > max_overlaps:
#         max_overlaps = len(overlaps)
#         max_set = overlaps

# return max_set
