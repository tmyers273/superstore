from bisect import bisect_left, bisect_right


def find_ids_with_most_overlap[T, Id](items: list[tuple[T, T, Id]]) -> set[Id]:
    n = len(items)
    starts = sorted(s for s, _, _ in items)
    ends = sorted(e for _, e, _ in items)

    counts = []
    for s, e, _ in items:
        left = bisect_right(starts, e)  # starts ≤ e
        right = bisect_left(ends, s)  # ends  < s
        counts.append(left - right - 1)

    max_c = max(counts)
    return {id for (cnt, (_, _, id)) in zip(counts, items) if cnt == max_c}
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
