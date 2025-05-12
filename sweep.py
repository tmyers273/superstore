from collections import defaultdict


def find_ids_with_most_overlap[T, Id](items: list[tuple[T, T, Id]]) -> set[Id]:
    vals = []
    for start, end, id in items:
        vals.append((start, 0, id))
        vals.append((end, 1, id))

    # Sort by v.0, then v.1
    vals.sort(key=lambda x: (x[0], x[1]))

    active = set()
    overlapped: dict[Id, set[Id]] = defaultdict(set)
    for val in vals:
        id = val[2]
        is_start = val[1] == 0
        if is_start:
            active.add(id)
        else:
            active.remove(id)

        if len(active) > 1:
            for a in active:
                for b in active:
                    if a != b:
                        overlapped.setdefault(a, set()).add(b)
                        overlapped.setdefault(b, set()).add(a)

    # Find the set with the most overlaps
    max_overlaps = 0
    max_set = set()
    for id, overlaps in overlapped.items():
        if len(overlaps) > max_overlaps:
            max_overlaps = len(overlaps)
            max_set = overlaps

    return max_set
