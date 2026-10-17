"""Bounded top-k partition summaries: a necessary selection membership check.

Valid only when total ordering keys and stable row identities are supplied. The
reproducer uses distinct ages, so SQL order is unambiguous. This is not a
SQL-equivalence checker and requires independent input-stage access.
"""
import heapq
from collections import Counter


def local_topq(rows, q):
    """Return descending (age, -id, id, name) tuples for at most q source rows.

    Row shape: (id, name, age); rank age DESC, then id ASC to break ties.
    When used for SQL ORDER BY age alone, a tie at the boundary is UNKNOWN.
    """
    if q < 0:
        raise ValueError('q must be nonnegative')
    if q == 0:
        return []
    heap = []
    for row in rows:
        ident, name, age = row
        if age is None or type(age) is not int or type(ident) is not int:
            raise TypeError('only non-null integral total order supported')
        x = (age, -ident, ident, name)
        if len(heap) < q:
            heapq.heappush(heap, x)
        elif x > heap[0]:
            heapq.heapreplace(heap, x)
    return sorted(heap, reverse=True)


def merge_topq(partition_summaries, q):
    """Top_q of union of local summaries (associative for total ranks)."""
    return heapq.nlargest(q, (x for summary in partition_summaries for x in summary))


def witness(expected_names, actual_names):
    if len(expected_names) != len(set(expected_names)):
        return {'verdict': 'UNKNOWN', 'reason': 'AMBIGUOUS_SOURCE_IDENTITY'}
    if len(actual_names) != len(set(actual_names)):
        return {'verdict': 'MEMBERSHIP_VIOLATION' if len(actual_names) != len(expected_names)
                else 'UNKNOWN', 'reason': 'DUPLICATE_OUTPUT_IDENTITY'}
    a, e = Counter(actual_names), Counter(expected_names)
    return {'verdict': 'NO_VIOLATION_OBSERVED' if a == e else 'MEMBERSHIP_VIOLATION',
            'expected': list(expected_names), 'actual': list(actual_names)}
