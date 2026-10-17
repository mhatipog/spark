"""A conservative, streaming necessary-condition check for observable SQL ORDER BY.

A violation is a definite failure of the supplied key-order contract.
No observed violation is NOT proof of SQL correctness or correct LIMIT membership.
Key positions/types are provided separately; we do not parse SQL automatically.
"""
from __future__ import annotations
from typing import Iterable, Sequence


def _compare_scalar(a: object, b: object, direction: str, nulls: str) -> int:
    if direction not in ('ASC', 'DESC') or nulls not in ('FIRST', 'LAST'):
        raise ValueError('unsupported direction or null policy')
    if a is None or b is None:
        if a is None and b is None:
            return 0
        return (-1 if nulls == 'FIRST' else 1) * (1 if a is None else -1)
    if type(a) is not int or type(b) is not int:
        raise TypeError('only integer key values supported; do not guess collation/NaN semantics')
    sign = (a > b) - (a < b)
    return sign if direction == 'ASC' else -sign


def _row_compare(a: Sequence[object], b: Sequence[object], keys: list[dict]) -> int:
    for key in keys:
        idx = key['index']
        if type(idx) is not int or idx < 0:
            raise ValueError('invalid key column index')
        c = _compare_scalar(a[idx], b[idx], key['direction'], key['nulls'])
        if c:
            return c
    return 0


def audit(rows: Iterable[Sequence[object]], keys: list[dict]) -> dict:
    """Compare neighboring observable output rows in O(1) additional space.

    CONTRACT_VIOLATED: witnessed decrease in the ORDER BY lexicographic preorder.
    NO_VIOLATION_OBSERVED: necessary condition held; other SQL bugs may remain.
    UNKNOWN: no supported exposed key or unsupported value encountered.
    """
    if not keys:
        return {'verdict': 'UNKNOWN', 'reason': 'NO_EXPOSED_GLOBAL_ORDER_KEYS'}
    previous = None
    pairs = 0
    first_violation = None
    try:
        for idx, row in enumerate(rows):
            if previous is not None:
                diff = _row_compare(previous, row, keys)
                pairs += 1
                if diff > 0 and first_violation is None:
                    first_violation = {'index': idx, 'previous': list(previous), 'current': list(row)}
            previous = row
    except (TypeError, ValueError, IndexError, KeyError) as exc:
        return {'verdict': 'UNKNOWN', 'reason': type(exc).__name__ + ': ' + str(exc)}
    return {'verdict': 'CONTRACT_VIOLATED' if first_violation else 'NO_VIOLATION_OBSERVED',
            'pairs_checked': pairs, 'first_violation': first_violation}
