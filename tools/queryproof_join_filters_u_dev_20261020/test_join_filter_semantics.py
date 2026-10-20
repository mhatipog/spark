"""Independent finite-model adversary for join guards and project commutation.

Exhausts multiset multiplicities 0,1,2 over NULL/0/1 for both relations.
Uses a separately implemented relational bag interpreter, not Spark or the
QueryProof AST checker. This is bounded evidence, not a theorem or oracle
for arbitrary expressions, physical Spark behavior, or runtime errors.
"""
from collections import Counter
from itertools import product

KEYS = (None, 0, 1)


def sql_equal(a, b):
    # SQL equality is TRUE only if both operands are non-NULL and equal.
    return a is not None and b is not None and a == b


def join_bag(left, right, kind, predicate=sql_equal):
    result = Counter()
    for a, left_n in left.items():
        if not left_n:
            continue
        matched = False
        for b, right_n in right.items():
            if right_n and predicate(a, b):
                result[(a, b)] += left_n * right_n
                matched = True
        if kind == "left" and not matched:
            result[(a, None)] += left_n
    return result


def guard_non_null(relation):
    return Counter({k: n for k, n in relation.items() if k is not None and n})


def project(relation, positions):
    result = Counter()
    for row, count in relation.items():
        if count:
            result[tuple(row[i] for i in positions)] += count
    return result


def filter_projected(relation):
    return Counter({row: count for row, count in relation.items()
                    if row[0] is not None and count})


def test_all():
    bags = [Counter({key: n for key, n in zip(KEYS, counts) if n})
            for counts in product(range(3), repeat=len(KEYS))]
    pairs = 0
    for left in bags:
        for right in bags:
            pairs += 1
            inner = join_bag(left, right, "inner")
            assert inner == join_bag(guard_non_null(left), right, "inner")
            assert inner == join_bag(left, guard_non_null(right), "inner")
            assert inner == join_bag(
                guard_non_null(left), guard_non_null(right), "inner")
            outer = join_bag(left, right, "left")
            assert outer == join_bag(left, guard_non_null(right), "left")

    # Important soundness boundary: preserved-side guarding loses NULL rows.
    left = Counter({None: 2})
    right = Counter({0: 1})
    assert join_bag(left, right, "left") != join_bag(
        guard_non_null(left), right, "left")

    # A null-safe equality invalidates the IsNotNull inference.
    nullsafe = lambda a, b: (a is None and b is None) or sql_equal(a, b)
    null_left = Counter({None: 2})
    null_right = Counter({None: 3})
    assert join_bag(null_left, null_right, "inner", nullsafe) != join_bag(
        guard_non_null(null_left), null_right, "inner", nullsafe)

    # The commuting projection is exact identity selection, with bag
    # multiplicities summing when dropped columns distinguish source rows.
    rows = tuple((key, payload, extra)
                 for key in KEYS for payload in (0, 1) for extra in (0, 1))
    projected_bags = 0
    for counts in product(range(3), repeat=len(rows)):
        relation = Counter({row: n for row, n in zip(rows, counts) if n})
        lhs = filter_projected(project(relation, (0, 1)))
        rhs = project(filter_projected(relation), (0, 1))
        assert lhs == rhs
        projected_bags += 1

    print("QUERYPROOF_U_FINITE_JOIN_LAWS", {
        "bag_pair_configurations": pairs,
        "join_guards_checked": pairs * 4,
        "project_commutation_bags": projected_bags,
        "left_preserved_side_unsound_counterexample": True,
        "nullsafe_equality_unsound_counterexample": True,
        "limitations": "bounded finite multiset model, not general proof",
    })


if __name__ == "__main__":
    test_all()
