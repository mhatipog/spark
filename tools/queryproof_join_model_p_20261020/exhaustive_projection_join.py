#!/usr/bin/env python3
"""Round P: exhaustive *model*, NOT a Spark optimizer correctness certificate.

Independently evaluate full bag joins versus joins after identity-attribute
projections. Precisely test multiplicities, SQL three-valued NULL comparisons,
left-outer NULL extension, and preservation of join-condition dependencies.

Each source relation is any bag of 0, 1, or 2 rows from a fixed 18-element
domain. The ignored third attribute deliberately creates projection collisions.
"""
from collections import Counter
from itertools import combinations_with_replacement, product
import json

VALUES = (None, 0, 1)
NOISE = (0, 1)
ROWS = tuple((key, value, noise) for key, value, noise in product(VALUES, VALUES, NOISE))
BAGS = tuple(tuple(rows) for size in range(3) for rows in combinations_with_replacement(ROWS, size))


def sql_eq(a, b):
    return None if a is None or b is None else a == b


def condition(a, b, predicate):
    equal = sql_eq(a[0], b[0])
    if predicate == "eq":
        return equal is True
    if predicate == "eq_and_right_nonnull":
        return equal is True and b[1] is not None
    raise AssertionError(predicate)


def project_output(a, b, mode):
    if mode == "value":
        return (a[1], b[1] if b is not None else None)
    if mode == "key_value":
        return (a[0], a[1], b[0] if b is not None else None,
                b[1] if b is not None else None)
    raise AssertionError(mode)


def original_full_bag_join(left, right, join_type, predicate, mode):
    """Full nested-loop bag join: preserve each original row occurrence."""
    out = Counter()
    for a in left:
        had_match = False
        for b in right:
            if condition(a, b, predicate):
                had_match = True
                out[project_output(a, b, mode)] += 1
        if join_type == "left" and not had_match:
            out[project_output(a, None, mode)] += 1
    return out


def projected_counted_bag_join(left, right, join_type, predicate, mode, *, deduplicate=False):
    """Independent bag-algebra evaluator: aggregate projected tuple counts.

    Only key and value survive on each side; noise is removed. Multiplicity of
    each projected row is retained except in the deliberate invalid mutant.
    """
    lc = Counter((a[0], a[1]) for a in left)
    rc = Counter((b[0], b[1]) for b in right)
    if deduplicate:
        lc = Counter({a: 1 for a in lc})
        rc = Counter({b: 1 for b in rc})
    out = Counter()
    for a, count_left in lc.items():
        total_right_matches = 0
        for b, count_right in rc.items():
            if condition(a, b, predicate):
                out[project_output(a, b, mode)] += count_left * count_right
                total_right_matches += count_right
        if join_type == "left" and total_right_matches == 0:
            out[project_output(a, None, mode)] += count_left
    return out


def without_key_mutant(left, right, join_type, predicate, mode):
    """Deliberately WRONG: replace key comparison with value comparison.

    It is impossible to evaluate the old key predicate if the key was omitted;
    using some surviving value is an adversarial non-preserving substitute.
    """
    out = Counter()
    for a in left:
        had_match = False
        for b in right:
            if sql_eq(a[1], b[1]) is True and (
                predicate != "eq_and_right_nonnull" or b[1] is not None
            ):
                had_match = True
                out[project_output(a, b, mode)] += 1
        if join_type == "left" and not had_match:
            out[project_output(a, None, mode)] += 1
    return out


def main():
    assert len(ROWS) == 18 and len(BAGS) == 190
    counts = Counter()
    dedup_counterexample = None
    missing_key_counterexample = None
    for left, right in product(BAGS, BAGS):
        for kind, predicate, mode in product(
            ("inner", "left"), ("eq", "eq_and_right_nonnull"), ("value", "key_value")
        ):
            reference = original_full_bag_join(left, right, kind, predicate, mode)
            transformed = projected_counted_bag_join(left, right, kind, predicate, mode)
            assert reference == transformed, (
                "VALID_PROJECTION_COUNTEREXAMPLE", left, right, kind, predicate,
                mode, reference, transformed
            )
            counts[(kind, predicate, mode)] += 1

            if dedup_counterexample is None:
                bad = projected_counted_bag_join(
                    left, right, kind, predicate, mode, deduplicate=True
                )
                if bad != reference:
                    dedup_counterexample = {
                        "left": left, "right": right, "join_type": kind,
                        "predicate": predicate, "output": mode,
                        "expected": sorted((str(k), v) for k, v in reference.items()),
                        "mutant": sorted((str(k), v) for k, v in bad.items()),
                    }
            if missing_key_counterexample is None:
                bad = without_key_mutant(left, right, kind, predicate, mode)
                if bad != reference:
                    missing_key_counterexample = {
                        "left": left, "right": right, "join_type": kind,
                        "predicate": predicate, "output": mode,
                        "expected": sorted((str(k), v) for k, v in reference.items()),
                        "mutant": sorted((str(k), v) for k, v in bad.items()),
                    }

    assert sum(counts.values()) == 190 * 190 * 8
    assert dedup_counterexample is not None, "BAD_DEDUP_MUTANT_NOT_FALSIFIED"
    assert missing_key_counterexample is not None, "BAD_KEY_MUTANT_NOT_FALSIFIED"
    result = {
        "status": "MODEL_EXHAUSTIVE_ALL_PASS",
        "not_spark_certificate": True,
        "full_row_domain": len(ROWS),
        "bags_per_relation": len(BAGS),
        "relation_pairs": len(BAGS) ** 2,
        "join_predicate_output_combinations": len(counts),
        "compared_full_vs_projected_instances": sum(counts.values()),
        "failed_safe_instances": 0,
        "checked_per_combination": {
            "/".join(parts): count for parts, count in sorted(counts.items())
        },
        "falsified_unsafe_deduplication_mutant": dedup_counterexample,
        "falsified_unsafe_missing_join_key_mutant": missing_key_counterexample,
        "scope_limits": [
            "Finite independent bag algebra, not a proof for all domains",
            "Join predicates deterministic and read retained named input columns only",
            "Only exact identity attribute projections, no evaluation side effects",
            "Two join types: inner equijoin and left outer equijoin",
            "NULL comparisons yield unknown; only TRUE counts as a match",
            "No Spark AST, Catalyst input state or JVM witnesses were checked",
        ],
    }
    print("QUERYPROOF_P_JOIN_BAG_MODEL", json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
