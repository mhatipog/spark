"""Exact equality-orbit certificates for actual Catalyst predicate transitions.

For the typed grammar over nullable integral variables, equality, null tests,
NOT, AND, OR, every expression is invariant under permutations of non-NULL
integer values.  A representative for each partition of the n variables plus
one distinguished NULL element is therefore sufficient: Bell(n+1) cases.
This is exact within that grammar, not for arbitrary Spark SQL.
"""
from itertools import product
from math import comb
import json
import sqlite3

TRUTH = (True, False, None)
PARTITIONS = {
    "ANY": (0, 0, 0),
    "TRUE_ONLY": (0, 1, 1),
    "FALSE_ONLY": (0, 1, 0),
    "NULL_ONLY": (0, 0, 1),
    "FULL_3VL": (0, 1, 2),
}
ARITY = {"Not": 1, "And": 2, "Or": 2, "IsNull": 1,
         "IsNotNull": 1, "EqualTo": 2, "EqualNullSafe": 2}
INTEGER_TYPES = {"integer", "long", "short", "byte"}
MAX_VARIABLES = 7


class Unsupported(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise Unsupported(reason)


def kind(node):
    return node.get("class", "").rsplit(".", 1)[-1]


def parse_expression(serialized):
    require(isinstance(serialized, list) and bool(serialized), "NO_EXPRESSION")

    def rec(pos):
        require(pos < len(serialized), "TRUNCATED")
        node = serialized[pos]
        require(isinstance(node, dict), "INVALID_NODE")
        op = kind(node)
        size = node.get("num-children")
        require(type(size) is int and 0 <= size <= 2, "INVALID_ARITY")
        if op == "AttributeReference":
            require(size == 0, "ATTRIBUTE_HAS_CHILDREN")
            dtype = node.get("dataType")
            require(dtype in INTEGER_TYPES, "NON_INTEGRAL_OR_BOOLEAN_ATTRIBUTE")
            eid = node.get("exprId")
            require(isinstance(eid, dict) and "id" in eid, "MISSING_EXPR_ID")
            key = json.dumps({"id": eid, "type": dtype}, sort_keys=True)
            return ("attr", key), "int", pos + 1
        require(op in ARITY and size == ARITY[op], "UNSUPPORTED_OP:" + op)
        children = []
        types = []
        end = pos + 1
        for _ in range(size):
            child, childtype, end = rec(end)
            children.append(child)
            types.append(childtype)
        if op in ("EqualTo", "EqualNullSafe"):
            require(types == ["int", "int"], "ILL_TYPED_EQUALITY")
        elif op in ("Not", "And", "Or"):
            require(all(t == "bool" for t in types), "ILL_TYPED_BOOLEAN")
        # IS NULL accepts both nullable integer and nullable Boolean inputs.
        return (op, *children), "bool", end

    tree, dtype, end = rec(0)
    require(end == len(serialized), "TRAILING_NODES")
    require(dtype == "bool", "NOT_A_BOOLEAN_PREDICATE")
    return tree


def attributes(tree):
    if tree[0] == "attr":
        return {tree[1]}
    result = set()
    for child in tree[1:]:
        result.update(attributes(child))
    return result


def canonical_representatives(n):
    """Canonical NULL/equality assignments; exactly Bell(n+1) valuations."""
    require(1 <= n <= MAX_VARIABLES, "ATTRIBUTE_COUNT_OUT_OF_SCOPE")
    def grow(parts, highest):
        if len(parts) == n + 1:
            yield tuple(None if label == 0 else label - 1 for label in parts[1:])
            return
        for label in range(highest + 2):
            yield from grow(parts + (label,), max(highest, label))
    yield from grow((0,), 0)


def bell_number(n):
    bell = [1]
    for i in range(n):
        bell.append(sum(comb(i, j) * bell[j] for j in range(i + 1)))
    return bell[n]


def logical_not(x):
    return None if x is None else not x


def logical_and(a, b):
    if a is False or b is False:
        return False
    if a is None or b is None:
        return None
    return True


def logical_or(a, b):
    if a is True or b is True:
        return True
    if a is None or b is None:
        return None
    return False


def evaluate(tree, env):
    op = tree[0]
    if op == "attr":
        return env[tree[1]]
    a = evaluate(tree[1], env)
    if op == "Not":
        return logical_not(a)
    if op == "IsNull":
        return a is None
    if op == "IsNotNull":
        return a is not None
    b = evaluate(tree[2], env)
    if op == "And":
        return logical_and(a, b)
    if op == "Or":
        return logical_or(a, b)
    if op == "EqualTo":
        return None if a is None or b is None else a == b
    if op == "EqualNullSafe":
        return a == b
    raise Unsupported("UNSUPPORTED_EVAL:" + op)


def distinguish(a, b, observer):
    require(observer in PARTITIONS, "UNKNOWN_OBSERVER")
    labels = PARTITIONS[observer]
    return labels[TRUTH.index(a)] != labels[TRUTH.index(b)]


def classify_trees(before, after, observer="TRUE_ONLY", allow_remap=False):
    keys = sorted(attributes(before) | attributes(after))
    if not allow_remap:
        require(attributes(before) == attributes(after), "INPUT_IDS_CHANGED")
    values = []
    for assignment in canonical_representatives(len(keys)):
        binding = dict(zip(keys, assignment))
        values.append({"bindings": binding, "before": evaluate(before, binding),
                       "after": evaluate(after, binding)})
    safe = [name for name in PARTITIONS
            if not any(distinguish(v["before"], v["after"], name) for v in values)]
    changed = [v for v in values
               if distinguish(v["before"], v["after"], observer)]
    changed.sort(key=lambda v: (
        sum(x is None for x in v["bindings"].values()),
        sum(abs(x) for x in v["bindings"].values() if x is not None),
        tuple(-1 if x is None else x for x in v["bindings"].values())))
    return {
        "status": "COUNTEREXAMPLE" if changed else "EQUIVALENT_IN_FRAGMENT",
        "observer": observer,
        "input_attributes": len(keys),
        "orbits_checked": len(values),
        "complete_orbits": bell_number(len(keys) + 1),
        "observable_differences": len(changed),
        "minimal_witness": changed[0] if changed else None,
        "safe_partitions": safe,
        "assumption": "No constants, arithmetic, coercions, collation, or effects; uncorrelated integral attributes",
    }


def changed_site(before, after):
    if before == after:
        return None
    if (before[0] != after[0] or len(before) != len(after) or
            before[0] in ("attr", "EqualTo", "EqualNullSafe", "IsNull", "IsNotNull")):
        return [], before, after
    indices = [i for i in range(1, len(before)) if before[i] != after[i]]
    if len(indices) != 1:
        return [], before, after
    found = changed_site(before[indices[0]], after[indices[0]])
    if found is None:
        return [], before, after
    path, old, new = found
    return [(before[0], indices[0] - 1), *path], old, new


def transfer_observation(observer, operator, child_index):
    """Exact partition pullback, universally quantifying sibling truth values."""
    require(observer in PARTITIONS, "UNKNOWN_OBSERVER")
    require(operator in {"Not", "And", "Or", "IsNull", "IsNotNull"},
            "UNSUPPORTED_CONSUMER:" + operator)
    require(child_index in (0, 1), "INVALID_CHILD_INDEX")
    require(operator in {"And", "Or"} or child_index == 0, "INVALID_CHILD_INDEX")
    labels = PARTITIONS[observer]
    signatures = []
    for value in TRUTH:
        outcomes = []
        siblings = TRUTH if operator in ("And", "Or") else (None,)
        for sibling in siblings:
            if operator == "Not":
                out = logical_not(value)
            elif operator == "IsNull":
                out = value is None
            elif operator == "IsNotNull":
                out = value is not None
            else:
                lhs, rhs = (value, sibling) if child_index == 0 else (sibling, value)
                out = logical_and(lhs, rhs) if operator == "And" else logical_or(lhs, rhs)
            outcomes.append(labels[TRUTH.index(out)])
        signatures.append(tuple(outcomes))
    equivalent = lambda p: all(
        (signatures[i] == signatures[j]) == (p[i] == p[j])
        for i in range(3) for j in range(3))
    matches = [name for name, partition in PARTITIONS.items() if equivalent(partition)]
    require(len(matches) == 1, "PARTITION_NOT_REPRESENTABLE")
    return matches[0]


def demanded_partition(context_path):
    demand = "TRUE_ONLY"
    for operator, child_index in context_path:
        demand = transfer_observation(demand, operator, child_index)
    return demand


def sqlite_validate(before, after, cert):
    """Independent SQL engine replay of every orbit (no producer truth table)."""
    keys = sorted(attributes(before) | attributes(after))
    columns = dict(zip(keys, ("c" + str(i) for i in range(len(keys)))))

    def emit(node):
        op = node[0]
        if op == "attr":
            return columns[node[1]]
        a = emit(node[1])
        if op == "Not":
            return "(NOT " + a + ")"
        if op == "IsNull":
            return "(" + a + " IS NULL)"
        if op == "IsNotNull":
            return "(" + a + " IS NOT NULL)"
        b = emit(node[2])
        sqlops = {"And": "AND", "Or": "OR", "EqualTo": "=", "EqualNullSafe": "IS"}
        require(op in sqlops, "SQLITE_UNSUPPORTED:" + op)
        return "(" + a + " " + sqlops[op] + " " + b + ")"

    args = ", ".join("? AS " + columns[k] for k in keys)
    sql = "SELECT " + emit(before) + ", " + emit(after) + " FROM (SELECT " + args + ")"
    pairs = []
    with sqlite3.connect(":memory:") as con:
        for values in canonical_representatives(len(keys)):
            a, b = con.execute(sql, values).fetchone()
            decode = lambda v: None if v is None else bool(v) if v in (0, 1) else _bad_value(v)
            pairs.append((decode(a), decode(b)))
    derived = [name for name in PARTITIONS
               if all(not distinguish(a, b, name) for a, b in pairs)]
    require(derived == cert["safe_partitions"], "SQLITE_SAFE_PARTITIONS_DISAGREE")
    observed = sum(distinguish(a, b, cert["observer"]) for a, b in pairs)
    require(observed == cert["observable_differences"], "SQLITE_DIFFERENCES_DISAGREE")
    return {"verified": True, "engine": "SQLite", "orbits_checked": len(pairs)}


def _bad_value(value):
    raise Unsupported("SQLITE_NON_BOOLEAN_VALUE:" + str(value))


def certify_predicate_event(event):
    """Certify a one-node Filter or INNER Join predicate-only Catalyst transition.

    Rule identity is deliberately excluded from the decision.  This is a
    selection/inner-join observation of TRUE, not three-valued scalar equality.
    Any other node, changed metadata, or unsupported expression fails closed.
    """
    try:
        old, new = event["old"], event["new"]
        require(isinstance(old, list) and isinstance(new, list)
                and len(old) == len(new), "PLAN_SHAPE_CHANGED")
        changed = [i for i in range(len(old)) if old[i] != new[i]]
        require(len(changed) == 1, "MULTIPLE_PLAN_NODES_CHANGED")
        i = changed[0]
        a, b = old[i], new[i]
        operator = kind(a)
        require(operator == kind(b) and operator in ("Join", "Filter"),
                "CHANGED_NODE_NOT_PREDICATE_CONSUMER")
        require({k: v for k, v in a.items() if k != "condition"} ==
                {k: v for k, v in b.items() if k != "condition"},
                "NON_CONDITION_PREDICATE_CHANGE")
        if operator == "Join":
            require("Inner$" in json.dumps(a.get("joinType", {})), "NOT_INNER_JOIN")
        else:
            require(a.get("num-children") == 1, "NOT_UNARY_FILTER")
        p, q = parse_expression(a.get("condition")), parse_expression(b.get("condition"))
        root = classify_trees(p, q, "TRUE_ONLY")
        site = changed_site(p, q)
        require(site is not None, "NO_REWRITE")
        path, local_p, local_q = site
        needed = demanded_partition(path)
        local = classify_trees(local_p, local_q, needed, allow_remap=True)
        if local["status"] == "EQUIVALENT_IN_FRAGMENT":
            require(root["status"] == "EQUIVALENT_IN_FRAGMENT",
                    "COMPOSITION_CONTRADICTION")
        check = sqlite_validate(p, q, root)
        check_local = sqlite_validate(local_p, local_q, local)
        return {"status": root["status"], "rule": event.get("rule"),
                "operator": operator, "context_path": path,
                "required_partition": needed,
                "root": root, "local": local,
                "local_guard": "ALLOW" if local["status"] == "EQUIVALENT_IN_FRAGMENT" else "BLOCK",
                "semantic_context_masking": (local["status"] == "COUNTEREXAMPLE" and
                                             root["status"] == "EQUIVALENT_IN_FRAGMENT"),
                "independent_verification": [check, check_local],
                "proof_scope": "exact equality/nullness orbit quotient only"}
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError) as exc:
        return {"status": "UNKNOWN", "reason": str(exc), "rule": event.get("rule")}


def certify_join_event(event):
    """Backward-compatible INNER Join-only entry point."""
    result = certify_predicate_event(event)
    if result.get("operator") == "Filter":
        return {"status": "UNKNOWN", "reason": "CHANGED_NODE_NOT_JOIN",
                "rule": event.get("rule")}
    return result
