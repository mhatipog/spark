"""Independent, fail-closed Catalyst JSON-to-guard-fact observation for V11.

No V9/V10 checker imports. Its output is a candidate Coq certificate, not a
proof of native Spark semantics. JSON erases metadata; equal JSON is NOT a
full Catalyst-state equality proof. Structural checks reject unknown syntax.
"""
import hashlib
import json

LOGICAL = "org.apache.spark.sql.catalyst.plans.logical."
EXPR = "org.apache.spark.sql.catalyst.expressions."
LEAVES = ("org.apache.spark.sql.execution.LogicalRDD", LOGICAL+"LocalRelation")
COMPARATORS = {EXPR+"EqualTo": "StrictEq", EXPR+"LessThan": "StrictLt"}
JOINS = {"org.apache.spark.sql.catalyst.plans.Inner$": "Inner",
         "org.apache.spark.sql.catalyst.plans.LeftOuter$": "LeftOuter"}
INTEGRAL = frozenset(("byte", "short", "integer", "long"))

class Rejected(ValueError):
    pass

def check(predicate, why):
    if not predicate:
        raise Rejected(why)

def tree(nodes):
    """Independently reconstruct the entirety of a TreeNode.toJSON pre-order list."""
    check(isinstance(nodes, list) and bool(nodes), "EMPTY_OR_NONLIST_TREE")
    index = 0
    def read():
        nonlocal index
        check(index < len(nodes), "TRUNCATED_PREORDER")
        head = nodes[index]
        index += 1
        check(isinstance(head, dict) and
              isinstance(head.get("class"), str) and
              type(head.get("num-children")) is int and
              0 <= head["num-children"] <= 20,
              "MALFORMED_PREORDER_NODE")
        return (head, [read() for _ in range(head["num-children"])])
    result = read()
    check(index == len(nodes), "TRAILING_PREORDER_NODES")
    return result

def attr(node):
    check(isinstance(node, dict) and
          node.get("class") == EXPR+"AttributeReference" and
          node.get("num-children") == 0 and
          node.get("dataType") in INTEGRAL and
          isinstance(node.get("name"), str) and bool(node["name"]) and
          type(node.get("nullable")) is bool,
          "NOT_EXACT_INTEGRAL_ATTR")
    ident = node.get("exprId")
    check(isinstance(ident, dict) and
          type(ident.get("id")) is int and
          isinstance(ident.get("jvmId"), str),
          "BAD_CATALYST_EXPR_ID")
    return (json.dumps(ident, sort_keys=True, separators=(",", ":")),
            node["dataType"], node["name"])

def attr_sig(node):
    """Qualifier is purely a name-resolution hint; all other JSON keys fixed.

    Source JVM metadata can still differ despite equal JSON, hence conditional.
    """
    ident, dtype, name = attr(node)
    return (ident, dtype, name,
            json.dumps({k:v for k,v in node.items() if k!="qualifier"},
                       sort_keys=True, separators=(",", ":")))

def identity_project(part):
    header, children = part
    check(header.get("class") == LOGICAL+"Project" and
          set(header) == {"class","num-children","child","projectList"} and
          header["num-children"] == 1 and header["child"] == 0 and
          len(children) == 1, "NOT_EXACT_PROJECT")
    leaf, leaf_children = children[0]
    check(leaf.get("class") in LEAVES and
          leaf.get("num-children") == 0 and not leaf_children and
          isinstance(leaf.get("output"), list) and leaf["output"],
          "NOT_SOURCE_LEAF")
    original = []
    for item in leaf["output"]:
        check(isinstance(item, list) and len(item) == 1,
              "SOURCE_OUTPUT_NONATTR")
        original.append(attr_sig(item[0]))
    check(len(set(x[0] for x in original)) == len(original),
          "DUPLICATE_LEAF_EXPR_ID")
    selected = []
    for item in header["projectList"]:
        check(isinstance(item, list) and len(item) == 1,
              "PROJECT_COMPUTED_EXPRESSION")
        selected.append(attr_sig(item[0]))
    check(bool(selected) and len(set(x[0] for x in selected)) == len(selected),
          "PROJECT_EMPTY_OR_DUPLICATED_ATTRS")
    expected = [x for x in original if x[0] in {v[0] for v in selected}]
    check(selected == expected, "NOT_EXACT_ORDERED_LEAF_PROJECTION")
    return {a[0]: a for a in selected}

def frame_to_join(before, after):
    """Exact Sort/Aggregate/Project frames, unchanged join header."""
    for name in ("Sort","Aggregate","Project"):
        h0, c0 = before
        h1, c1 = after
        check(h0 == h1 and h0.get("class") == LOGICAL+name and
              h0.get("num-children") == 1 and h0.get("child") == 0 and
              len(c0) == len(c1) == 1,
              "FRAME_NOT_IDENTICAL_"+name)
        before, after = c0[0],c1[0]
    h0, old_sides = before
    h1, new_sides = after
    check(h0 == h1 and h0.get("class") == LOGICAL+"Join" and
          h0.get("num-children") == 2 and
          h0.get("left") == 0 and h0.get("right") == 1 and
          len(old_sides) == len(new_sides) == 2,
          "JOIN_FRAME_CHANGED")
    join_obj = h0.get("joinType")
    check(isinstance(join_obj, dict) and set(join_obj) == {"object"} and
          join_obj["object"] in JOINS,"JOIN_KIND_UNSUPPORTED")
    cond = h0.get("condition")
    check(isinstance(cond, list) and len(cond) == 3,
          "PREDICATE_NOT_DIRECT_BINARY")
    operator, x, y = cond
    check(isinstance(operator, dict) and
          operator.get("class") in COMPARATORS and
          operator.get("num-children") == 2 and
          operator.get("left") == 0 and operator.get("right") == 1 and
          set(operator).issubset(
              {"class","num-children","left","right","evalContext"}),
          "PREDICATE_NOT_STRICT_COMPARISON")
    xa, ya = attr(x), attr(y)
    check(xa[1] == ya[1], "PREDICATE_MIXED_TYPES")
    return JOINS[join_obj["object"]], COMPARATORS[operator["class"]], \
           old_sides,new_sides, {xa[0],ya[0]}

def exact_inserted_guard(old, new, side_attrs, join_keys):
    header, children = new
    check(header.get("class") == LOGICAL+"Filter" and
          set(header) == {"class","num-children","condition","child"} and
          header["num-children"] == 1 and header["child"] == 0 and
          len(children) == 1 and children[0] == old,
          "NOT_EXACT_ADDED_FILTER")
    cond = header["condition"]
    check(isinstance(cond, list) and len(cond) == 2,
          "FILTER_NOT_SINGLE_NULL_GUARD")
    operator, operand = cond
    check(isinstance(operator, dict) and
          operator.get("class") == EXPR+"IsNotNull" and
          operator.get("num-children") == 1 and
          operator.get("child") == 0 and
          set(operator).issubset(
              {"class","num-children","child","evalContext"}),
          "FILTER_PREDICATE_NOT_ISNOTNULL")
    sig = attr_sig(operand)
    check(sig[0] in side_attrs and
          sig[1:] == side_attrs[sig[0]][1:] and
          sig[0] in join_keys,
          "GUARD_NOT_BOUND_TO_CROSS_SIDE_JOIN_KEY")

def observe(event):
    check(isinstance(event, dict) and
          event.get("old") != event.get("new") and
          isinstance(event.get("old"), list) and
          isinstance(event.get("new"), list),
          "NO_REAL_CHANGED_EVENT")
    kind, cmp, old_sides, new_sides, join_keys = frame_to_join(
        tree(event["old"]), tree(event["new"]))
    projected = [identity_project(part) for part in old_sides]
    check(set(projected[0]).isdisjoint(projected[1]),
          "AMBIGUOUS_CROSS_SIDE_BINDING")
    check(len(join_keys) == 2 and
          len(join_keys & set(projected[0])) == 1 and
          len(join_keys & set(projected[1])) == 1,
          "PREDICATE_NOT_TWO_BOUND_CROSS_SIDE_KEYS")
    guards = []
    for i,(old,new) in enumerate(zip(old_sides,new_sides)):
        if old != new:
            exact_inserted_guard(old, new, projected[i],join_keys)
            guards.append(i)
    check(bool(guards),"NO_GUARD_INSERTED")
    check(not (kind == "LeftOuter" and 0 in guards),
          "LEFT_OUTER_PRESERVED_SIDE_FILTER")
    encoding = json.dumps(
        {"old":event["old"],"new":event["new"]},
        sort_keys=True,separators=(",",":"),ensure_ascii=True).encode("ascii")
    return {
        "case": event.get("case"),
        "comparator":cmp, "join_kind":kind,
        "guard_left":0 in guards,"guard_right":1 in guards,
        "serialized_plan_pair_sha256":hashlib.sha256(encoding).hexdigest()
    }
