"""Exact extensional row-bag certificate for a pure CollapseProject transition.

For a fixed input relation R, pi_E(pi_F(R)) = pi_H(R) whenever H is
the pointwise substitution of F into E. We check that identity syntactically
for direct resolved attributes, aliases of attributes, and typed literals.
All expressions are deterministic and total; NULLs and duplicates are preserved.

This proves ordered *column values with bag row multiplicities*, not Spark
physical ordering, cost, evaluation timing, or arbitrary Catalyst expressions.
The arbitrary unchanged R is treated as a fixed bag. We never trust rule labels.
"""
import json

from equality_orbits import Unsupported, require
from join_prefilter_certificate import tree_from_preorder, LOGICAL, EXPR

LEAVES = {"org.apache.spark.sql.execution.LogicalRDD",
          LOGICAL + "LocalRelation"}
DTYPES = {"integer", "long", "short", "byte", "string", "boolean",
          "double", "float", "decimal", "date", "timestamp"}
BAG_JOINS = {"Inner$", "LeftOuter$", "RightOuter$", "FullOuter$", "Cross$"}
JOIN_TYPE_PREFIX = "org.apache.spark.sql.catalyst.plans."


def _identity(node):
    require(isinstance(node, dict), "INVALID_EXPRESSION_NODE")
    value = node.get("exprId")
    require(isinstance(value, dict) and type(value.get("id")) is int
            and isinstance(value.get("jvmId"), str), "INVALID_OUTPUT_ID")
    return json.dumps(value, sort_keys=True)


def _attribute(node):
    require(isinstance(node, dict) and
            node.get("class") == EXPR + "AttributeReference" and
            node.get("num-children") == 0, "NOT_A_DIRECT_ATTRIBUTE")
    require(node.get("dataType") in DTYPES, "UNSUPPORTED_ATTRIBUTE_TYPE")
    return _identity(node), node["dataType"], node.get("name")


def _literal(node):
    require(isinstance(node, dict) and
            node.get("class") == EXPR + "Literal" and
            node.get("num-children") == 0, "NOT_A_LITERAL")
    require(node.get("dataType") in DTYPES, "UNSUPPORTED_LITERAL_TYPE")
    require("value" in node, "LITERAL_VALUE_ABSENT")
    return ("literal", node["dataType"],
            json.dumps(node, sort_keys=True, separators=(",", ":")))


def _project_items(node, input_scope, substitution=None):
    require(node.get("class") == LOGICAL + "Project" and
            node.get("num-children") == 1 and node.get("child") == 0 and
            set(node) == {"class", "num-children", "projectList", "child"},
            "INVALID_PROJECT_METADATA")
    items = node["projectList"]
    require(isinstance(items, list) and bool(items), "INVALID_PROJECT_LIST")
    mapping, descriptions, output_scope = {}, [], {}
    for item in items:
        require(isinstance(item, list) and 1 <= len(item) <= 2,
                "COMPLEX_OR_MALFORMED_PROJECTION")
        if len(item) == 1:
            output_id, dtype, name = _attribute(item[0])
            reference = output_id
        else:
            head, target = item
            require(head.get("class") == EXPR + "Alias" and
                    head.get("num-children") == 1 and
                    head.get("child") == 0, "NON_DIRECT_ALIAS")
            output_id = _identity(head)
            name = head.get("name")
            if target.get("class") == EXPR + "AttributeReference":
                reference, dtype, _ = _attribute(target)
            else:
                atom = _literal(target)
                dtype = atom[1]
                reference = None
        if reference is not None:
            require(reference in input_scope, "PROJECT_SOURCE_OUT_OF_SCOPE")
            require(input_scope[reference] == dtype,
                    "PROJECT_ATTRIBUTE_TYPE_DISAGREEMENT")
            value = (substitution[reference] if substitution is not None
                     else ("attribute", dtype, reference))
        else:
            value = atom
        require(output_id not in mapping, "DUPLICATE_PROJECT_EXPR_ID")
        mapping[output_id] = value
        output_scope[output_id] = dtype
        descriptions.append((output_id, dtype, name))
    return mapping, descriptions, output_scope


def _input_scope(tree):
    node, children = tree
    klass = node.get("class")
    if klass in LEAVES:
        require(node.get("num-children") == 0 and len(children) == 0,
                "INVALID_LEAF_SHAPE")
        entries = node.get("output")
        require(isinstance(entries, list) and entries, "MISSING_LEAF_OUTPUT")
        scope = {}
        for entry in entries:
            require(isinstance(entry, list) and len(entry) == 1,
                    "MALFORMED_LEAF_OUTPUT")
            key, dtype, _ = _attribute(entry[0])
            require(key not in scope, "DUPLICATE_LEAF_EXPR_ID")
            scope[key] = dtype
        return scope
    if klass == LOGICAL + "Join":
        require(node.get("num-children") == 2 and len(children) == 2 and
                node.get("left") == 0 and node.get("right") == 1,
                "INVALID_JOIN_SCOPE")
        kind = node.get("joinType")
        require(isinstance(kind, dict) and
                kind.get("object", "").startswith(JOIN_TYPE_PREFIX) and
                kind["object"][len(JOIN_TYPE_PREFIX):] in BAG_JOINS,
                "UNSUPPORTED_JOIN_OUTPUT_SHAPE")
        left, right = _input_scope(children[0]), _input_scope(children[1])
        require(left.keys().isdisjoint(right), "DUPLICATE_JOIN_EXPR_ID")
        return {**left, **right}
    if klass in (LOGICAL + "Filter", LOGICAL + "Sort",
                 LOGICAL + "SubqueryAlias"):
        require(node.get("num-children") == 1 and len(children) == 1 and
                node.get("child") == 0, "BAD_UNARY_INPUT_SCOPE")
        return _input_scope(children[0])
    if klass == LOGICAL + "Project":
        require(len(children) == 1, "BAD_NESTED_PROJECT")
        _, _, outputs = _project_items(node, _input_scope(children[0]))
        return outputs
    raise Unsupported("UNKNOWN_INPUT_OUTPUT_SCHEMA")


def _certify(event):
    old = tree_from_preorder(event["old"])
    new = tree_from_preorder(event["new"])
    frame_count = 0
    while old[0].get("class") == LOGICAL + "Sort":
        require(new[0] == old[0] and old[0].get("num-children") == 1
                and old[0].get("child") == 0 and
                len(old[1]) == len(new[1]) == 1,
                "SORT_FRAME_CHANGED")
        old, new = old[1][0], new[1][0]
        frame_count += 1
    before, before_children = old
    after, after_children = new
    require(before.get("class") == after.get("class") == LOGICAL + "Project"
            and len(before_children) == len(after_children) == 1,
            "NOT_PROJECT_COLLAPSE")
    lower, lower_children = before_children[0]
    require(lower.get("class") == LOGICAL + "Project" and
            len(lower_children) == 1 and
            after_children[0] == lower_children[0],
            "NOT_ONE_ELIMINATED_PROJECT")
    input_scope = _input_scope(lower_children[0])
    inner, _, middle_scope = _project_items(lower, input_scope)
    original, before_schema, _ = _project_items(
        before, middle_scope, substitution=inner)
    merged, after_schema, _ = _project_items(after, input_scope)
    require(before_schema == after_schema, "OUTPUT_SCHEMA_OR_ID_CHANGED")
    require(list(original.values()) == list(merged.values()),
            "PROJECTION_COMPOSITION_NOT_IDENTICAL")
    # Same base subtree and matching symbolic expression for each corresponding
    # field imply one equal output tuple per input row, including duplicate bags.
    return {"status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "pure_project_substitution_row_bag",
            "columns": len(original),
            "eliminated_project_columns": len(inner),
            "sort_frames": frame_count,
            "literal_outputs": sum(value[0] == "literal"
                                   for value in original.values()),
            "semantic_law": "pi_E(pi_F(R)) == pi_(E composed F)(R) as row bags"}


def certify_collapse_project_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as exc:
        return {"status": "UNKNOWN", "reason": str(exc)}
