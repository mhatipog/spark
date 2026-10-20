"""Round U posthoc: two separate local laws for actual Spark join filter rule steps.

1. InferFiltersFromConstraints: add IsNotNull guards on equijoin keys only.
   INNER: either/both sides. LEFT OUTER: right side ONLY, never preserved left.
2. PushDownPredicates: commute the SAME IsNotNull filter through an exact
   identity Project, with all referenced fields retained.

Both require exact Sort/Aggregate/Project frames, pure integral EqualTo join,
unchanged source leaves, exact exprIds and explicit NULL-key semantics. This
is only a conditional DEFINED row-bag certificate; no error/UDF/order proof.
"""
from equality_orbits import Unsupported, require
from join_prefilter_certificate import LOGICAL, EXPR, tree_from_preorder
from collapse_project_certificate import _attribute, _identity
from grouping_repetition_certificate import _checked_aggregate_references
from join_project_v1 import _leaf_attributes, _identity_prune, _condition, JOIN_TYPES

NULL_GUARD = EXPR + "IsNotNull"


def _frames_to_join(event):
    old = tree_from_preorder(event["old"])
    new = tree_from_preorder(event["new"])
    require(old != new, "NO_CHANGED_EVENT")
    for op in ("Sort", "Aggregate", "Project"):
        h, children = old
        h2, children2 = new
        require(h == h2 and h.get("class") == LOGICAL + op
                and h.get("num-children") == 1 and h.get("child") == 0
                and len(children) == len(children2) == 1,
                "UNCHANGED_FRAME_NOT_" + op.upper())
        if op == "Aggregate":
            _checked_aggregate_references(h)
        if op == "Project":
            expressions = h.get("projectList")
            require(isinstance(expressions, list) and expressions,
                    "TOP_PROJECT_MISSING")
            for item in expressions:
                require(isinstance(item, list) and len(item) == 1,
                        "TOP_PROJECT_COMPUTED")
                _attribute(item[0])
        old, new = children[0], children2[0]
    original, oc = old
    transformed, tc = new
    require(original == transformed and
            original.get("class") == LOGICAL + "Join" and
            original.get("num-children") == 2 and
            original.get("left") == 0 and original.get("right") == 1 and
            len(oc) == len(tc) == 2,
            "JOIN_FRAME_OR_METADATA_CHANGED")
    kind = original.get("joinType")
    require(isinstance(kind, dict) and set(kind) == {"object"}
            and kind["object"] in JOIN_TYPES, "JOIN_TYPE_NOT_SUPPORTED")
    return original, oc, tc, JOIN_TYPES[kind["object"]]


def _source_project(tree):
    project, pc = tree
    require(project.get("class") == LOGICAL + "Project" and
            project.get("num-children") == 1 and project.get("child") == 0
            and len(pc) == 1, "JOIN_INPUT_NOT_IDENTITY_PROJECT")
    desc, order = _leaf_attributes(pc[0])
    kept, changed = _identity_prune(pc[0], tree, desc, order)
    require(changed, "JOIN_INPUT_PROJECT_REQUIRED")
    selected = {key: desc[key] for key in kept}
    return selected, project, pc[0]


def _is_not_null(filter_header, retained):
    require(filter_header.get("class") == LOGICAL + "Filter" and
            filter_header.get("num-children") == 1 and
            filter_header.get("child") == 0 and
            set(filter_header) == {"class", "num-children", "condition", "child"},
            "FILTER_METADATA_NOT_EXACT")
    items = filter_header.get("condition")
    require(isinstance(items, list) and len(items) == 2,
            "FILTER_NOT_UNARY_IS_NOT_NULL")
    operator, reference = items
    require(operator.get("class") == NULL_GUARD and
            operator.get("num-children") == 1 and
            operator.get("child") == 0 and
            set(operator).issubset({"class", "num-children", "child", "evalContext"}),
            "FILTER_NOT_IS_NOT_NULL")
    key, dtype, name = _attribute(reference)
    require(key in retained and (dtype, name) == retained[key][:2] and
            [reference] == retained[key][2],
            "FILTER_ATTRIBUTE_NOT_EXACT_RETAINED_IDENTITY")
    return key


def _infer(event):
    join, before, after, kind = _frames_to_join(event)
    baseline = [_source_project(side)[0] for side in before]
    refs = _condition(join, baseline[0], baseline[1])
    filters = []
    for i, (old, new) in enumerate(zip(before, after)):
        if old == new:
            continue
        hdr, children = new
        require(hdr.get("class") == LOGICAL + "Filter" and
                len(children) == 1 and children[0] == old,
                "INFERENCE_NOT_EXACT_INSERTED_FILTER")
        key = _is_not_null(hdr, baseline[i])
        require(key in refs, "INFERRED_NULL_GUARD_NOT_A_JOIN_KEY")
        require(not (kind == "left" and i == 0),
                "LEFT_OUTER_PRESERVED_SIDE_FILTER_UNSOUND")
        filters.append(i)
    require(filters, "NO_INFERRED_FILTERS")
    return {"status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "NULL_INTOLERANT_EQUALTO_IMPLIED_KEY_GUARDS",
            "join_kind": kind, "filter_sides": filters,
            "semantic_law": "Eq(a,b)=TRUE requires both non-NULL; preserve all unmatched left rows"}


def _push(event):
    join, before, after, kind = _frames_to_join(event)
    moved = []
    initial_outputs = []
    for i, (old, new) in enumerate(zip(before, after)):
        if old == new:
            outputs, _, _ = _source_project(old)
            initial_outputs.append(outputs)
            continue
        filter_before, fchildren = old
        require(filter_before.get("class") == LOGICAL + "Filter" and
                len(fchildren) == 1, "PUSH_BEFORE_NOT_FILTER")
        projected_before = fchildren[0]
        retained, project_header, leaf = _source_project(projected_before)
        key = _is_not_null(filter_before, retained)
        new_project, new_children = new
        require(new_project == project_header and len(new_children) == 1,
                "PUSH_CHANGED_IDENTITY_PROJECT")
        new_filter, filtered_children = new_children[0]
        require(new_filter == filter_before and
                len(filtered_children) == 1 and filtered_children[0] == leaf,
                "PUSH_NOT_EXACT_FILTER_PROJECT_COMMUTE")
        initial_outputs.append(retained)
        moved.append((i, key))
    require(moved, "NO_PUSHED_FILTERS")
    refs = _condition(join, initial_outputs[0], initial_outputs[1])
    require(all(key in refs for _, key in moved),
            "PUSH_FILTER_NOT_JOIN_KEY")
    return {"status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "IS_NOT_NULL_FILTER_IDENTITY_PROJECT_COMMUTATION",
            "join_kind": kind, "moved_sides": [i for i, _ in moved],
            "semantic_law": "sigma_g(pi_S(R)) = pi_S(sigma_g(R)) when g reads only S"}


def certify_inferred_filter_event(event):
    try:
        return _infer(event)
    except (Unsupported, KeyError, TypeError, IndexError,
            AttributeError, ValueError, RecursionError) as e:
        return {"status": "UNKNOWN", "reason": str(e)}


def certify_filter_project_push_event(event):
    try:
        return _push(event)
    except (Unsupported, KeyError, TypeError, IndexError,
            AttributeError, ValueError, RecursionError) as e:
        return {"status": "UNKNOWN", "reason": str(e)}
