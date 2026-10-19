"""Compositional exact certificate through unchanged pure Project frames.

Given identical, passthrough/renaming-only Project ancestors, bag equivalence
of their input plans lifts to bag equivalence of their output plans. At the
nontrivial core use the separately proven INNER JOIN guard or Filter/Project
commutation checker. This checker does not inspect or trust Catalyst rule names.
"""
from equality_orbits import Unsupported, require
from join_prefilter_certificate import (
    certify_inner_join_prefilter_event, tree_from_preorder, LOGICAL, INNER,
)
from project_pushdown_certificate import (
    certify_project_filter_event, project_substitution,
)
from join_filter_transport_certificate import certify_inner_join_filter_transport


def flat(tree):
    node, children = tree
    return [node, *(item for child in children for item in flat(child))]


def output_scope(tree):
    node, children = tree
    klass = node.get("class")
    if klass == LOGICAL + "Project":
        require(len(children) == 1, "BAD_SCOPED_PROJECT")
        return set(project_substitution(node))
    if klass == LOGICAL + "Filter":
        require(len(children) == 1 and node.get("child") == 0,
                "BAD_SCOPED_FILTER")
        return output_scope(children[0])
    if klass == LOGICAL + "Join":
        require(node.get("num-children") == 2 and len(children) == 2
                and node.get("left") == 0 and node.get("right") == 1
                and node.get("joinType") == INNER,
                "UNSUPPORTED_JOIN_FOR_FRAME_SCOPING")
        left = output_scope(children[0])
        right = output_scope(children[1])
        require(left.isdisjoint(right), "FRAME_JOIN_OUTPUT_IDS_OVERLAP")
        return left | right
    raise Unsupported("UNKNOWN_FRAME_CHILD_OUTPUT_SCOPE")


def _certify(event):
    old, new = tree_from_preorder(event["old"]), tree_from_preorder(event["new"])
    frames = []
    while True:
        a, ac = old
        b, bc = new
        if (a.get("class") != LOGICAL + "Project" or
                b.get("class") != LOGICAL + "Project"):
            break
        require(a == b and len(ac) == len(bc) == 1,
                "FRAME_PROJECT_CHANGED")
        mapping = project_substitution(a)
        src = set(mapping.values())
        require(src <= output_scope(ac[0]), "FRAME_OLD_PROJECTION_OUT_OF_SCOPE")
        require(src <= output_scope(bc[0]), "FRAME_NEW_PROJECTION_OUT_OF_SCOPE")
        frames.append({"columns": len(mapping),
                       "aliases": sum(k != v for k, v in mapping.items())})
        old, new = ac[0], bc[0]
    require(frames, "NO_PURE_PROJECT_FRAMES")
    core_event = {"old": flat(old), "new": flat(new)}
    guard = certify_inner_join_prefilter_event(core_event)
    if guard["status"] == "EQUIVALENT_IN_FRAGMENT":
        return {
            "status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "compositional_pure_project_frame",
            "core": "inner_join_implied_filter_insertion",
            "frames": len(frames), "alias_outputs": sum(x["aliases"] for x in frames),
            "sqlite_verified": guard["sqlite_verified"],
            "orbits_checked_per_core": [guard["orbits_checked"]]}
    swap = certify_project_filter_event(core_event)
    if swap["status"] == "EQUIVALENT_IN_FRAGMENT":
        return {
            "status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "compositional_pure_project_frame",
            "core": "filter_project_commutation",
            "frames": len(frames), "alias_outputs": sum(x["aliases"] for x in frames),
            "sqlite_verified": swap["sqlite_verified"],
            "orbits_checked_per_core": swap["orbits_checked_per_branch"]}
    transfer = certify_inner_join_filter_transport(core_event)
    if transfer["status"] == "EQUIVALENT_IN_FRAGMENT":
        return {
            "status": "EQUIVALENT_IN_FRAGMENT",
            "proof_scope": "compositional_pure_project_frame",
            "core": "inner_join_filter_transport",
            "frames": len(frames), "alias_outputs": sum(x["aliases"] for x in frames),
            "sqlite_verified": transfer["sqlite_verified"],
            "orbits_checked_per_core": [transfer["orbits_checked"]]}
    raise Unsupported("UNDERLYING_CORE_NOT_CERTIFIED")


def certify_pure_project_framed_event(event):
    try:
        return _certify(event)
    except (Unsupported, KeyError, TypeError, IndexError, AttributeError,
            ValueError, RecursionError) as error:
        return {"status": "UNKNOWN", "reason": str(error)}
