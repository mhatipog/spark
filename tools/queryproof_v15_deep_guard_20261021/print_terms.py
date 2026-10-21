"""V15 printer: Catalyst TreeNode JSON -> QueryProofDeepGuard plan terms.

This is the ONLY trusted translation step. It never decides acceptance: it
prints OLD/NEW terms, and the Coq kernel evaluates `check old new`.
Anything outside the grammar raises Unsupported (counted as UNKNOWN,
never as proof). Opaque nodes are printed as POp1/PLeaf with a descriptor
index: equal canonical JSON (children excluded) within one event gets
the same index, so the kernel can only match them by identity.
"""
import json

INTEGRAL = {"integer", "long", "short", "byte"}
CAT = "org.apache.spark.sql.catalyst."
COMPARE = {
    CAT + "expressions.LessThan": "ELt",
    CAT + "expressions.LessThanOrEqual": "ELe",
    CAT + "expressions.GreaterThan": "EGt",
    CAT + "expressions.GreaterThanOrEqual": "EGe",
    CAT + "expressions.EqualTo": "EEq",
    CAT + "expressions.EqualNullSafe": "ENullSafeEq",
}
CONNECTIVE = {CAT + "expressions.And": "EAnd", CAT + "expressions.Or": "EOr"}
ATTR = CAT + "expressions.AttributeReference"
ALIAS = CAT + "expressions.Alias"
LEAVES = {"org.apache.spark.sql.execution.LogicalRDD",
          CAT + "plans.logical.LocalRelation"}
JOIN_KINDS = {CAT + "plans.Inner$": "Inner", CAT + "plans.LeftOuter$": "LeftOuter"}
# Opaque unary operators whose output attribute list we can read from JSON.
OPAQUE_UNARY = {CAT + "plans.logical.Sort", CAT + "plans.logical.Aggregate",
                CAT + "plans.logical.Project", CAT + "plans.logical.GlobalLimit",
                CAT + "plans.logical.LocalLimit", CAT + "plans.logical.Distinct"}


class Unsupported(ValueError):
    pass


def tree(nodes):
    """Preorder TreeNode JSON list -> (node, children)."""
    def descend(pos):
        node = nodes[pos]
        count = node.get("num-children")
        if type(count) is not int or count < 0:
            raise Unsupported("BAD_CHILD_COUNT")
        kids, end = [], pos + 1
        for _ in range(count):
            kid, end = descend(end)
            kids.append(kid)
        return (node, kids), end
    if not isinstance(nodes, list) or not nodes:
        raise Unsupported("EMPTY_TREE")
    root, end = descend(0)
    if end != len(nodes):
        raise Unsupported("TRAILING_NODES")
    return root


class Printer:
    def __init__(self):
        self.ids = {}
        self.descriptors = {}

    def attr_id(self, node):
        if node.get("class") != ATTR:
            raise Unsupported("NOT_ATTRIBUTE " + str(node.get("class")))
        if node.get("dataType") not in INTEGRAL:
            raise Unsupported("NON_INTEGRAL_ATTRIBUTE")
        e = node["exprId"]
        key = (e["id"], e["jvmId"])
        return self.ids.setdefault(key, len(self.ids))

    def descriptor(self, node):
        key = json.dumps(node, sort_keys=True)
        return self.descriptors.setdefault(key, len(self.descriptors))

    def expr(self, nodes):
        (node, kids) = tree(nodes)
        return self._expr(node, kids)

    def _expr(self, node, kids):
        cls = node.get("class")
        if cls == ATTR:
            return f"(EAttr {self.attr_id(node)})"
        if cls == CAT + "expressions.Literal":
            if node.get("dataType") not in INTEGRAL:
                raise Unsupported("NON_INTEGRAL_LITERAL")
            v = node.get("value")
            return "(ELit None)" if v is None else f"(ELit (Some (VInt ({int(v)})%Z)))"
        if cls in COMPARE or cls in CONNECTIVE:
            if len(kids) != 2:
                raise Unsupported("BINARY_ARITY")
            op = COMPARE.get(cls) or CONNECTIVE[cls]
            return f"({op} {self._expr(*kids[0])} {self._expr(*kids[1])})"
        if cls == CAT + "expressions.IsNotNull":
            if len(kids) != 1:
                raise Unsupported("UNARY_ARITY")
            return f"(EIsNotNull {self._expr(*kids[0])})"
        raise Unsupported("EXPRESSION " + str(cls).split(".")[-1])

    def ids_of(self, attr_lists):
        return [self.attr_id(tree(a)[0]) for a in attr_lists]

    def named_ids(self, exprs):
        """Output ids of a projectList/aggregateExpressions (top-level)."""
        out = []
        for e in exprs:
            node = tree(e)[0]
            if node.get("class") == ATTR:
                out.append(self.attr_id(node))
            elif node.get("class") == ALIAS:
                k = node["exprId"]
                out.append(self.ids.setdefault((k["id"], k["jvmId"]), len(self.ids)))
            else:
                raise Unsupported("UNNAMED_OUTPUT")
        return out

    def plan(self, nodes):
        return self._plan(tree(nodes))

    def _plan(self, t):
        node, kids = t
        cls = node.get("class")
        if cls in LEAVES:
            if kids:
                raise Unsupported("LEAF_WITH_CHILDREN")
            out = self.ids_of(node["output"])
            return f"(PLeaf {self.descriptor(node)} {coq_list(out)})", out
        if cls == CAT + "plans.logical.Filter":
            if set(node) != {"class", "num-children", "condition", "child"}:
                raise Unsupported("FILTER_FIELDS")
            child, out = self._plan(kids[0])
            return f"(PFilter {self.expr(node['condition'])} {child})", out
        if cls == CAT + "plans.logical.Join":
            kind = JOIN_KINDS.get(node.get("joinType", {}).get("object"))
            if kind is None:
                raise Unsupported("JOIN_TYPE")
            if node.get("hint") is not None:
                raise Unsupported("JOIN_HINT")
            extra = set(node) - {"class", "num-children", "left", "right",
                                 "joinType", "condition", "hint"}
            if extra:
                raise Unsupported("JOIN_FIELDS " + ",".join(sorted(extra)))
            cond = node.get("condition") or []
            c = "None" if not cond else f"(Some {self.expr(cond)})"
            (l, lo), (r, ro) = self._plan(kids[0]), self._plan(kids[1])
            return f"(PJoin {kind} {c} {l} {r})", lo + ro
        if len(kids) != 1:
            raise Unsupported("UNSUPPORTED_NODE " + str(cls).split(".")[-1])
        child, child_out = self._plan(kids[0])
        if cls == CAT + "plans.logical.Project":
            plist = [tree(e)[0] for e in node["projectList"]]
            if all(p.get("class") == ATTR for p in plist):
                ids = self.ids_of(node["projectList"])
                return f"(PProject {coq_list(ids)} {child})", ids
            out = self.named_ids(node["projectList"])
        elif cls == CAT + "plans.logical.Aggregate":
            out = self.named_ids(node["aggregateExpressions"])
        elif cls in OPAQUE_UNARY:
            out = child_out
        else:
            raise Unsupported("UNSUPPORTED_NODE " + str(cls).split(".")[-1])
        return f"(POp1 {self.descriptor(node)} {coq_list(out)} {child})", out


def coq_list(xs):
    return "[" + "; ".join(str(x) for x in xs) + "]"


def print_pair(old_nodes, new_nodes):
    """Returns (old_term, new_term); one Printer so ids/descriptors are shared."""
    p = Printer()
    return p.plan(old_nodes)[0], p.plan(new_nodes)[0]
