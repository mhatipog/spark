"""V19 (from V18, adds IsNull, IN and Add) independent differential evaluator for (OLD, NEW) plan-term pairs.

Parses the exact Coq terms the kernel saw, translates them to SQL, and runs both
in SQLite on randomized NULL- and duplicate-rich databases. This shares no code
with Coq or with print_terms. Opaque POp1 operators get the identity
interpretation (child columns, NULL for new output ids), which is one admissible
interpretation of check3_sound's universally quantified operators. A bag
difference on any trial is therefore a genuine semantic difference.

differs(old, new) -> True if some trial distinguishes them, False if none does,
None if the pair is outside this evaluator.
"""
import random
import re
import sqlite3
from collections import Counter

TOKEN = re.compile(r"\(|\)|\[|\]|;|%Z|-?\d+|[A-Za-z_][A-Za-z_0-9]*")
CMP = {"ELt": "<", "ELe": "<=", "EGt": ">", "EGe": ">=", "EEq": "="}


class Outside(ValueError):
    pass


class IllScoped(ValueError):
    """A reference to an attribute its input does not output: not a valid plan."""


class Parser:
    def __init__(self, text):
        self.toks = TOKEN.findall(text)
        self.i = 0

    def take(self, expected=None):
        tok = self.toks[self.i]
        self.i += 1
        if expected is not None and tok != expected:
            raise Outside(f"expected {expected} got {tok}")
        return tok

    def ids(self):
        self.take("[")
        out = []
        while self.toks[self.i] != "]":
            out.append(int(self.take()))
            if self.toks[self.i] == ";":
                self.take()
        self.take("]")
        return out

    def int_lit(self):
        self.take("(")
        z = int(self.take())
        self.take(")")
        self.take("%Z")
        return z

    def expr(self):
        self.take("(")
        head = self.take()
        if head == "EAttr":
            node = ("attr", int(self.take()))
        elif head == "ELit":
            if self.toks[self.i] == "None":
                self.take()
                node = ("lit", None)
            else:
                self.take("(")
                self.take("Some")
                self.take("(")
                self.take("VInt")
                node = ("lit", self.int_lit())
                self.take(")")
                self.take(")")
        elif head in CMP or head in ("ENullSafeEq", "EAnd", "EOr"):
            node = (head, self.expr(), self.expr())
        elif head == "EIsNotNull":
            node = ("isnotnull", self.expr())
        elif head == "EIsNull":
            node = ("isnull", self.expr())
        elif head == "EIn":
            value = self.expr()
            self.take("[")
            items = []
            while self.toks[self.i] != "]":
                if self.toks[self.i] == "None":
                    self.take()
                    items.append(None)
                else:
                    self.take("Some")
                    self.take("(")
                    self.take("VInt")
                    items.append(self.int_lit())
                    self.take(")")
                if self.toks[self.i] == ";":
                    self.take()
            self.take("]")
            node = ("in", value, list(items))
        elif head == "EAdd":
            self.take()                                   # width: overflow is not reachable
            node = ("add", self.expr(), self.expr())      # with this evaluator's small values
        else:
            raise Outside("expr " + head)
        self.take(")")
        return node

    def plan(self):
        self.take("(")
        head = self.take()
        if head == "PLeaf":
            node = ("leaf", int(self.take()), self.ids())
        elif head == "PFilter":
            node = ("filter", self.expr(), self.plan())
        elif head == "PProject":
            node = ("project", self.ids(), self.plan())
        elif head == "POp1":
            node = ("op", int(self.take()), self.ids(), self.plan())
        elif head == "PJoin":
            kind = self.take()
            if self.toks[self.i] == "None":
                self.take()
                cond = None
            else:
                self.take("(")
                self.take("Some")
                cond = self.expr()
                self.take(")")
            node = ("join", kind, cond, self.plan(), self.plan())
        else:
            raise Outside("plan " + head)
        self.take(")")
        return node


def parse(text):
    p = Parser(text)
    node = p.plan()
    if p.i != len(p.toks):
        raise Outside("trailing tokens")
    return node


def out(node):
    kind = node[0]
    if kind == "leaf":
        return node[2]
    if kind == "filter":
        return out(node[2])
    if kind == "project":
        return node[1]
    if kind == "op":
        return node[2]
    return out(node[3]) + out(node[4])


def expr_refs(e, acc):
    if e[0] == "attr":
        acc.add(e[1])
    else:
        for x in e[1:]:
            if isinstance(x, tuple):
                expr_refs(x, acc)
    return acc


def check_scope(p):
    kind = p[0]
    if kind == "filter":
        if not expr_refs(p[1], set()) <= set(out(p[2])):
            raise IllScoped("filter")
        check_scope(p[2])
    elif kind == "project":
        if not set(p[1]) <= set(out(p[2])):
            raise IllScoped("project")
        check_scope(p[2])
    elif kind == "op":
        check_scope(p[3])
    elif kind == "join":
        if p[2] is not None and not expr_refs(p[2], set()) <= set(out(p[3]) + out(p[4])):
            raise IllScoped("join")
        check_scope(p[3])
        check_scope(p[4])


def sql_expr(e):
    kind = e[0]
    if kind == "attr":
        return f"c{e[1]}"
    if kind == "lit":
        return "NULL" if e[1] is None else str(e[1])
    if kind in CMP:
        return f"({sql_expr(e[1])} {CMP[kind]} {sql_expr(e[2])})"
    if kind == "ENullSafeEq":
        return f"({sql_expr(e[1])} IS {sql_expr(e[2])})"
    if kind == "EAnd":
        return f"({sql_expr(e[1])} AND {sql_expr(e[2])})"
    if kind == "EOr":
        return f"({sql_expr(e[1])} OR {sql_expr(e[2])})"
    if kind == "isnotnull":
        return f"({sql_expr(e[1])} IS NOT NULL)"
    if kind == "isnull":
        return f"({sql_expr(e[1])} IS NULL)"
    if kind == "in":
        items = ", ".join("NULL" if v is None else str(v) for v in e[2])
        return f"({sql_expr(e[1])} IN ({items}))"
    if kind == "add":
        return f"({sql_expr(e[1])} + {sql_expr(e[2])})"
    raise Outside(kind)


class Sql:
    def __init__(self):
        self.n = 0

    def alias(self):
        self.n += 1
        return f"t{self.n}"

    def plan(self, p):
        kind = p[0]
        if kind == "leaf":
            return f"SELECT {', '.join(f'c{i}' for i in p[2])} FROM leaf{p[1]}"
        if kind == "filter":
            return f"SELECT * FROM ({self.plan(p[2])}) {self.alias()} WHERE {sql_expr(p[1])}"
        if kind == "project":
            return f"SELECT {', '.join(f'c{i}' for i in p[1])} FROM ({self.plan(p[2])}) {self.alias()}"
        if kind == "op":
            # Opaque operator k: a k-dependent deterministic row filter plus
            # projection (NULL for output ids the child lacks). One admissible
            # interpretation of check3_sound's universally quantified ops,
            # and it distinguishes different operator descriptors.
            child_out = out(p[3])
            have = set(child_out)
            cols = ", ".join(f"c{i}" if i in have else f"NULL AS c{i}" for i in p[2])
            first = f"c{child_out[0]}" if child_out else None
            keep = (f" WHERE {first} IS NULL OR (({first} + {p[1]}) % 3) <> 0"
                    if first else "")       # NULL rows always kept: no masking
            return f"SELECT {cols} FROM ({self.plan(p[3])}) {self.alias()}{keep}"
        _, jk, cond, left, right = p
        if set(out(left)) & set(out(right)):
            raise Outside("overlapping join outputs")
        on = sql_expr(cond) if cond is not None else "1"
        word = "JOIN" if jk == "Inner" else "LEFT JOIN"
        cols = ", ".join(f"c{i}" for i in out(left) + out(right))
        return (f"SELECT {cols} FROM ({self.plan(left)}) {self.alias()} {word} "
                f"({self.plan(right)}) {self.alias()} ON {on}")


def leaves(p, acc):
    if p[0] == "leaf":
        acc[p[1]] = p[2]
    else:
        for child in p[1:]:
            if isinstance(child, tuple) and child and child[0] in (
                    "leaf", "filter", "project", "op", "join"):
                leaves(child, acc)
    return acc


ILL_SCOPED = "ILL_SCOPED"


def differs(old_text, new_text, trials=40, seed=0):
    """True / False / None (outside) / ILL_SCOPED (either side invalid)."""
    try:
        old, new = parse(old_text), parse(new_text)
        check_scope(old)
        check_scope(new)
        if out(old) != out(new):
            return True
        tabs, new_tabs = leaves(old, {}), leaves(new, {})
        # A leaf on one side only (e.g. Spark's empty LocalRelation) has
        # contents we cannot randomize consistently: outside this evaluator.
        if tabs != new_tabs:
            raise Outside("leaf sets differ")
        q_old, q_new = Sql().plan(old), Sql().plan(new)
    except IllScoped:
        return ILL_SCOPED
    except (Outside, IndexError):
        return None
    rng = random.Random(seed)
    for _ in range(trials):
        db = sqlite3.connect(":memory:")
        for k, cols in tabs.items():
            db.execute(f"CREATE TABLE leaf{k} ({', '.join(f'c{c} INTEGER' for c in cols)})")
            rows = [[None if rng.random() < 0.3 else rng.randint(-2, 2) for _ in cols]
                    for _ in range(rng.randint(0, 6))]
            rows += [list(rng.choice(rows)) for _ in range(rng.randint(0, 2))] if rows else []
            if rows:
                db.executemany(f"INSERT INTO leaf{k} VALUES ({', '.join('?' * len(cols))})", rows)
        try:
            a = Counter(db.execute(q_old).fetchall())
            b = Counter(db.execute(q_new).fetchall())
        except sqlite3.Error:
            return None
        if a != b:
            return True
    return False
