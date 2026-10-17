"""Conservative Spark Catalyst analyzed-plan ORDER BY obligation extraction.

Only *analyzed* logical plans: no string scraping of EXPLAIN and no SQL regex.
Return UNKNOWN on nontrivial roots, derived keys, unsupported comparator types,
unexposed attributes, ambiguous projection rewrites, and all errors.

A returned key specification asserts only a checkable necessary condition.
It is NOT a complete query correctness proof, nor a physical plan invariant.
"""
from __future__ import annotations

SAFE_ROOT_WRAPPERS = {
    'WithCTE': 'plan',
    'GlobalLimit': 'child',
    'LocalLimit': 'child',
    'Offset': 'child',
    'SubqueryAlias': 'child',
}
INTEGRAL_TYPES = {'byte', 'short', 'integer', 'long'}


def _cls(node):
    return str(node.getClass().getSimpleName())


def _items(scala_seq, limit=512):
    count = int(scala_seq.size())
    if count > limit:
        raise ValueError('PLAN_SEQUENCE_TOO_LARGE')
    return [scala_seq.apply(i) for i in range(count)]


def _output(node):
    return _items(node.output())


def _find_stage_boundaries(root, max_nodes=200):
    """Diagnostic only; a boundary alone is NOT a validated TopK witness."""
    pending = [(root, ())]; visited = 0; candidates = []
    while pending:
        node, path = pending.pop()
        visited += 1
        if visited > max_nodes:
            return [{'status': 'UNKNOWN', 'reason': 'PLAN_TOO_LARGE'}]
        kind = _cls(node)
        new_path = path + (kind,)
        if kind in ('GlobalLimit', 'Offset'):
            candidates.append({'kind': kind, 'path': list(new_path),
                               'status': 'SOURCE_NOT_CERTIFIED'})
        try:
            children = _items(node.children(), max_nodes)
        except Exception:
            return [{'status': 'UNKNOWN', 'reason': 'CHILDREN_UNAVAILABLE'}]
        pending.extend((c, new_path) for c in reversed(children))
    return candidates


def extract(qe):
    """Extract exposed keys of a root *global* Sort from qe.analyzed().

    Supplying the resulting keys to contract.audit must not be confused with
    proving sortedness, source membership, or correctness of scalar outputs.
    """
    try:
        analyzed = qe.analyzed()
        root = analyzed
        route = []
        for _ in range(18):
            kind = _cls(root)
            if kind not in SAFE_ROOT_WRAPPERS:
                break
            route.append(kind)
            root = getattr(root, SAFE_ROOT_WRAPPERS[kind])()
        else:
            return {'verdict': 'UNKNOWN', 'reason': 'WRAPPER_DEPTH_EXCEEDED'}
        if _cls(root) != 'Sort':
            return {'verdict': 'UNKNOWN', 'reason': 'NO_ROOT_GLOBAL_SORT',
                    'unwrapped': route, 'root': _cls(root),
                    'stage_boundaries': _find_stage_boundaries(analyzed)}
        if not bool(root.global_() if hasattr(root, 'global_') else getattr(root, 'global')()):
            return {'verdict': 'UNKNOWN', 'reason': 'PARTITION_LOCAL_SORT_ONLY'}
        exposed = _output(analyzed)
        attr_ids = {}
        for index, attr in enumerate(exposed):
            # ExprId (not a textual column name) disambiguates joins/aliases.
            eid = int(attr.exprId().id())
            if eid in attr_ids:
                return {'verdict': 'UNKNOWN', 'reason': 'DUPLICATE_OUTPUT_EXPR_ID'}
            attr_ids[eid] = (index, str(attr.name()), str(attr.dataType().typeName()))
        keys = []
        for order in _items(root.order(), 64):
            expr = order.child()
            if _cls(expr) != 'AttributeReference':
                return {'verdict': 'UNKNOWN', 'reason': 'DERIVED_OR_NON_ATTRIBUTE_SORT_KEY',
                        'unwrapped': route}
            expr_id = int(expr.exprId().id())
            if expr_id not in attr_ids:
                return {'verdict': 'UNKNOWN', 'reason': 'SORT_KEY_NOT_EXPOSED',
                        'unwrapped': route}
            index, name, data_type = attr_ids[expr_id]
            if data_type not in INTEGRAL_TYPES:
                return {'verdict': 'UNKNOWN', 'reason': 'UNSUPPORTED_KEY_TYPE:'+data_type}
            direction = str(order.direction().sql())
            nulls = str(order.nullOrdering().sql())
            if direction not in ('ASC', 'DESC') or nulls not in ('NULLS FIRST', 'NULLS LAST'):
                return {'verdict': 'UNKNOWN', 'reason': 'UNSUPPORTED_ORDER_POLICY'}
            keys.append({'index': index, 'direction': direction,
                         'nulls': nulls.split()[-1]})
        if not keys:
            return {'verdict': 'UNKNOWN', 'reason': 'EMPTY_SORT_KEY_LIST'}
        return {'verdict': 'EXPOSED_ORDER_CONTRACT',
                'keys': keys, 'key_names': [attr_ids[int(x.child().exprId().id())][1]
                                          for x in _items(root.order(),64)],
                'root_wrappers': route,
                'stage_boundaries': _find_stage_boundaries(analyzed),
                'not_proven': ['source membership', 'SQL equivalence',
                               'physical ordering', 'correct aggregate values']}
    except Exception as exc:
        return {'verdict': 'UNKNOWN', 'reason': 'INSPECTION_ERROR',
                'detail': type(exc).__name__+': '+str(exc)[:350]}
