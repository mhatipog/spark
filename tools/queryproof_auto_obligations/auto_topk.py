"""Conservative automatic bounded TopK witness for one Spark CTE plan shape.

Input: analyzed Catalyst plan, SparkSession, returned rows.
No SQL parsing, pre-named source table, ordering key, ID, or LIMIT/OFFSET
is supplied. Unsupported logical operators -> UNKNOWN.

Research prototype: independent source-view reread; no guarantee against
source mutation or correlated bugs in the source scan itself. Not general.
"""
import time
from plan_extractor import _cls, _items


MAX_SOURCE_ROWS=64
NUMERIC={'byte','short','integer','long'}


class NotEligible(Exception):
    pass


def require(condition, why):
    if not condition:raise NotEligible(why)


def output(node):
    return _items(node.output())


def eid(node):
    return int(node.exprId().id())


def literal_int(expr):
    require(_cls(expr)=='Literal','NON_LITERAL_BOUNDARY')
    value=int(expr.value())
    require(0<=value<=MAX_SOURCE_ROWS,'BOUNDARY_OUT_OF_SCOPE')
    return value


def pure_project(node):
    require(_cls(node)=='Project','EXPECTED_IDENTITY_PROJECT')
    xs=_items(node.projectList())
    require(xs and all(_cls(x)=='AttributeReference' for x in xs),
            'DERIVED_STAGE_PROJECTION')
    ids=[eid(x) for x in xs]
    require(len(ids)==len(set(ids)),'DUPLICATE_STAGE_EXPRID')
    return xs


def extract_stage(df):
    """Return source name, id/key/identity names and boundary, no data read."""
    try:
        root=df._jdf.queryExecution().analyzed()
        require(_cls(root)=='WithCTE','EXPECTED_WITHCTE')
        definitions=_items(root.cteDefs())
        require(len(definitions)==1,'CTE_COUNT_NOT_ONE')
        definition=definitions[0]
        require(_cls(definition)=='CTERelationDef','EXPECTED_CTE_DEF')
        final=root.plan()
        require(_cls(final)=='Project','EXPECTED_DIRECT_OUTPUT_PROJECT')
        final_child=final.child()
        require(_cls(final_child)=='SubqueryAlias' and
                _cls(final_child.child())=='CTERelationRef','EXPECTED_CTE_OUTPUT')
        cte_ref=final_child.child()
        require(int(cte_ref.cteId())==int(definition.id()),'CTE_ID_MISMATCH')
        eligible_cte_ids={eid(x) for x in output(cte_ref)}
        direct=[(idx,x) for idx,x in enumerate(_items(final.projectList()))
                if _cls(x)=='AttributeReference' and eid(x) in eligible_cte_ids]
        require(len(direct)==1,'AMBIGUOUS_OR_DERIVED_OUTPUT_IDENTITY')
        output_index, identity = direct[0]
        ident_eid=eid(identity)
        require(str(identity.dataType().typeName())=='string','IDENTITY_TYPE_NOT_STRING')

        cte_alias=definition.child()
        require(_cls(cte_alias)=='SubqueryAlias','EXPECTED_CTE_ALIAS')
        boundary=cte_alias.child()
        kind=_cls(boundary)
        require(kind in ('GlobalLimit','Offset'),'UNSUPPORTED_BOUNDARY')
        if kind=='GlobalLimit':
            q=literal_int(boundary.limitExpr())
            limit=q; offset=0
            step=boundary.child()
            require(_cls(step)=='LocalLimit' and literal_int(step.limitExpr())==q,
                    'UNEXPECTED_LOCAL_LIMIT')
            project=step.child()
        else:
            offset=literal_int(boundary.offsetExpr())
            limit=None
            project=boundary.child()
        stage_refs=pure_project(project)
        require(ident_eid in {eid(x) for x in stage_refs},'IDENTITY_NOT_IN_STAGE')
        the_sort=project.child()
        require(_cls(the_sort)=='Sort' and bool(getattr(the_sort,'global')()),
                'NO_GLOBAL_SORT_IN_STAGE')
        orders=_items(the_sort.order())
        require(len(orders)==1 and _cls(orders[0].child())=='AttributeReference',
                'UNSUPPORTED_STAGE_SORT')
        key=orders[0].child()
        require(str(orders[0].direction().sql())=='DESC' and
                str(orders[0].nullOrdering().sql())=='NULLS LAST',
                'UNSUPPORTED_SORT_POLICY')
        require(str(key.dataType().typeName()) in NUMERIC,'NONINTEGRAL_SORT_KEY')
        lower=pure_project(the_sort.child())
        source=the_sort.child().child()
        require(_cls(source)=='SubqueryAlias','SOURCE_NOT_DIRECT_ALIAS')
        alias=str(source.identifier().name())
        require(_cls(source.child())=='View','SOURCE_NOT_REGISTERED_VIEW')
        leaf=source.child().child()
        if _cls(leaf)=='Repartition':leaf=leaf.child()
        require(_cls(leaf)=='LogicalRDD','SOURCE_NOT_SIMPLE_IN_MEMORY_RELATION')
        source_attrs=output(source)
        source_by_id={eid(x):x for x in source_attrs}
        require(len(source_by_id)==len(source_attrs),'SOURCE_DUPLICATE_EXPR_ID')
        require(set(eid(x) for x in lower)==set(source_by_id),
                'LOWER_PROJECTION_NOT_IDENTITY')
        require(ident_eid in source_by_id and eid(key) in source_by_id,
                'IDENTITY_OR_ORDER_NOT_SOURCE_MAPPED')
        identity_src=source_by_id[ident_eid]
        key_src=source_by_id[eid(key)]
        id_candidates=[x for x in stage_refs if eid(x)!=ident_eid and
                       eid(x) in source_by_id and
                       str(x.dataType().typeName()) in NUMERIC]
        require(len(id_candidates)==1,'UNIQUE_INTEGRAL_ID_NOT_ESTABLISHED')
        raw_id=id_candidates[0]
        require(len({eid(raw_id),ident_eid,eid(key)})==3,'ID_KEY_IDENTITY_OVERLAP')
        id_name,identity_name,key_name=(str(source_by_id[eid(x)].name())
                                         for x in (raw_id,identity_src,key_src))
        require(len({id_name,identity_name,key_name})==3,'AMBIGUOUS_SOURCE_COLUMNS')
        return {'status':'CERTIFICATE_CANDIDATE','source_view':alias,
                'id_column':id_name,'identity_column':identity_name,
                'sort_column':key_name,'output_index':output_index,
                'offset':offset,'limit':limit,'max_rows':MAX_SOURCE_ROWS,
                'cte_id':int(definition.id()),'evidence':'Catalyst exprId + direct source stage'}
    except (NotEligible,Exception) as exc:
        return {'status':'UNKNOWN','reason':str(exc)[:280] or type(exc).__name__,
                'exception':type(exc).__name__}


def check(ss, df, *, extracted=None):
    """Check pre-execution analyzed plan and independent stage source snapshot.

    Run before df.collect() so both source and target can be compared.
    """
    contract=extracted or extract_stage(df)
    if contract['status']!='CERTIFICATE_CANDIDATE':
        return contract
    t0=time.perf_counter()
    try:
        from topk_certificate import local_topq, merge_topq, witness
        source=ss.table(contract['source_view'])
        names=[contract['id_column'],contract['identity_column'],contract['sort_column']]
        require(len(set(source.columns))==len(source.columns),'DUPLICATE_SOURCE_FIELD_NAMES')
        require(all(x in source.columns for x in names),'SOURCE_SCHEMA_CHANGED')
        limited=source.select(*names).limit(MAX_SOURCE_ROWS+1).collect()
        require(len(limited)<=MAX_SOURCE_ROWS,'SOURCE_TOO_LARGE')
        triples=[(int(x[0]),str(x[1]),int(x[2])) for x in limited]
        require(all(x[1] is not None and x[2] is not None for x in limited),
                'NULL_INPUT_NOT_SUPPORTED')
        require(len(set(x[0] for x in triples))==len(triples),'NONUNIQUE_SOURCE_ID')
        require(len(set(x[1] for x in triples))==len(triples),'NONUNIQUE_SOURCE_IDENTITY')
        require(len(set(x[2] for x in triples))==len(triples),'TIED_ORDER_KEY')
        q=(contract['limit'] if contract['limit'] is not None else len(triples))
        require(q+contract['offset']<=MAX_SOURCE_ROWS,'SELECTION_BOUNDARY_TOO_LARGE')
        # Apply partition algebra, not a full Spark SQL top-k query.
        rdd=source.select(*names).rdd
        summaries=rdd.mapPartitions(lambda part:iter(
            local_topq(((int(x[0]),str(x[1]),int(x[2])) for x in part),q))).collect()
        full=merge_topq([summaries],q)
        expected=[x[3] for x in full[contract['offset']:
                                  contract['offset']+(contract['limit'] or len(full))]]
        rows=[list(row) for row in df.collect()]
        actual=[str(row[contract['output_index']]) for row in rows]
        result=witness(expected,actual)
        return {**contract,'status':result['verdict'],
                'oracle':result,'source_rows':len(triples),
                'expected':expected,'actual':actual,
                'elapsed_ms':round((time.perf_counter()-t0)*1000,2),
                'source_data_assumption':'immutable replayable in-memory view',
                'known_limits':'No proof that mutable source stayed unchanged between scans'}
    except NotEligible as exc:
        return {'status':'UNKNOWN','reason':str(exc),'source_view':contract['source_view']}
    except Exception as exc:
        return {'status':'UNKNOWN','reason':'INSPECTION_OR_SOURCE_ERROR',
                'detail':type(exc).__name__+': '+str(exc)[:300]}
