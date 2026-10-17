"""Read-only structural introspection for choosing a safe automated TopK scope."""
import json
from pathlib import Path
from pyspark.sql import SparkSession

ROWS=[(1,'a',30),(2,'b',20),(3,'c',40),(4,'d',60),(5,'e',10),(6,'f',50)]
REL=[(1,2),(1,3),(1,5),(2,3)]
ROOT=Path(__file__).resolve().parent


def cls(node):return str(node.getClass().getSimpleName())


def seq(s):return [s.apply(i) for i in range(s.size())]


def explore(node, depth=0, budget=None):
    if budget is None:budget=[75]
    if budget[0]<=0 or depth>20:return []
    budget[0]-=1
    kind=cls(node)
    line={'depth':depth,'kind':kind,
          'output':[(str(a.name()),int(a.exprId().id()))
                    for a in seq(node.output())]}
    try:
        if kind=='SubqueryAlias':
            line['alias']=str(node.identifier().name())
        if kind=='Sort':
            line['global']=str(getattr(node,'global')())
            line['order']=[{'expr':str(o.child()),'class':cls(o.child()),
                            'direction':str(o.direction().sql()),
                            'nulls':str(o.nullOrdering().sql())}
                           for o in seq(node.order())]
        if kind in ('GlobalLimit','LocalLimit'):
            line['limitExpr']=str(node.limitExpr())
        if kind=='Offset':
            line['offsetExpr']=str(node.offsetExpr())
        if kind=='Project':
            line['projectList']=[str(v) for v in seq(node.projectList())]
    except Exception as exc:
        line['field_error']=str(exc)[:150]
    lines=[line]
    for child in seq(node.children()):
        lines.extend(explore(child,depth+1,budget))
    return lines


def main():
    spark=(SparkSession.builder.master('local[2]').appName('QueryProof stage schema inspection')
           .config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    ss=spark.newSession()
    ss.createDataFrame(ROWS,'id INT, name STRING, age INT').repartition(5).createOrReplaceTempView('p')
    ss.createDataFrame(REL,'a INT, b INT').createOrReplaceTempView('k')
    cases={
        'correlated_limit':"WITH s AS (SELECT id, name FROM p ORDER BY age DESC LIMIT 3) SELECT name,(SELECT count(*) FROM k WHERE k.a=s.id) AS c FROM s",
        'correlated_offset':"WITH s AS (SELECT id, name FROM p ORDER BY age DESC OFFSET 3) SELECT name,EXISTS(SELECT 1 FROM k WHERE k.a=s.id) AS e FROM s",
        'safe_scalar':"WITH s AS (SELECT id, name FROM p ORDER BY age DESC LIMIT 3) SELECT name,(SELECT max(b) FROM k) AS c FROM s",
        'safe_plain':"WITH s AS (SELECT id, name FROM p ORDER BY age DESC LIMIT 3) SELECT name FROM s",
        'not_direct_source':"WITH s AS (SELECT id, name FROM p WHERE age > 20 ORDER BY age DESC LIMIT 3) SELECT name FROM s",
    }
    results={}
    try:
        for label,sql in cases.items():
            try:
                plan=ss.sql(sql)._jdf.queryExecution().analyzed()
                data={'tree':str(plan.treeString()),'nodes':explore(plan)}
                results[label]=data
                print('QUERYPROOF_PLAN_SHAPE',label,json.dumps(data),flush=True)
            except Exception as e:
                print('QUERYPROOF_PLAN_ERROR',label,str(e)[:800],flush=True)
                results[label]={'error':repr(e)}
    finally:
        spark.stop()
    Path('artifacts').mkdir(exist_ok=True)
    (Path('artifacts')/'queryproof_stage_shapes.json').write_text(json.dumps(results,indent=2))
    return 1 if any('error' in a for a in results.values()) else 0


if __name__=='__main__':raise SystemExit(main())
