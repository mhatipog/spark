"""SQL 3VL semantic discrimination for a historical nullable JOIN predicate.

No assumption that either Spark or DuckDB is correct: Python and SQLite
independently derive the SQL-TRUE row set, DuckDB evaluates alternative
equivalent SQL forms, and PRAGMA disable_optimizer tests planner involvement.
"""
import json
import sqlite3
from pathlib import Path

import duckdb

HERE=Path(__file__).resolve().parent
DATA_LEFT=[(0,10),(None,11)]
DATA_RIGHT=[(None,20),(0,21),(1,22)]
PREDICATE='NOT (tl.a = tr.x OR (tl.a IS NULL AND tr.x IS NULL))'
COLS='tl.a,tl.b,tr.x,tr.y'
QUERY_JOIN='SELECT '+COLS+' FROM tl INNER JOIN tr ON '+PREDICATE+' ORDER BY tl.b,tr.y'
QUERY_WHERE='SELECT '+COLS+' FROM tl CROSS JOIN tr WHERE '+PREDICATE+' ORDER BY tl.b,tr.y'
QUERY_EXPLICIT='SELECT '+COLS+' FROM tl INNER JOIN tr ON (tl.a IS NOT NULL AND tr.x IS NOT NULL AND tl.a <> tr.x) ORDER BY tl.b,tr.y'
QUERY_NULL_DISTINCT='SELECT '+COLS+' FROM tl INNER JOIN tr ON (tl.a IS DISTINCT FROM tr.x) ORDER BY tl.b,tr.y'
QUERY_TRUTH=('SELECT '+COLS+',tl.a = tr.x AS eq, '
    '(tl.a IS NULL AND tr.x IS NULL) AS both_null, '
    +PREDICATE+' AS predicate FROM tl CROSS JOIN tr ORDER BY tl.b,tr.y')
QUERY_MINIMAL=('SELECT tl.a,tr.x FROM (VALUES (0)) tl(a) INNER JOIN '
    '(VALUES (CAST(NULL AS INTEGER))) tr(x) ON '
    'NOT(tl.a = tr.x OR (tl.a IS NULL AND tr.x IS NULL))')


def tri_not(p):
    return None if p is None else not p


def tri_or(a,b):
    return True if (a is True or b is True) else None if (a is None or b is None) else False


def oracle():
    pairs=[]
    for a,b in DATA_LEFT:
        for x,y in DATA_RIGHT:
            p=tri_not(tri_or(None if a is None or x is None else (a==x),
                             a is None and x is None))
            pairs.append({'row':[a,b,x,y],'predicate':p})
    return {'truth_table':pairs,
            'selected':[item['row'] for item in pairs if item['predicate'] is True]}


def setup(conn):
    conn.execute('CREATE TABLE tl(a INTEGER, b INTEGER)')
    conn.execute('CREATE TABLE tr(x INTEGER, y INTEGER)')
    conn.executemany('INSERT INTO tl VALUES (?,?)',DATA_LEFT)
    conn.executemany('INSERT INTO tr VALUES (?,?)',DATA_RIGHT)


def queries(conn,sqlite=False):
    result={}
    sqls={'join_not_equal_or_both_null':QUERY_JOIN,
          'cross_join_where':QUERY_WHERE,
          'explicit_join_match':QUERY_EXPLICIT,
          'null_distinct_deliberately_different':QUERY_NULL_DISTINCT,
          'predicate_truth':QUERY_TRUTH,
          'one_pair_null_mismatch':QUERY_MINIMAL}
    for label,sql in sqls.items():
        # SQLite uses IS NOT for NULL-aware inequality.
        if sqlite and label=='null_distinct_deliberately_different':
            sql=sql.replace('tl.a IS DISTINCT FROM tr.x','tl.a IS NOT tr.x')
        try:
            result[label]=[list(row) for row in conn.execute(sql).fetchall()]
        except Exception as exc:
            result[label]={'error':type(exc).__name__+': '+str(exc)[:300]}
    if not sqlite:
        for label in ('join_not_equal_or_both_null','cross_join_where'):
            sql=sqls[label]
            try:
                result['explain_'+label]=str(conn.execute('EXPLAIN '+sql).fetchall())[:12000]
            except Exception as exc:
                result['explain_'+label]={'error':repr(exc)}
    return result


def norm(rows):
    return sorted(json.dumps(row,sort_keys=True) for row in rows)


def main():
    out={'duckdb_version':duckdb.__version__,'sqlite_version':sqlite3.sqlite_version,
         'source':'SPARK-58384 historical frozen B01 join predicate',
         'gold_python_3vl':oracle()}
    sq=sqlite3.connect(':memory:')
    try:
        setup(sq)
        out['sqlite']=queries(sq,sqlite=True)
    finally:sq.close()
    optimized=duckdb.connect(':memory:')
    try:
        setup(optimized)
        out['duckdb_optimizer_on']=queries(optimized)
    finally:optimized.close()
    unoptimized=duckdb.connect(':memory:')
    try:
        setup(unoptimized)
        unoptimized.execute('PRAGMA disable_optimizer')
        out['duckdb_optimizer_off']=queries(unoptimized)
    except Exception as exc:
        out['duckdb_optimizer_off']={'error':type(exc).__name__+': '+str(exc)[:500]}
    finally:unoptimized.close()
    expected=out['gold_python_3vl']['selected']
    expected_table=[x['predicate'] for x in out['gold_python_3vl']['truth_table']]
    out['checks']={}
    for label,result in [('sqlite',out['sqlite']),('duckdb_on',out['duckdb_optimizer_on']),
                         ('duckdb_off',out['duckdb_optimizer_off'])]:
        if 'error' in result:
            out['checks'][label]={'error':result['error']}
            continue
        join=result['join_not_equal_or_both_null']
        where=result['cross_join_where']
        explicit=result['explicit_join_match']
        truth=result['predicate_truth']
        out['checks'][label]={
            'join_matches_python_3vl':norm(join)==norm(expected),
            'where_matches_python_3vl':norm(where)==norm(expected),
            'explicit_join_matches_python_3vl':norm(explicit)==norm(expected),
            'join_matches_cross_where':norm(join)==norm(where),
            'observed_join_rows':len(join),
            'truth_table_matches_3vl':([None if r[-1] is None else bool(r[-1]) for r in truth]
                ==expected_table if isinstance(truth,list) else False),
            'one_pair_null_mismatch_no_rows':result['one_pair_null_mismatch']==[],
        }
    print('QUERYPROOF_3VL_CROSSENGINE',duckdb.__version__,json.dumps(out['checks']),flush=True)
    Path('artifacts').mkdir(exist_ok=True)
    (Path('artifacts')/('queryproof_duckdb_3vl_'+duckdb.__version__+'.json')).write_text(
        json.dumps(out,indent=2),encoding='utf8')
    # A DuckDB discrepancy is a *finding*, not a CI harness failure.
    # SQLite and Python model must agree before we interpret any failure.
    if not out['checks']['sqlite']['join_matches_python_3vl']:
        return 2
    if not out['checks']['sqlite']['truth_table_matches_3vl']:
        return 3
    return 0


if __name__=='__main__':raise SystemExit(main())
