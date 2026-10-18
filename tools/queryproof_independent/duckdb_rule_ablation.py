"""Exhaustive isolated DuckDB optimizer ablation for a frozen nullable predicate."""
import json
import re
from pathlib import Path
import duckdb
from duckdb_null_join_matrix import (
    setup, QUERY_JOIN, QUERY_TRUTH, oracle, norm,
)

def evaluate(con):
    rows=[list(r) for r in con.execute(QUERY_JOIN).fetchall()]
    truth=[list(r) for r in con.execute(QUERY_TRUTH).fetchall()]
    expected=oracle()
    truth_expected=[x['predicate'] for x in expected['truth_table']]
    truth_actual=[None if x[-1] is None else bool(x[-1]) for x in truth]
    return {'join_rows':rows,'join_count':len(rows),
            'join_correct':norm(rows)==norm(expected['selected']),
            'truth_table_correct':truth_actual==truth_expected,
            'truth_predicate_actual':truth_actual,
            'truth_predicate_expected':truth_expected}

def main():
    con=duckdb.connect(':memory:')
    setup(con)
    names=[x[0] for x in con.execute('SELECT name FROM duckdb_optimizers() ORDER BY name').fetchall()]
    baseline=evaluate(con)
    explain_base=str(con.execute('EXPLAIN '+QUERY_JOIN).fetchall())[:22000]
    results=[]
    for name in names:
        if not re.fullmatch('[a-z_0-9]+',name):
            results.append({'name':name,'error':'UNSAFE_OPTIMIZER_IDENTIFIER'})
            continue
        try:
            con.execute("SET disabled_optimizers = '"+name+"'")
            observation=evaluate(con)
            row={'name':name,**observation}
            if observation['join_correct']:
                row['explain_corrected_join']=str(con.execute('EXPLAIN '+QUERY_JOIN).fetchall())[:22000]
            results.append(row)
            print('QUERYPROOF_DUCKDB_ABLATION',duckdb.__version__,name,
                  observation['join_correct'],observation['truth_table_correct'],
                  observation['join_count'],flush=True)
        except Exception as exc:
            row={'name':name,'error':type(exc).__name__+': '+str(exc)[:350]}
            results.append(row)
            print('QUERYPROOF_DUCKDB_ABLATION_ERROR',name,row['error'],flush=True)
        finally:
            con.execute("SET disabled_optimizers = ''")
    con.execute('PRAGMA disable_optimizer')
    unoptimized=evaluate(con)
    explain_off=str(con.execute('EXPLAIN '+QUERY_JOIN).fetchall())[:22000]
    con.close()
    fixed=[r['name'] for r in results
           if r.get('join_correct') and r.get('truth_table_correct')]
    summary={'duckdb_version':duckdb.__version__,'optimizer_rules_count':len(names),
             'fixes_both_join_and_predicate':fixed,
             'join_correct_base':baseline['join_correct'],
             'truth_correct_base':baseline['truth_table_correct'],
             'join_correct_optimizer_fully_disabled':unoptimized['join_correct'],
             'truth_correct_optimizer_fully_disabled':unoptimized['truth_table_correct'],
             'errors':[r for r in results if 'error' in r],
             'caveat':'One-rule ablation, not code-level causality or a new-bug novelty claim.'}
    Path('artifacts').mkdir(exist_ok=True)
    (Path('artifacts')/('queryproof_duckdb_rule_isolation_'+duckdb.__version__+'.json')).write_text(
        json.dumps({'summary':summary,'baseline':baseline,'unoptimized':unoptimized,
                    'explain_baseline':explain_base,'explain_unoptimized':explain_off,
                    'optimizer_single_rule_results':results},indent=2),encoding='utf8')
    print('QUERYPROOF_DUCKDB_ABLATION_SUMMARY',json.dumps(summary),flush=True)
    return 0

if __name__=='__main__':raise SystemExit(main())
