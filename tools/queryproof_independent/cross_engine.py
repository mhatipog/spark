"""Independent DuckDB 1.4.1 reference: executable cross-engine control, not SQLancer.

Only six existing frozen ANSI-style SQL cases. No SQL expected answers here.
SQL adapter modifies Spark range() output *column alias*, nothing semantic.
"""
import json
import re
from pathlib import Path
import duckdb

ROOT=Path(__file__).resolve().parent
MANIFEST=json.loads((ROOT.parent/'queryproof_prospective'/'manifest.json').read_text())
CONFIG=json.loads((ROOT/'FROZEN_CROSS_ENGINE.json').read_text())


def adapt(sql):
    # DuckDB's range function returns column named "range", Spark names it "id".
    # Only add the column alias to a bare FROM range(...) table function.
    return re.sub(r'\bFROM\s+range\((\s*\d+\s*(?:,\s*\d+\s*)?)\)',
                  r'FROM range(\1) AS __queryproof_range(id)',sql,
                  flags=re.IGNORECASE)


def fixture(con,key):
    if key=='nullable_joins':
        con.execute('CREATE TABLE tl (a INTEGER,b INTEGER)')
        con.execute('CREATE TABLE tr (x INTEGER,y INTEGER)')
        con.executemany('INSERT INTO tl VALUES (?,?)',[(0,10),(None,11)])
        con.executemany('INSERT INTO tr VALUES (?,?)',[(None,20),(0,21),(1,22)])
    elif key=='in_null':
        con.execute('CREATE TABLE tn (c INTEGER)')
        con.execute('CREATE TABLE t (c INTEGER)')
        con.executemany('INSERT INTO tn VALUES (?)',[(1,),(2,),(None,)])
        con.executemany('INSERT INTO t VALUES (?)',[(1,),(2,)])
    elif key=='none':
        pass
    else:
        raise ValueError('unrecognized historical fixture '+str(key))


def run():
    results={}
    for case in MANIFEST['cases']:
        if case['id'] not in CONFIG['case_ids']:continue
        con=duckdb.connect(':memory:')
        try:
            fixture(con,case['fixture'])
            sql=adapt(case['sql'])
            results[case['id']]={'status':'OK','issue':case['issue'],
                'original_sql':case['sql'],'adapted_sql':sql,
                'comparison':case['comparison'],
                'rows':[list(x) for x in con.execute(sql).fetchall()],
                'duckdb_version':duckdb.__version__}
        except Exception as exc:
            results[case['id']]={'status':'ERROR',
                'error':type(exc).__name__+': '+str(exc)[:600]}
        finally:
            con.close()
        print('QUERYPROOF_DUCKDB',case['id'],results[case['id']]['status'],
              results[case['id']].get('rows','NO_ROWS'),flush=True)
    Path('artifacts').mkdir(exist_ok=True)
    (Path('artifacts')/'queryproof_duckdb_reference.json').write_text(
        json.dumps(results,indent=2),encoding='utf8')
    return 1 if any(x['status']=='ERROR' for x in results.values()) or set(results)!=set(CONFIG['case_ids']) else 0


if __name__=='__main__':
    raise SystemExit(run())
