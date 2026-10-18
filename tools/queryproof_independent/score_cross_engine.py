"""Separate postexecution scoring for independent cross-engine reference."""
import json
import sys
from pathlib import Path

HERE=Path(__file__).resolve().parent
MANIFEST=json.loads((HERE.parent/'queryproof_prospective'/'manifest.json').read_text())


def canon(rows,mode):
    data=[json.dumps(x,sort_keys=True,default=str) for x in rows]
    return data if mode=='sequence' else sorted(data)


def main(path):
    result=json.loads(Path(path).read_text())
    findings={}
    for case in MANIFEST['cases']:
        if case['id'] not in result:continue
        run=result[case['id']]
        matches=(canon(run['rows'],case['comparison'])==canon(case['expected'],case['comparison'])
                 if run['status']=='OK' else False)
        findings[case['id']]={'status':run['status'],'frozen_expected_matches':matches,
                              'issue':case['issue'],'kind':case['kind'],
                              'observed':run.get('rows')}
        print('QUERYPROOF_DUCKDB_SCORE',case['id'],matches,flush=True)
    summary={'engine':'duckdb==1.4.1','cases_scored':len(findings),
             'bug_reference_consistent':sum(findings[k]['frozen_expected_matches'] for k in ('B01','B02','B03')),
             'controls_correct':sum(findings[k]['frozen_expected_matches'] for k in ('N01','N02','N03')),
             'errors':[k for k,v in findings.items() if v['status']!='OK'],
             'warning':'Independent engine oracle, NOT proof of general SQL equivalence or SQLancer comparison'}
    output={'summary':summary,'cases':findings}
    (Path('artifacts')/'queryproof_duckdb_score.json').write_text(json.dumps(output,indent=2))
    print('QUERYPROOF_DUCKDB_SUMMARY',json.dumps(summary),flush=True)
    return 1 if summary['errors'] or summary['bug_reference_consistent']!=3 or summary['controls_correct']!=3 else 0


if __name__=='__main__':raise SystemExit(main(sys.argv[1]))
