"""Test actual Catalyst before/after optimizer rewrites, without rule-name hints.

Rule names are recorded AFTER classification, not used as a whitelist.
No expected query output rows or frozen oracle fields are used by proof
generation; only after proof do we score B01 and N01 calibration outcomes.
"""
import json,sys
from pathlib import Path
from collections import defaultdict,Counter

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'queryproof_instrumented'))
from rank_ast import decode
from context_proof import certificate_for_transition
from sqlite_check import independently_verify

def main(trace):
    evidence=defaultdict(list)
    total=Counter()
    for line in Path(trace).read_text(errors='replace').splitlines():
        if not line.startswith('QUERYPROOF_AST\t'):continue
        ev=decode(line)
        total[ev['case']]+=1
        result=certificate_for_transition(ev)
        if result['status'] in ('COUNTEREXAMPLE','EQUIVALENT_IN_FRAGMENT'):
            result['independent_verifier']=independently_verify(result)
            evidence[ev['case']].append(result)
            print('QUERYPROOF_CONTEXT_CERT',ev['case'],ev['rule'],result['status'],
                  'demand',result['required_partition'],
                  'witness',bool(result['minimal_counterexample']),
                  'verified',result['independent_verifier']['verified'],flush=True)
        else:
            # Retain abstention reason and event but not full AST of unsupported
            # transformations to keep public output bounded.
            evidence[ev['case']].append({
                'status':'UNKNOWN','reason':result.get('reason'),
                'rule':ev['rule']})
    supported={case:[x for x in cases
                 if x['status'] in ('COUNTEREXAMPLE','EQUIVALENT_IN_FRAGMENT')]
               for case,cases in evidence.items()}
    calibrations={
      'B01_has_verified_counterexample':any(
          e['status']=='COUNTEREXAMPLE' and e['independent_verifier']['verified']
          for e in supported.get('B01',[])),
      'N01_has_verified_contextually_safe_rewrite':any(
          e['status']=='EQUIVALENT_IN_FRAGMENT' and e['independent_verifier']['verified']
          and e['independent_verifier']['root_full_value_differences']>0
          for e in supported.get('N01',[])),
      'N01_has_no_observable_counterexample':not any(
          e['status']=='COUNTEREXAMPLE' for e in supported.get('N01',[])),
    }
    summary={'cases':sorted(evidence),
             'total_ast_events':dict(total),
             'supported_rule_transitions':{k:len(v) for k,v in supported.items()},
             'unknown_rule_transitions':{k:sum(e['status']=='UNKNOWN' for e in v)
                                         for k,v in evidence.items()},
             'calibration':calibrations,
             'main_claim':'Local contextual equivalence for a matched, unchanged INNER JOIN condition over two nullable integral attributes; not general Spark proof',
             'independence':'Both proof generator and SQLite witness verifier run before any known-answer scoring'}
    Path('artifacts').mkdir(exist_ok=True)
    (Path('artifacts')/'queryproof_contextual_rule_certificates.json').write_text(
        json.dumps({'summary':summary,'events':dict(evidence)},indent=2),encoding='utf8')
    print('QUERYPROOF_CONTEXT_CERT_SUMMARY',json.dumps(summary),flush=True)
    return 0 if all(calibrations.values()) else 1

if __name__=='__main__':raise SystemExit(main(sys.argv[1]))
