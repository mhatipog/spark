"""Frozen prospective test: automatically extract contracts from Catalyst plans.

Comparison key specifications in the pre-existing manifest are *scoring labels*
and are NEVER passed to plan_extractor.extract().
"""
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from plan_extractor import extract
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'queryproof_observed_contract'))
from contract import audit

ROOT = Path(__file__).resolve().parent
MANIFEST = json.loads((ROOT.parent/'queryproof_observed_contract'/'manifest.json').read_text())
PROTOCOL = json.loads((ROOT/'FROZEN_PROTOCOL.json').read_text())


def execute(ss, test_case, aqe, calibration=False):
    ss.conf.set('spark.sql.adaptive.enabled', str(aqe).lower())
    ss.conf.set('spark.sql.shuffle.partitions', '5')
    if calibration:
        for name, value in test_case.get('config', {}).items():
            ss.conf.set(name,value)
    statement = test_case['sql']
    df = ss.sql(statement)
    result = extract(df._jdf.queryExecution())
    rows = [list(row) for row in df.collect()]
    result['auto_order_observation'] = (
        audit(rows, result['keys']) if result['verdict']=='EXPOSED_ORDER_CONTRACT'
        else {'verdict':'UNKNOWN','reason':result['reason']})
    result['manual_oracle_observation'] = audit(rows, test_case['keys'])
    result['case_id'] = test_case['id']; result['aqe'] = aqe
    result['calibration'] = calibration; result['row_count'] = len(rows)
    result['expected_key_match'] = (
        result.get('keys') == test_case['keys'] if test_case['keys'] else
        result['verdict'] == 'UNKNOWN'
    )
    return result


def main():
    spark=(SparkSession.builder.master('local[2]').appName('QueryProof automatic plan contracts')
           .config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    report=[]
    try:
        for entry in MANIFEST['cases']+[MANIFEST['calibration']]:
            case=dict(entry)
            if entry is MANIFEST['calibration']:
                case.setdefault('id',case['id'])
            for aqe in (False,True):
                n=3 if entry is MANIFEST['calibration'] else 1
                for iteration in range(n):
                    try:
                        item=execute(spark.newSession(),case,aqe,entry is MANIFEST['calibration'])
                    except Exception as exc:
                        item={'case_id':case['id'],'aqe':aqe,'calibration':entry is MANIFEST['calibration'],
                              'error': type(exc).__name__+': '+str(exc)[:800],
                              'verdict':'ERROR'}
                    item['iteration']=iteration
                    report.append(item)
                    print('QUERYPROOF_AUTO',spark.version,case['id'],
                          'AQE_ON' if aqe else 'AQE_OFF',iteration,
                          item.get('verdict'),item.get('reason',''),
                          item.get('auto_order_observation',{}).get('verdict'),
                          item.get('expected_key_match'),flush=True)
        holdout=[x for x in report if not x['calibration']]
        calibration=[x for x in report if x['calibration']]
        supported=[x for x in holdout if x['verdict']=='EXPOSED_ORDER_CONTRACT']
        errors=[x for x in report if x['verdict']=='ERROR']
        false_accusations=[x for x in holdout if x.get('auto_order_observation',{}).get('verdict')=='CONTRACT_VIOLATED'
                           and x.get('manual_oracle_observation',{}).get('verdict')!='CONTRACT_VIOLATED']
        mismatched=[x for x in supported if not x.get('expected_key_match')]
        # A successful program run is not evidence that extracted keys matched the
        # gold reference; preserve mismatches explicitly in the scientific report.
        summary={'spark_version':spark.version,'frozen_protocol_date':PROTOCOL['date'],
                 'holdout_executions':len(holdout),'covered':len(supported),
                 'unknown':sum(x['verdict']=='UNKNOWN' for x in holdout),
                 'matched_gold_keys':sum(x.get('expected_key_match') is True for x in supported),
                 'mismatched_extracted_key_cases':[(x['case_id'],x.get('keys')) for x in mismatched],
                 'holdout_false_accusations':len(false_accusations),
                 'known_bug_detected':sum(x.get('auto_order_observation',{}).get('verdict')=='CONTRACT_VIOLATED'
                                           for x in calibration if x['aqe']),
                 'known_bug_aqe_on_runs':sum(x['aqe'] for x in calibration),
                 'run_errors':errors,
                 'meaning':'A positive plan extraction is a necessary observable ordering contract, never a full SQL proof'}
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts')/('queryproof_auto_obligations_'+spark.version+'.json')).write_text(
            json.dumps({'summary':summary,'results':report},indent=2),encoding='utf8')
        print('QUERYPROOF_AUTO_SUMMARY',json.dumps(summary),flush=True)
        # Fail the CI on a source API crash or an apparent unsound extraction.
        # An UNKNOWN is an honest research outcome, not a CI failure.
        return 1 if errors or false_accusations or mismatched else 0
    finally:
        spark.stop()


if __name__=='__main__':sys.exit(main())
