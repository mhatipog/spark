"""Prospective read-only SQL ordering observations on unmodified PySpark wheels.

The manifest was committed before this harness. Runs do not tune cases or SQL.
A violated exposed-key order is decisive; not violated never certifies row-bag or LIMIT.
"""
import json
import sys
import time
from pathlib import Path

from contract import audit
from pyspark.sql import SparkSession


HERE = Path(__file__).resolve().parent
MANIFEST = json.loads((HERE / 'manifest.json').read_text(encoding='utf8'))


def run_query(spark, case, aqe, calibration=False):
    ss = spark.newSession()
    ss.conf.set('spark.sql.adaptive.enabled', str(aqe).lower())
    ss.conf.set('spark.sql.shuffle.partitions', '5')
    if calibration:
        for key, val in case.get('config', {}).items():
            ss.conf.set(key, val)
    started = time.perf_counter()
    try:
        rows = [list(row) for row in ss.sql(case['sql']).collect()]
        result = audit(iter(rows), case['keys'])
        result.update({'id': case['id'], 'aqe': aqe, 'calibration': calibration,
                       'row_count': len(rows), 'elapsed_ms': round((time.perf_counter()-started)*1000,1)})
        return result
    except Exception as exc:
        return {'id': case['id'], 'aqe': aqe, 'calibration': calibration,
                'verdict': 'ERROR', 'error': repr(exc)}


def main():
    spark = (SparkSession.builder.master('local[2]')
             .appName('QueryProof observed ORDER BY, frozen holdout')
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    all_results = []
    try:
        for case in MANIFEST['cases']:
            for aqe in (False, True):
                result = run_query(spark, case, aqe)
                all_results.append(result)
                print('QUERYPROOF_CONTRACT', spark.version, case['id'],
                      'AQE_ON' if aqe else 'AQE_OFF', result['verdict'], flush=True)
        for aqe in (False, True):
            for repeat in range(3):
                case = MANIFEST['calibration']
                result = run_query(spark, case, aqe, calibration=True)
                result['repeat'] = repeat
                all_results.append(result)
                print('QUERYPROOF_CALIBRATION', spark.version, 'AQE_ON' if aqe else 'AQE_OFF',
                      result['verdict'], flush=True)
        failures = []
        for result in all_results:
            if result['verdict'] == 'ERROR':
                failures.append(result['id'] + ': ' + result.get('error',''))
            if not result['calibration']:
                expected_unknown = not next(x for x in MANIFEST['cases'] if x['id'] == result['id'])['keys']
                if expected_unknown != (result['verdict'] == 'UNKNOWN'):
                    failures.append(result['id'] + ': unexpected scope/verdict ' + result['verdict'])
        summary = {
            'spark_version': spark.version, 'frozen_manifest_date': MANIFEST['frozen_date'],
            'holdout_sql_families': len(MANIFEST['cases']),
            'known_bug_calibration_count': 1,
            'observed_violations_holdout': sum(x['verdict'] == 'CONTRACT_VIOLATED' and not x['calibration'] for x in all_results),
            'observed_violations_known_bug': sum(x['verdict'] == 'CONTRACT_VIOLATED' and x['calibration'] for x in all_results),
            'out_of_scope_count': sum(x['verdict'] == 'UNKNOWN' for x in all_results),
            'harness_failures': failures,
            'interpretation': 'Only observable sortedness was checked. NO_VIOLATION_OBSERVED is not a proof of SQL correctness.',
        }
        report = {'summary': summary, 'observations': all_results}
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts') / ('queryproof_observed_contract_'+spark.version+'.json')).write_text(
            json.dumps(report, indent=2), encoding='utf8')
        print('QUERYPROOF_CONTRACT_SUMMARY', json.dumps(summary), flush=True)
        return 1 if failures else 0
    finally:
        spark.stop()


if __name__ == '__main__':
    sys.exit(main())
