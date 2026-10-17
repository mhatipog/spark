"""Read-only empirical sensitivity check on the 2026-10-16 frozen Spark bug suite.

The 5 historical bugs + 4 controls predate the plan-derived checker.
Expected answers are not supplied to the checker. This exercise measures a
narrow *necessary output-order* property, not SQL correctness certification.
"""
import json
import sys
import tempfile
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TOOLS = ROOT.parent
sys.path.insert(0, str(TOOLS / 'queryproof_observed_contract'))
sys.path.insert(0, str(TOOLS / 'queryproof_prospective'))

from contract import audit
from extract import extract
from run import fixture, configured_session
from pyspark.sql import SparkSession


PROTOCOL = json.loads((TOOLS / 'queryproof_prospective' / 'manifest.json').read_text())


def equivalent(actual, expected, comparison):
    if comparison == 'sequence':
        return actual == expected
    def canonical(rows):
        return Counter(json.dumps(row, default=str, sort_keys=True) for row in rows)
    return canonical(actual) == canonical(expected)


def run():
    spark = (SparkSession.builder.master('local[2]')
             .appName('QueryProof cross-family negative-science benchmark')
             .config('spark.ui.enabled', 'false')
             .config('spark.sql.adaptive.enabled', 'false')
             .config('spark.sql.shuffle.partitions', '5')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    records = []
    try:
        with tempfile.TemporaryDirectory(prefix="queryproof_cross_family_") as root:
            for case in PROTOCOL['cases']:
                cid = case['id']
                for repetition in range(2):
                    try:
                        s, settings = configured_session(spark, case)
                        fixture(s, case['fixture'], root + '/' + cid + '_' + str(repetition))
                        df = s.sql(case['sql'])
                        property_spec = extract(df)
                        original_rows = [list(row) for row in df.collect()]
                        number = case.get('observed_columns')
                        scored_rows = [row[:number] if number else row for row in original_rows]
                        witness = (audit(iter(original_rows), property_spec['keys'])
                                   if property_spec['status'] == 'EXTRACTED'
                                   else {'verdict': 'UNKNOWN',
                                         'reason': property_spec.get('reason', 'UNSUPPORTED')})
                        oracle_ok = equivalent(scored_rows, case['expected'], case['comparison'])
                        observation = {
                            'id': cid, 'issue': case['issue'], 'category': case['category'],
                            'kind': case['kind'], 'repetition': repetition,
                            'oracle_correct': oracle_ok,
                            'detector_verdict': witness['verdict'],
                            'automatic_contract': property_spec,
                            'witness': witness, 'rows': scored_rows,
                        }
                    except Exception as error:
                        observation = {'id': cid, 'kind': case['kind'],
                                       'repetition': repetition, 'error': repr(error)}
                    records.append(observation)
                    print('QUERYPROOF_CROSS_FAMILY', spark.version, cid, repetition,
                          'ERROR' if 'error' in observation else
                          ('WRONG' if not observation['oracle_correct'] else 'CORRECT'),
                          observation.get('detector_verdict'),flush=True)
        failed = [x for x in records if 'error' in x]
        wrong = [x for x in records if x.get('oracle_correct') is False]
        observed = [x for x in records if 'error' not in x]
        false_accusations = [x for x in observed
                             if x['oracle_correct'] and x['detector_verdict'] == 'CONTRACT_VIOLATED']
        caught = [x for x in wrong if x['detector_verdict'] == 'CONTRACT_VIOLATED']
        bugs_reproduced = sorted({x['id'] for x in wrong if x['kind']=='bug'})
        summary = {
            'spark_version': spark.version, 'bug_families': 5, 'negative_control_families': 4,
            'observations': len(observed), 'runtime_errors': failed,
            'reproduced_distinct_bug_families': bugs_reproduced,
            'wrong_result_executions': len(wrong),
            'witnessed_wrong_result_executions': len(caught),
            'false_accusations_on_correct_results': len(false_accusations),
            'detector_verdict_counts': dict(Counter(x['detector_verdict'] for x in observed)),
            'meaning': 'The checker evaluates exposed final-result order only. Failed recall '
                       'on wrong bags/aggregates is expected, NOT evidence of SQL correctness.',
            'historical_protocol_date': PROTOCOL['selection_date'],
        }
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts') / ('queryproof_cross_family_'+spark.version+'.json')).write_text(
            json.dumps({'summary':summary,'records':records}, indent=2),encoding='utf-8')
        print('QUERYPROOF_CROSS_FAMILY_SUMMARY',json.dumps(summary),flush=True)
        return 1 if failed or false_accusations else 0
    finally:
        spark.stop()


if __name__=='__main__':
    raise SystemExit(run())
