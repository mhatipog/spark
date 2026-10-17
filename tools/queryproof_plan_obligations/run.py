"""Development benchmark for *automatically extracted* plan contracts.

The extractor reads only the Spark analyzed plan; manifest labels are kept
outside it for independent scoring. No SQL-string pattern matching.
"""
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'queryproof_observed_contract'))
from contract import audit
from extract import extract
from pyspark.sql import SparkSession


PREVIOUS = json.loads((HERE.parent / "queryproof_observed_contract" / "manifest.json").read_text())
CHALLENGE = [
    {"id": "project_alias_passthrough",
     "sql": "SELECT k FROM (SELECT id AS k FROM range(0,12,1,4) ORDER BY id ASC) AS s",
     "expect_exposed": True},  # order might be present through child projection
    {"id": "order_by_string",
     "sql": "SELECT s FROM VALUES ('z'),('a'),('m') AS t(s) ORDER BY s",
     "expect_exposed": False},
    {"id": "computed_hidden_key",
     "sql": "SELECT id FROM range(0,12,1,4) ORDER BY id % 5, id",
     "expect_exposed": False},
    {"id": "filter_above_internal_sort",
     "sql": "SELECT id FROM (SELECT id FROM range(0,12,1,4) ORDER BY id) s WHERE id > 4",
     "expect_exposed": False},
    {"id": "non_global_sort",
     "sql": "SELECT id FROM range(0,12,1,4) SORT BY id",
     "expect_exposed": False},
    {"id": "raw_interval_order",
     "sql": "SELECT id FROM range(0,15,1,4) ORDER BY id DESC LIMIT 4 OFFSET 2",
     "expect_exposed": True},
    {"id": "string_projection_from_integer_key",
     "sql": "SELECT CAST(id AS STRING) AS s FROM range(0,12,1,4) ORDER BY s",
     "expect_exposed": False},
]
# Note: supplemental cases above are *development controls*, not independent
# prospective holdouts. Do not count them in an out-of-sample discovery metric.


def inspect_one(spark, sql, name, aqe, declared_keys=None, is_calibration=False):
    session = spark.newSession()
    session.conf.set('spark.sql.adaptive.enabled', str(aqe).lower())
    session.conf.set('spark.sql.shuffle.partitions', '5')
    if is_calibration:
        session.conf.set('spark.sql.optimizer.excludedRules',
                         'org.apache.spark.sql.catalyst.optimizer.InlineCTE')
    began = time.perf_counter()
    try:
        df = session.sql(sql)
        extracted = extract(df)
        extraction_ms = (time.perf_counter() - began) * 1000
        rows = [list(row) for row in df.collect()]
        witness = audit(iter(rows), extracted["keys"]) if extracted["status"] == "EXTRACTED" else {
            "verdict": "UNKNOWN", "reason": extracted["reason"]}
        match = (extracted["keys"] == declared_keys) if declared_keys is not None and extracted["status"] == "EXTRACTED" else None
        result = {
            "id": name, "aqe": aqe, "spark": spark.version,
            "extracted": extracted, "witness": witness, "manual_label_agrees": match,
            "manual_keys_present": bool(declared_keys),
            "extraction_latency_ms_including_analysis": round(extraction_ms, 3),
            "observed_rows": len(rows), "calibration": is_calibration,
        }
        print('QUERYPROOF_PLAN', spark.version, name, int(aqe), extracted["status"],
              extracted.get("reason", "KEYS="+str(extracted["keys"])),
              witness['verdict'], 'matches', match, flush=True)
        return result
    except Exception as exc:
        print('QUERYPROOF_PLAN_ERROR', spark.version, name, repr(exc), flush=True)
        return {"id": name, "aqe": aqe, "spark": spark.version,
                "error": repr(exc), "calibration": is_calibration}


def main():
    spark = (SparkSession.builder.master('local[2]')
             .appName('QueryProof automatic analyzed-plan obligation extractor')
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    all_results = []
    try:
        for case in PREVIOUS['cases']:
            for aqe in (False, True):
                all_results.append(inspect_one(spark, case['sql'], case['id'], aqe,
                                               declared_keys=case['keys']))
        for case in CHALLENGE:
            for aqe in (False, True):
                all_results.append(inspect_one(spark, case['sql'], case['id'], aqe))
        for aqe in (False, True):
            for repeat in range(3):
                all_results.append(inspect_one(spark, PREVIOUS['calibration']['sql'],
                    'known_bug_calibration_'+str(repeat), aqe,
                    declared_keys=PREVIOUS['calibration']['keys'], is_calibration=True))
        errors = [r for r in all_results if "error" in r]
        mismatches = [r['id'] for r in all_results if r.get('manual_label_agrees') is False]
        previous = all_results[:2*len(PREVIOUS['cases'])]
        eligible = [r for r in previous if r['manual_keys_present'] and "error" not in r]
        supported = [r for r in eligible if r['extracted']['status'] == 'EXTRACTED']
        out_scope = [r for r in previous if not r['manual_keys_present'] and "error" not in r]
        false_scopes = [r['id'] for r in out_scope if r['extracted']['status'] != 'UNKNOWN']
        calibration = [r for r in all_results if r.get('calibration') and 'error' not in r]
        summary = {
            'spark_version': spark.version,
            'frozen_controls_extracted': len(supported),
            'frozen_controls_eligible': len(eligible),
            'frozen_controls_unknown': len(eligible)-len(supported),
            'wrong_extracted_key_sequences': mismatches,
            'incorrectly_extracted_out_of_scope_keys': false_scopes,
            'calibration_violations': sum(r['witness']['verdict'] == 'CONTRACT_VIOLATED' for r in calibration),
            'calibration_evaluations': len(calibration),
            'errors': errors,
            'scope': 'Necessary observable ordering, no correctness certification; existing labeled benchmark reused for development.'
        }
        Path('artifacts').mkdir(exist_ok=True)
        (Path('artifacts') / ('queryproof_plan_contracts_'+spark.version+'.json')).write_text(
            json.dumps({'summary': summary, 'cases': all_results}, indent=2),encoding='utf8')
        print('QUERYPROOF_PLAN_SUMMARY',json.dumps(summary),flush=True)
        return 1 if errors or mismatches or false_scopes else 0
    finally:
        spark.stop()


if __name__ == '__main__':
    sys.exit(main())
