"""Frozen 2026-10-18 Catalyst holdout: process-isolated Spark and sanitized proof scoring.

Run from the repository root:
    python3 tools/queryproof_holdout_20261018/run_holdout.py

The instrumented PySpark/Scala RuleExecutor overlay must already be installed.
Raw JVM stderr (which includes base64-encoded whole Catalyst ASTs) is captured
in a private temporary directory and is never published as an artifact.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
MANIFEST = HERE / "CASES.json"
PROTOCOL = HERE / "FROZEN_PROTOCOL.json"
OUTPUT = ROOT / "artifacts" / "queryproof_holdout_20261018.json"


def canonical_bag(rows):
    """Stable row multiset hash; no SQL result tuples are published."""
    count = Counter(tuple(x) for x in rows)
    records = [[list(row), multiplicity] for row, multiplicity in count.items()]
    records.sort(key=lambda x: json.dumps(x[0], separators=(",", ":")))
    encoded = json.dumps(records, separators=(",", ":"), ensure_ascii=True)
    return {"count": sum(count.values()), "distinct_rows": len(count),
            "sha256": hashlib.sha256(encoded.encode("ascii")).hexdigest()}


def _worker(manifest_path, output_path):
    """Executed in a child process whose entire stderr is held privately."""
    from pyspark.sql import SparkSession
    from pyspark.sql.types import IntegerType, StructField, StructType

    manifest = json.loads(Path(manifest_path).read_text())
    schema = StructType([StructField(k, IntegerType(), True)
                         for k in manifest["columns"]])
    spark = (SparkSession.builder.master("local[2]")
             .appName("QueryProof frozen unseen optimizer holdout")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    reference = sqlite3.connect(":memory:")
    try:
        for table, rows in manifest["source_rows"].items():
            name = table
            assert name in ("qp_l", "qp_r")
            ddl = ", ".join(k + " INTEGER" for k in manifest["columns"])
            reference.execute("CREATE TABLE " + name + " (" + ddl + ")")
            reference.executemany("INSERT INTO " + name + " VALUES (?,?,?)", rows)
            spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
        jvm = spark._jvm.java.lang.System
        observations = {}
        for case in manifest["cases"]:
            key = case["id"]
            jvm.setProperty("queryproof.query.id", key)
            jvm.setProperty("queryproof.ast.enabled", "true")
            try:
                expected = reference.execute(case["sql"]).fetchall()
                df = spark.sql(case["sql"])
                # Trigger optimizer explicitly under the case label.
                df._jdf.queryExecution().optimizedPlan()
                actual = [tuple(row) for row in df.collect()]
                expected_bag, actual_bag = canonical_bag(expected), canonical_bag(actual)
                observations[key] = {
                    "status": "EXECUTED",
                    "oracle_match": expected_bag == actual_bag,
                    "spark": actual_bag, "sqlite_reference": expected_bag}
            except Exception as exc:
                # Only an exception class is reported; messages may contain SQL text.
                observations[key] = {"status": "ERROR",
                                     "error_type": type(exc).__name__}
            finally:
                jvm.clearProperty("queryproof.ast.enabled")
                jvm.clearProperty("queryproof.query.id")
            print("QUERYPROOF_HOLDOUT_CASE", key, observations[key]["status"],
                  flush=True)
        payload = {"spark_version": spark.version, "cases": observations}
        Path(output_path).write_text(json.dumps(payload, indent=2))
        return 1 if any(v["status"] != "EXECUTED" for v in observations.values()) else 0
    finally:
        reference.close()
        spark.stop()


def _rule_basename(name):
    return str(name or "").split(".")[-1].split("$")[0]


def analyze_trace(path, known_rule_names):
    sys.path.insert(0, str(HERE.parent / "queryproof_context_validator"))
    sys.path.insert(0, str(HERE.parent / "queryproof_instrumented"))
    from rank_ast import decode
    from run_orbit_trace import classify_events
    from equality_orbits import certify_predicate_event

    events, failures, instrumentation_failures = [], [], []
    for lineno, line in enumerate(Path(path).read_text(errors="replace").splitlines(), 1):
        if line.startswith("QUERYPROOF_AST_ERROR\t"):
            instrumentation_failures.append({"line": lineno})
            continue
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        try:
            events.append(decode(line))
        except Exception as exc:
            failures.append({"line": lineno, "type": type(exc).__name__})
    scored = classify_events(events)
    # Exploratory diagnostics contain only logical-plan operator classes/arity.
    # Never persist SQL literals, attributes, exprIds, or the raw AST.
    def public_plan_shape(nodes):
        if not isinstance(nodes, list):
            return "<invalid>"
        prefix = "org.apache.spark.sql.catalyst.plans.logical."
        return "/".join(
            (node.get("class", "").removeprefix(prefix)
             if isinstance(node, dict)
             and str(node.get("class", "")).startswith(prefix) else "OTHER")
            + ":" + str(node.get("num-children", "?"))
            for node in nodes if isinstance(node, dict)
        )

    unknown_shapes = Counter()
    for event in events:
        case = event.get("case", "unlabelled")
        report = certify_predicate_event(event)
        if report["status"] == "UNKNOWN":
            key = (case, _rule_basename(event.get("rule")),
                   report.get("reason", "UNKNOWN"),
                   public_plan_shape(event.get("old")),
                   public_plan_shape(event.get("new")))
            unknown_shapes[key] += 1
    # Exactly the same checker is run for all events.  Prior rule names only
    # determine which already-investigated family is excluded when reporting.
    independent = [e for e in events
                   if _rule_basename(e.get("rule")) not in known_rule_names]
    holdout = classify_events(independent)
    by_rule = defaultdict(Counter)
    by_operator = defaultdict(Counter)
    for case, entries in holdout["cases"].items():
        for entry in entries:
            by_rule[entry.get("rule", "<unknown>")][entry["status"]] += 1
            by_operator[entry.get("operator", "UNKNOWN")][entry["status"]] += 1
    return {
        "trace_events": len(events),
        "decode_failures": failures,
        "instrumentation_failures": instrumentation_failures,
        "possible_trace_truncation": len(events) >= 2500,
        "all_events": scored["summary"],
        "structural_diagnostics": [
            {"case": k[0], "rule": k[1], "reason": k[2],
             "old_shape": k[3], "new_shape": k[4], "count": v}
            for k, v in sorted(unknown_shapes.items())],
        "unseen_rule_events": holdout["summary"],
        "per_unseen_rule": {k: dict(v) for k, v in sorted(by_rule.items())},
        "per_operator": {k: dict(v) for k, v in sorted(by_operator.items())},
        "unseen_rule_case_reports": holdout["cases"]}


def parent(manifest_path, protocol_path, output_path):
    manifest_bytes = manifest_path.read_bytes()
    protocol = json.loads(protocol_path.read_text())
    manifest = json.loads(manifest_bytes)
    assert manifest["benchmark_id"] == protocol["benchmark_id"]
    assert len(manifest["cases"]) == 18
    with tempfile.TemporaryDirectory(prefix="qp-holdout-private-") as tmp:
        raw_trace = Path(tmp) / "catalyst_raw_stderr.log"
        observations_path = Path(tmp) / "worker_observations.json"
        # Shell/CI stdout contains only status and synthetic case IDs.
        with raw_trace.open("w", encoding="utf8") as private_stream:
            child = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()),
                 "--worker", str(manifest_path), str(observations_path)],
                cwd=str(ROOT), stderr=private_stream, check=False)
        observations = (json.loads(observations_path.read_text())
                        if observations_path.exists() else
                        {"spark_version": None, "cases": {}})
        scored = analyze_trace(raw_trace,
            set(protocol["rules_known_from_prior_round_excluded_from_primary"]))
    # Temporary directory and raw Catalyst payload gone before writing summary.
    cases = observations.get("cases", {})
    missing = sorted(set(x["id"] for x in manifest["cases"]) - set(cases))
    mismatches = [k for k, v in cases.items()
                  if v["status"] == "EXECUTED" and v["oracle_match"] is False]
    report = {
        "protocol": manifest["benchmark_id"],
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "protocol_sha256": hashlib.sha256(protocol_path.read_bytes()).hexdigest(),
        "spark_version": observations.get("spark_version"),
        "worker_exit_code": child.returncode,
        "executed_cases": len(cases),
        "missing_cases": missing,
        "row_oracle_mismatch_cases": sorted(mismatches),
        "cases": cases,
        "trace": scored,
        "cautions": [
            "UNKNOWN means a transition is outside this narrow predicate-only proof "
            "fragment; it does not establish optimizer failure.",
            "COUNTEREXAMPLE is a local expression valuation; it does not "
            "automatically establish a reachable database bug.",
            "SQLite row oracle covers only the fixed finite fixture.",
            "Raw Catalyst JSON stays private and was discarded after scoring."
        ]}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf8")
    print("QUERYPROOF_HOLDOUT_SUMMARY",
          json.dumps({"worker_exit_code": child.returncode,
                      "cases": len(cases), "mismatches": len(mismatches),
                      "total_trace_events": scored["trace_events"],
                      "unseen_statuses": scored["unseen_rule_events"]["statuses"],
                      "decode_failures": len(scored["decode_failures"]),
                      "instrumentation_failures":
                          len(scored["instrumentation_failures"]),
                      "possible_trace_truncation":
                          scored["possible_trace_truncation"]}),
          flush=True)
    complete = (child.returncode == 0 and not missing and
                scored["trace_events"] > 0 and not scored["decode_failures"]
                and not scored["instrumentation_failures"]
                and not scored["possible_trace_truncation"])
    return 0 if complete else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("worker_args", nargs="*")
    args = parser.parse_args()
    if args.worker:
        if len(args.worker_args) != 2:
            parser.error("--worker needs manifest and observation paths")
        raise SystemExit(_worker(Path(args.worker_args[0]),
                                 Path(args.worker_args[1])))
    if args.worker_args:
        parser.error("unexpected positional args")
    raise SystemExit(parent(MANIFEST, PROTOCOL, OUTPUT))
