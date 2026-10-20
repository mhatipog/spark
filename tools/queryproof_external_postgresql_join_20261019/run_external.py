"""Preselected third-party-derived Spark SQL regression replay.

Pure orchestration and reporting. The frozen QueryProof certificate implementations
are imported unchanged; this file MUST NOT define or alter any proof decision.
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
sys.path.insert(0, str(ROOT / "tools" / "queryproof_holdout_20261018"))
from run_holdout import canonical_bag, analyze_trace, _rule_basename

MANIFEST = HERE / "CASES.json"
PROTOCOL = HERE / "FROZEN_PROTOCOL.json"
OUTPUT = ROOT / "artifacts" / "queryproof_external_postgresql_join_20261019.json"
DIAGNOSTIC_GENERALIZED = False


def worker(manifest_path, output_path):
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (ByteType, IntegerType, LongType,
        ShortType, StringType, StructField, StructType)

    manifest = json.loads(manifest_path.read_text())
    spark = (SparkSession.builder.master("local[2]")
             .appName("QueryProof externally sourced PostgreSQL join regression")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    reference = sqlite3.connect(":memory:")
    try:
        def install_tables(definitions, prior_names):
            for old_name in prior_names:
                reference.execute('DROP TABLE IF EXISTS "' + old_name + '"')
                spark.catalog.dropTempView(old_name)
            for name, definition in definitions.items():
                assert name.isidentifier() and name[0].isalpha()
                columns = definition["columns"]
                data_types = {
                    "integer": IntegerType, "string": StringType,
                    "tinyint": ByteType, "smallint": ShortType,
                    "long": LongType}
                assert all(column["type"] in data_types
                           for column in columns)
                spark_schema = StructType([
                    StructField(column["name"],
                                data_types[column["type"]](), True)
                    for column in columns])
                sqlite_types = {
                    "integer": "INTEGER", "string": "TEXT",
                    "tinyint": "INTEGER", "smallint": "INTEGER",
                    "long": "INTEGER"}
                ddl = ", ".join('"' + c["name"] + '" ' + sqlite_types[c["type"]]
                                for c in columns)
                reference.execute('CREATE TABLE "' + name + '" (' + ddl + ')')
                placeholders = ",".join("?" for _ in columns)
                # Preserve exact signed BIGINT values across JSON's limited
                # JavaScript number precision: the frozen manifest encodes
                # those two source integers as decimal strings.
                rows = []
                for row in definition["rows"]:
                    assert len(row) == len(columns)
                    converted = []
                    for value, column in zip(row, columns):
                        if column["type"] == "long" and isinstance(value, str):
                            value = int(value)
                        converted.append(value)
                    rows.append(tuple(converted))
                reference.executemany(
                    'INSERT INTO "' + name + '" VALUES (' + placeholders + ')',
                    rows)
                (spark.createDataFrame(rows, spark_schema)
                 .createOrReplaceTempView(name))
            return set(definitions)

        active_names, active_definition = set(), None
        observations = {}
        jvm = spark._jvm.java.lang.System
        for case in manifest["cases"]:
            desired = case.get("tables", manifest["tables"])
            if desired != active_definition:
                active_names = install_tables(desired, active_names)
                active_definition = desired
            key = case["id"]
            jvm.setProperty("queryproof.query.id", key)
            jvm.setProperty("queryproof.ast.enabled", "true")
            try:
                expected = reference.execute(case["sql"]).fetchall()
                df = spark.sql(case["sql"])
                df._jdf.queryExecution().optimizedPlan()
                actual = [tuple(row) for row in df.collect()]
                expected_bag = canonical_bag(expected)
                actual_bag = canonical_bag(actual)
                observations[key] = {
                    "status": "EXECUTED",
                    "oracle_match": expected_bag == actual_bag,
                    "spark": actual_bag, "sqlite_reference": expected_bag}
            except Exception as exc:
                # Deliberately omit error text and full logical plans.
                observations[key] = {"status": "ERROR", "error_type": type(exc).__name__}
            finally:
                jvm.clearProperty("queryproof.ast.enabled")
                jvm.clearProperty("queryproof.query.id")
            print("QUERYPROOF_EXTERNAL_CASE", key, observations[key]["status"], flush=True)
        output_path.write_text(json.dumps({
            "spark_version": spark.version,
            "sqlite_version": sqlite3.sqlite_version,
            "cases": observations}, indent=2))
        return int(any(x["status"] != "EXECUTED" or not x["oracle_match"]
                       for x in observations.values()))
    finally:
        reference.close()
        spark.stop()


def union_coverage(private_trace_path):
    """Combine five *frozen* checkers per event without counting overlaps twice."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode
    from equality_orbits import certify_predicate_event
    from join_prefilter_certificate import certify_inner_join_prefilter_event
    from project_pushdown_certificate import certify_project_filter_event
    from compositional_project_frame_certificate import certify_pure_project_framed_event
    from join_filter_transport_certificate import certify_inner_join_filter_transport

    checkers = {
        "predicate_orbit": certify_predicate_event,
        "inner_join_prefilter": certify_inner_join_prefilter_event,
        "project_filter": certify_project_filter_event,
        "pure_project_frame": certify_pure_project_framed_event,
        "inner_join_transport": certify_inner_join_filter_transport,
    }
    counts = Counter()
    by_family = Counter()
    by_rule = defaultdict(Counter)
    by_case = defaultdict(Counter)
    overlapping = Counter()
    unconstrained = Counter()
    counterexample_events = []
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        accepted = []
        for family, checker in checkers.items():
            verdict = checker(event)
            status = verdict["status"]
            if status == "EQUIVALENT_IN_FRAGMENT":
                accepted.append(family)
                by_family[family] += 1
            elif status == "COUNTEREXAMPLE":
                counterexample_events.append({
                    "case": event.get("case"),
                    "rule": _rule_basename(event.get("rule")),
                    "family": family})
        case = event.get("case", "unlabelled")
        rule = _rule_basename(event.get("rule"))
        label = "CERTIFIED" if accepted else "UNKNOWN"
        counts[label] += 1
        by_rule[rule][label] += 1
        by_case[case][label] += 1
        if len(accepted) > 1:
            overlapping["+".join(accepted)] += 1
        if not accepted:
            unconstrained[rule] += 1
    return {
        "event_denominator": sum(counts.values()),
        "union_statuses": dict(counts),
        "per_certificate_family": dict(by_family),
        "overlap_families": dict(overlapping),
        "by_rule": {key: dict(val) for key, val in sorted(by_rule.items())},
        "by_case": {key: dict(val) for key, val in sorted(by_case.items())},
        "unknown_rule_counts": dict(unconstrained),
        "counterexample_events": counterexample_events,
        "coverage_explanation": (
            "An event counts once in union CERTIFIED iff at least one unchanged "
            "checker returned EQUIVALENT_IN_FRAGMENT. Others are UNKNOWN, not bugs. "
            "COUNTEREXAMPLE is only a fragment-local proof result, not necessarily "
            "a reachable optimizer defect.")
    }


def plan_key_diagnostics(private_trace_path):
    """Metadata keys/op classes only; never serialize full expressions or values."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode
    from collections import Counter
    signatures = Counter()
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        rule = _rule_basename(event.get("rule"))
        if rule not in ("InferFiltersFromConstraints", "CollapseProject",
                        "PushDownPredicates"):
            continue
        for tag in ("old", "new"):
            for node in event.get(tag, []):
                cls = str(node.get("class", "")).split(".")[-1]
                attrs = tuple(sorted(node.keys()))
                if cls == "Project":
                    shapes = []
                    for item in node.get("projectList", []):
                        if isinstance(item, list):
                            shapes.append(tuple(
                                str(x.get("class", "")).split(".")[-1]
                                for x in item if isinstance(x, dict)))
                    signature = ("Project", tuple(sorted(Counter(shapes).items())))
                elif node.get("num-children") == 0:
                    output = node.get("output", None)
                    shape = (type(output).__name__,
                             len(output) if isinstance(output, list) else None,
                             tuple(type(x).__name__ for x in output[:3])
                             if isinstance(output, list) else ())
                    signature = ("Leaf", shape)
                else:
                    continue
                signatures[(rule, tag, cls, attrs, signature)] += 1
    return [
        {"rule": k[0], "side": k[1], "class": k[2],
         "keys": list(k[3]), "signature": str(k[4]), "count": v}
        for k, v in sorted(signatures.items(), key=lambda row: (-row[1], row[0]))
    ]


def retrospective_contextual_guard(private_trace_path):
    """Deliberately *not* part of the source-frozen five-checker union metric."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode
    from contextual_join_guard_certificate import certify_contextual_join_guard_event
    passed = []
    by_rule = Counter()
    reasons = Counter()
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        verdict = certify_contextual_join_guard_event(event)
        rule = _rule_basename(event.get("rule"))
        if verdict["status"] == "EQUIVALENT_IN_FRAGMENT":
            passed.append({
                "case": event.get("case"), "rule": rule,
                "frames": verdict["frames"], "constants": verdict["constants"],
                "guarded_sides": verdict["guarded_sides"],
                "orbits_checked": verdict["orbits_checked"],
                "sqlite_verified": verdict["sqlite_verified"]})
            by_rule[rule] += 1
        else:
            reasons[verdict.get("reason", "UNKNOWN")] += 1
    return {
        "status": "RETROSPECTIVE_EXPLORATORY_ONLY_AFTER_ZERO_COVERAGE_HOLDOUT",
        "certified": len(passed), "certificates": passed,
        "by_rule": dict(by_rule), "unknown_reasons": dict(reasons)}



def safe_collapse_plan_shape(serialized):
    """Only operator names and expression *class* shapes; never SQL/data values."""
    from join_prefilter_certificate import tree_from_preorder

    def compress(tree):
        node, children = tree
        name = str(node.get("class", "")).split(".")[-1]
        result = {"node": name}
        if name == "Project":
            expressions = []
            for item in node.get("projectList", []):
                expressions.append([
                    str(expression.get("class", "")).split(".")[-1]
                    for expression in item if isinstance(expression, dict)])
            result["project_classes"] = expressions
        if name == "Aggregate":
            result["aggregate_field_keys"] = sorted(node)
            for field in ("groupingExpressions", "aggregateExpressions"):
                values = node.get(field)
                if isinstance(values, list):
                    result[field + "_classes"] = [
                        [str(part.get("class", "")).split(".")[-1]
                         for part in item if isinstance(part, dict)]
                        for item in values if isinstance(item, list)]
        result["children"] = [compress(child) for child in children]
        return result

    return compress(tree_from_preorder(serialized))




def retrospective_column_pruning_liveness(private_trace_path):
    """Pure free-variable theorem on already inspected source; no holdout credit."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode
    from column_pruning_liveness_certificate import (
        certify_column_pruning_liveness_event,
    )
    successes, unknowns, named = [], Counter(), 0
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        if _rule_basename(event.get("rule")) != "ColumnPruning":
            continue
        named += 1
        result = certify_column_pruning_liveness_event(event)
        if result["status"] == "EQUIVALENT_IN_FRAGMENT":
            successes.append({
                "case": event.get("case"),
                "kept_columns": result["kept_columns"],
                "dropped_columns": result["dropped_columns"],
                "empty_projection_supported": result["empty_projection_supported"],
                "sort_frames": result["sort_frames"],
            })
        else:
            unknowns[result.get("reason", "UNKNOWN")] += 1
    return {"status": "RETROSPECTIVE_DEVELOPMENT_ONLY_NO_NEW_HOLDOUT_CREDIT",
            "named_column_pruning_events": named,
            "certified": len(successes),
            "certificates": successes,
            "unknown_reasons": dict(unknowns)}


def score_generalized_column_liveness(private_trace_path):
    """Score every actual named rule event; never use rule identity as proof."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode
    from generalized_projection_liveness_certificate import (
        certify_generalized_projection_liveness_event,
    )
    certificates, unknowns, events = [], Counter(), 0
    safe_unknown_alias_shapes = []
    safe_unknown_expression_shapes = []
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        if _rule_basename(event.get("rule")) != "ColumnPruning":
            continue
        events += 1
        result = certify_generalized_projection_liveness_event(event)
        if result["status"] == "EQUIVALENT_IN_FRAGMENT":
            certificates.append({
                "case": event.get("case"),
                "live_attributes": result["live_attributes"],
                "old_project_depth": result["old_project_depth"],
                "new_project_depth": result["new_project_depth"],
                "old_output_width": result["old_output_width"],
                "new_output_width": result["new_output_width"],
                "sort_frames": result["sort_frames"],
            })
        else:
            unknowns[result.get("reason", "UNKNOWN")] += 1
            if (result.get("reason") == "BAD_PROJECT_ALIAS" and
                    len(safe_unknown_alias_shapes) < 10):
                shapes = {"case": event.get("case")}
                for side in ("old", "new"):
                    found = []
                    for plan in event[side]:
                        if plan.get("class") != (
                                "org.apache.spark.sql.catalyst.plans.logical."
                                "Project"):
                            continue
                        for entry in plan.get("projectList", []):
                            if (isinstance(entry, list) and entry and
                                    isinstance(entry[0], dict) and
                                    entry[0].get("class") == (
                                        "org.apache.spark.sql.catalyst."
                                        "expressions.Alias")):
                                found.append({
                                    "keys": sorted(entry[0]),
                                    "expr_length": len(entry),
                                    "child_arity": entry[0].get("num-children"),
                                    "name_is_string": isinstance(
                                        entry[0].get("name"), str),
                                })
                    shapes[side] = found
                safe_unknown_alias_shapes.append(shapes)
            if (result.get("reason") ==
                    "UNSAFE_OR_UNKNOWN_PROJECT_EXPRESSION" and
                    len(safe_unknown_expression_shapes) < 10):
                shape = {"case": event.get("case")}
                for side in ("old", "new"):
                    expressions = []
                    for plan in event[side]:
                        if plan.get("class") != (
                                "org.apache.spark.sql.catalyst.plans.logical."
                                "Project"):
                            continue
                        for entry in plan.get("projectList", []):
                            if not isinstance(entry, list):
                                continue
                            expressions.append([{
                                "operator": node.get("class", "").rsplit(".", 1)[-1],
                                "keys": sorted(node),
                                "arity": node.get("num-children"),
                            } for node in entry if isinstance(node, dict)])
                    shape[side] = expressions[:12]
                safe_unknown_expression_shapes.append(shape)
    return {
        "status": "RETROSPECTIVE_UNPINNED",
        "named_column_pruning_events": events,
        "certified": len(certificates),
        "certificates": certificates,
        "unknown_reasons": dict(unknowns),
        "safe_unknown_alias_shapes": safe_unknown_alias_shapes,
        "safe_unknown_expression_shapes": safe_unknown_expression_shapes,
        "certificate_scope": "pure_nested_project_observational_liveness",
    }


def diagnose_column_pruning(private_trace_path):
    """Structural triage only: no certificates or private values exported."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode

    cases, named_count = [], 0
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        if _rule_basename(event.get("rule")) != "ColumnPruning":
            continue
        named_count += 1
        if len(cases) < 20:
            cases.append({
                "case": event.get("case"),
                "old_shape": safe_collapse_plan_shape(event["old"]),
                "new_shape": safe_collapse_plan_shape(event["new"]),
            })
    return {"status": "RETROSPECTIVE_STRUCTURE_ONLY_NO_PROOF",
            "named_events": named_count,
            "redacted_event_shapes": cases,
            "truncated": named_count > len(cases)}


def retrospective_project_collapse(private_trace_path):
    """New theorem on previously seen SQL: development evidence ONLY."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode
    from collapse_project_certificate import certify_collapse_project_event

    accepted, reasons, observed, abstained_shapes = [], Counter(), Counter(), []
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        rule = _rule_basename(event.get("rule"))
        if rule != "CollapseProject":
            continue
        observed[rule] += 1
        verdict = certify_collapse_project_event(event)
        if verdict["status"] == "EQUIVALENT_IN_FRAGMENT":
            accepted.append({
                "case": event.get("case"),
                "rule": rule,
                "columns": verdict["columns"],
                "eliminated_project_columns": verdict["eliminated_project_columns"],
                "sort_frames": verdict["sort_frames"],
                "literal_outputs": verdict["literal_outputs"]})
        else:
            reasons[verdict.get("reason", "UNKNOWN")] += 1
            abstained_shapes.append({
                "case": event.get("case"),
                "old_shape": safe_collapse_plan_shape(event["old"]),
                "new_shape": safe_collapse_plan_shape(event["new"]),
                "reason": verdict.get("reason", "UNKNOWN")})
    return {
        "status": "RETROSPECTIVE_DEVELOPMENT_ONLY_NO_NEW_HOLDOUT_CREDIT",
        "named_collapse_events": sum(observed.values()),
        "certified": len(accepted),
        "certificates": accepted,
        "unknown_reasons": dict(reasons),
        "abstained_shapes": abstained_shapes}


def retrospective_aggregate_identity(private_trace_path):
    """Aggregate-framed identity theorem, post-hoc on ALL earlier cohorts."""
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_context_validator"))
    sys.path.insert(0, str(ROOT / "tools" / "queryproof_instrumented"))
    from rank_ast import decode
    from aggregate_framed_identity_certificate import (
        certify_aggregate_framed_identity_event,
    )
    results, reasons, total = [], Counter(), 0
    for line in private_trace_path.read_text(errors="replace").splitlines():
        if not line.startswith("QUERYPROOF_AST\t"):
            continue
        event = decode(line)
        rule = _rule_basename(event.get("rule"))
        if rule != "CollapseProject":
            continue
        total += 1
        proof = certify_aggregate_framed_identity_event(event)
        if proof["status"] == "EQUIVALENT_IN_FRAGMENT":
            results.append({"case": event.get("case"),
                            "aggregate_frames": proof["aggregate_frames"],
                            "sort_frames": proof.get("sort_frames", 0),
                            "forwarded_columns": proof["forwarded_columns"]})
        else:
            reasons[proof.get("reason", "UNKNOWN")] += 1
    return {"status": "RETROSPECTIVE_DEVELOPMENT_ONLY",
            "named_collapse_events": total,
            "certified": len(results),
            "certificates": results,
            "unknown_reasons": dict(reasons)}

def parent():
    manifest_bytes = MANIFEST.read_bytes()
    protocol_bytes = PROTOCOL.read_bytes()
    manifest = json.loads(manifest_bytes)
    protocol = json.loads(protocol_bytes)
    assert len(manifest["cases"]) == protocol.get("required_case_count", 18)
    assert manifest["benchmark_id"] == protocol["benchmark_id"]
    assert len(set((case.get("source_file", ""), case["source_query_ordinal"])
                   for case in manifest["cases"])) == len(manifest["cases"])
    with tempfile.TemporaryDirectory(prefix="qp-external-private-") as tmp:
        raw_stderr = Path(tmp) / "raw-catalyst-stderr.txt"
        child_report = Path(tmp) / "private-observations.json"
        with raw_stderr.open("w", encoding="utf8") as stream:
            child = subprocess.run([
                sys.executable, str(HERE / "run_external.py"),
                "--worker", str(MANIFEST), str(child_report)
            ], cwd=str(ROOT), stderr=stream, check=False)
        cases_observed = json.loads(child_report.read_text()) if child_report.exists() else {}
        scored = analyze_trace(raw_stderr, set())
        coverage = union_coverage(raw_stderr)
        key_signatures = plan_key_diagnostics(raw_stderr)
        contextual = retrospective_contextual_guard(raw_stderr)
        collapsed = retrospective_project_collapse(raw_stderr)
        aggregate_identity = retrospective_aggregate_identity(raw_stderr)
        pruning_diagnostic = diagnose_column_pruning(raw_stderr)
        column_liveness = retrospective_column_pruning_liveness(raw_stderr)
        generalized = (
            score_generalized_column_liveness(raw_stderr)
            if (protocol.get("prospective_generalized_liveness") or
                DIAGNOSTIC_GENERALIZED) else None)
        if generalized is not None and protocol.get(
                "prospective_generalized_liveness"):
            pin = json.loads((MANIFEST.parent / "VERIFIER_PIN.json").read_text())
            assert pin["benchmark_id"] == manifest["benchmark_id"]
            assert pin["frozen_verifier_commit"] == protocol["frozen_verifier_commit"]
            assert pin["verifier_module"] == (
                "tools/queryproof_context_validator/"
                "generalized_projection_liveness_certificate.py")
            current = subprocess.run(
                ["git", "hash-object", pin["verifier_module"]],
                cwd=ROOT, check=True, capture_output=True,
                text=True).stdout.strip()
            expected = pin["frozen_verifier_git_blob_sha"]
            assert len(expected) == len(current) == 40
            matches = current == expected
            generalized["status"] = (
                "RETROSPECTIVE_FIXTURE_REPAIR_DIAGNOSTIC"
                if protocol.get("fixture_repair_after_first_run") else
                "PROSPECTIVE_FROZEN_THIRD_PARTY_GENERALIZED_LIVENESS"
                if matches else "RETROSPECTIVE_VERIFIER_CHANGED_AFTER_FREEZE")
            generalized["verifier_provenance"] = {
                "frozen_verifier_git_blob_sha": expected,
                "current_verifier_git_blob_sha": current,
                "matches_frozen_checker": matches,
            }
        if protocol.get("prospective_aggregate_identity"):
            pin_path = MANIFEST.parent / "VERIFIER_PIN.json"
            if pin_path.exists():
                pin = json.loads(pin_path.read_text())
                assert pin["benchmark_id"] == manifest["benchmark_id"]
                assert pin["frozen_verifier_commit"] == protocol["frozen_verifier_commit"]
                module_path = pin["verifier_module"]
                assert module_path == (
                    "tools/queryproof_context_validator/"
                    "aggregate_framed_identity_certificate.py")
                current = subprocess.run(
                    ["git", "hash-object", module_path], cwd=ROOT,
                    check=True, capture_output=True, text=True).stdout.strip()
                baseline = pin["frozen_verifier_git_blob_sha"]
                assert len(current) == len(baseline) == 40
                unchanged = current == baseline
                aggregate_identity["status"] = (
                    "PROSPECTIVE_FROZEN_THIRD_PARTY_AGGREGATE_COHORT"
                    if unchanged else "RETROSPECTIVE_VERIFIER_CHANGED_AFTER_FREEZE")
                aggregate_identity["verifier_provenance"] = {
                    "frozen_verifier_git_blob_sha": baseline,
                    "current_verifier_git_blob_sha": current,
                    "matches_frozen_checker": unchanged,
                    "original_sql_protocol_unchanged": True,
                }
            else:
                aggregate_identity["status"] = (
                    "PROSPECTIVE_UNPINNED_VERIFIER_PROVENANCE")
        if protocol.get("prospective_column_liveness"):
            pin = json.loads(
                (MANIFEST.parent / "VERIFIER_PIN.json").read_text())
            assert pin["benchmark_id"] == manifest["benchmark_id"]
            assert pin["frozen_verifier_commit"] == protocol["frozen_verifier_commit"]
            module_path = pin["verifier_module"]
            assert module_path == (
                "tools/queryproof_context_validator/"
                "column_pruning_liveness_certificate.py")
            current = subprocess.run(
                ["git", "hash-object", module_path], cwd=ROOT,
                check=True, capture_output=True, text=True).stdout.strip()
            baseline = pin["frozen_verifier_git_blob_sha"]
            assert len(current) == len(baseline) == 40
            unchanged = current == baseline
            column_liveness["status"] = (
                "PROSPECTIVE_FROZEN_THIRD_PARTY_COLUMN_LIVENESS"
                if unchanged else
                "RETROSPECTIVE_VERIFIER_CHANGED_AFTER_FREEZE")
            column_liveness["verifier_provenance"] = {
                "frozen_verifier_git_blob_sha": baseline,
                "current_verifier_git_blob_sha": current,
                "matches_frozen_checker": unchanged,
            }
        if protocol.get("prospective_collapse_project"):
            collapsed["status"] = "PROSPECTIVE_FROZEN_THIRD_PARTY_COHORT"
        if protocol.get("prospective_contextual_guard"):
            contextual["status"] = "PROSPECTIVE_FROZEN_DUCKDB_COHORT"
    observations = cases_observed.get("cases", {})
    missing = sorted(set(c["id"] for c in manifest["cases"]) - set(observations))
    failed = sorted(k for k, v in observations.items() if v["status"] != "EXECUTED")
    mismatches = sorted(k for k, v in observations.items()
                        if v["status"] == "EXECUTED" and not v["oracle_match"])
    assert coverage["event_denominator"] == scored["trace_events"], "Scorer denominator differs"
    report = {
        "protocol": manifest["benchmark_id"],
        "source": manifest["source"],
        "verifier_frozen_commit": protocol["frozen_verifier_commit"],
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "protocol_sha256": hashlib.sha256(protocol_bytes).hexdigest(),
        "spark_version": cases_observed.get("spark_version"),
        "sqlite_version": cases_observed.get("sqlite_version"),
        "worker_exit_code": child.returncode,
        "executed_cases": len(observations),
        "missing_cases": missing,
        "error_cases": failed,
        "row_oracle_mismatch_cases": mismatches,
        "cases": observations,
        "union_coverage": coverage,
        "sanitized_plan_key_inventory": key_signatures,
        "posthoc_contextual_guard": contextual,
        "posthoc_collapse_project": collapsed,
        "posthoc_aggregate_identity": aggregate_identity,
        "column_pruning_diagnostic": pruning_diagnostic,
        "retrospective_column_liveness": column_liveness,
        "generalized_column_liveness": generalized,
        "trace": scored,
        "limitations": [
            "Third-party source provenance is specified by each frozen protocol; "
            "these are purposive regressions, not random production workloads.",
            "Selection excludes non-cross-dialect cases by predeclared schema/syntax rules.",
            "Cohort size and selection are frozen by the source protocol.",
            "SQLite finite row bag agreement is not a universal semantic equivalence proof.",
            "UNKNOWN is an abstention, not an incorrect optimizer transformation.",
            "Original raw AST and full JVM stderr were deleted with private temp directory."
        ]}
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, indent=2), encoding="utf8")
    print("QUERYPROOF_EXTERNAL_SUMMARY", json.dumps({
        "cases": len(observations), "errors": failed, "mismatches": mismatches,
        "events": scored["trace_events"],
        "union": coverage["union_statuses"],
        "families": coverage["per_certificate_family"],
        "posthoc_contextual_guard": contextual["certified"],
        "retrospective_collapse_project": collapsed["certified"],
        "retrospective_aggregate_identity": aggregate_identity["certified"],
        "counterexamples": len(coverage["counterexample_events"]),
        "decode_failures": len(scored["decode_failures"]),
        "instrumentation_failures": len(scored["instrumentation_failures"])}), flush=True)
    complete = (child.returncode == 0 and
                len(observations) == protocol.get("required_case_count", 18)
                and not missing and not failed and not mismatches
                and scored["trace_events"] > 0
                and not scored["decode_failures"]
                and not scored["instrumentation_failures"]
                and not scored["possible_trace_truncation"])
    return 0 if complete else 1


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--worker", action="store_true")
    p.add_argument("worker_args", nargs="*")
    args = p.parse_args()
    if args.worker:
        if len(args.worker_args) != 2:
            p.error("worker expects manifest path and private observations path")
        raise SystemExit(worker(Path(args.worker_args[0]), Path(args.worker_args[1])))
    if args.worker_args:
        p.error("unexpected positional arguments")
    raise SystemExit(parent())
