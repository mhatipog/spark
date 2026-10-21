"""Freeze / verify V18 components by git blob SHA.
  python freeze.py write    record the blob SHAs of FROZEN_PATHS into PROTOCOL.json
  python freeze.py verify   fail unless every recorded SHA matches the checkout,
                            and the generator reproduces COHORT.json byte for byte
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PROTOCOL = HERE / "PROTOCOL.json"
FROZEN_PATHS = [
    "tools/queryproof_v18_validated_20261021/gen_cohort.py",
    "tools/queryproof_v18_validated_20261021/COHORT.json",
    "tools/queryproof_v18_validated_20261021/differential.py",
    "tools/queryproof_v18_validated_20261021/validate.py",
    "tools/queryproof_v18_validated_20261021/summarize.py",
    "tools/queryproof_v17_heldout_20261021/gen_cohort.py",
    "tools/queryproof_v17_heldout_20261021/capture_cohort.py",
    "tools/queryproof_v17_heldout_20261021/oracle_sqlite.py",
    "tools/queryproof_v15_deep_guard_20261021/print_terms.py",
    "tools/queryproof_v15_deep_guard_20261021/run_v15.py",
    "tools/queryproof_instrumented/rank_ast.py",
    "tools/queryproof_instrumented/RuleExecutor.scala",
    "tools/queryproof_instrumented/build_overlay.sh",
    "tools/queryproof_scopes_20261020/instrument_scopes.py",
    "research/queryproof/proofs/QueryProofDeepGuard.v",
    "research/queryproof/proofs/QueryProofDeepRewrite.v",
    ".github/workflows/queryproof-v18-validated.yml",
]


def blob(path):
    return subprocess.run(["git", "hash-object", str(ROOT / path)], capture_output=True,
                          text=True, check=True).stdout.strip()


def main(mode):
    protocol = json.loads(PROTOCOL.read_text())
    if mode == "write":
        protocol["frozen_git_blob_sha"] = {p: blob(p) for p in FROZEN_PATHS}
        PROTOCOL.write_bytes((json.dumps(protocol, indent=1) + "\n").encode("utf-8"))
        print("QUERYPROOF_V18_FROZEN", len(FROZEN_PATHS))
        return 0
    recorded = protocol["frozen_git_blob_sha"]
    assert set(recorded) == set(FROZEN_PATHS), "FROZEN_SET_CHANGED"
    for path, sha in recorded.items():
        assert blob(path) == sha, f"FROZEN_COMPONENT_MODIFIED {path}"
    with tempfile.TemporaryDirectory() as tmp:
        regenerated = Path(tmp) / "COHORT.json"
        subprocess.run([sys.executable, "-B", str(HERE / "gen_cohort.py"), str(regenerated)],
                       check=True)
        assert regenerated.read_bytes() == (HERE / "COHORT.json").read_bytes(), \
            "COHORT_NOT_REPRODUCIBLE_FROM_SEED"
    print("QUERYPROOF_V18_FREEZE_VERIFIED", len(recorded))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
