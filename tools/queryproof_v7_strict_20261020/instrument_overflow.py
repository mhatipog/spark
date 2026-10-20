"""Disposable last-stage V8 instrumentation: explicit global effective-event overflow.
Only apply after the pinned V6 scope/state/frame overlays. No persistent Spark code changes.
"""
from pathlib import Path

SOURCE = Path("tools/queryproof_instrumented/RuleExecutor.scala")
HEAD = "  private val seen = new java.util.concurrent.atomic.AtomicInteger(0)"
TAIL = "    }\n  }\n}\n\nclass PlanChangeLogger"

def patch_text(source):
    if source.count(HEAD) != 1 or source.count(TAIL) != 1:
        raise ValueError("Cannot locate uniquely pinned instrumentation anchors")
    if "QUERYPROOF_AST_OVERFLOW" in source:
        raise ValueError("Overflow instrumentation already applied")
    source = source.replace(
        HEAD,
        HEAD + "\n  private val overflowSignaled = new java.util.concurrent.atomic.AtomicBoolean(false)"
    )
    replacement = (
        '    } else if (java.lang.Boolean.getBoolean("queryproof.ast.enabled") &&\n'
        '        before.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] &&\n'
        '        seen.get() > 2500 && overflowSignaled.compareAndSet(false, true)) {\n'
        '      System.err.println("QUERYPROOF_AST_OVERFLOW")\n'
        '    }\n  }\n}\n\nclass PlanChangeLogger'
    )
    return source.replace(TAIL, replacement)

if __name__ == "__main__":
    SOURCE.write_text(patch_text(SOURCE.read_text()))
    print("QUERYPROOF_COMPLETE_TRACE_OVERFLOW_GUARD_READY")
