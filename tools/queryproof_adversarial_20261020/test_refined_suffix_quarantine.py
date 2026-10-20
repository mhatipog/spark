"""Unit tests for normalization-cut-local tag quarantine."""
import unittest

from refined_suffix_quarantine import (
    tagged_optimizer_suffix_cases, suffix_tag_quarantine)


def scope(kind, batch, phase, tagged=0, uid="1", case="H01"):
    return {"kind": "SCOPE",
            "core": [case, kind, "Optimizer", batch, uid, phase],
            "states": [[tagged, 0, 0, 3]]}


def event(tagged=0, case="H01"):
    return {"kind": "EVENT",
            "core": [case, "Optimizer", "Aggregate", "Rule", "1"],
            "states": [[tagged, 0, 0, 3], [tagged, 0, 0, 3]]}


def trace(pre_tag=0, cut_tag=0, suffix_tag=0):
    return [
        scope("EXECUTOR", "", "BEGIN", uid="1"),
        scope("BATCH", "Finish Analysis", "BEGIN", tagged=pre_tag, uid="2"),
        scope("BATCH", "Finish Analysis", "END", tagged=cut_tag, uid="2"),
        scope("BATCH", "Aggregate", "BEGIN", uid="3"),
        event(tagged=suffix_tag),
        scope("BATCH", "Aggregate", "END", uid="3"),
        scope("EXECUTOR", "", "END", uid="1"),
    ]


class RefinedSuffixTagTests(unittest.TestCase):
    def setUp(self):
        self.original = {"status": "COMPLETE_FRAME_COMPARISON",
                         "newly_frame_preserved_suffixes": ["H01"]}

    def test_pre_normalization_tag_does_not_quarantine(self):
        result = suffix_tag_quarantine(self.original, trace(pre_tag=1), [])
        self.assertEqual(result["status"], "COMPLETE_SUFFIX_TAG_AUDIT")
        self.assertEqual(result["strict_certified_suffix_cases"], ["H01"])

    def test_normalization_cut_tag_quarantines(self):
        result = suffix_tag_quarantine(self.original, trace(cut_tag=1), [])
        self.assertEqual(result["strict_certified_changed_suffixes"], 0)

    def test_post_normalization_tag_quarantines(self):
        result = suffix_tag_quarantine(self.original, trace(suffix_tag=1), [])
        self.assertEqual(result["strict_certified_changed_suffixes"], 0)

    def test_missing_frame_fails_closed(self):
        result = suffix_tag_quarantine(self.original, trace(), ["MISSING_FRAME"])
        self.assertEqual(result["status"], "INVALID_SUFFIX_TAG_AUDIT")
        self.assertEqual(result["strict_certified_suffix_cases"], [])

    def test_missing_cut_fails_closed(self):
        incomplete = [scope("EXECUTOR", "", "BEGIN"),
                      scope("EXECUTOR", "", "END")]
        result = suffix_tag_quarantine(self.original, incomplete, [])
        self.assertEqual(result["status"], "INVALID_SUFFIX_TAG_AUDIT")

    def test_tag_scanner_reports_no_false_pre_cut_signal(self):
        tagged, cuts = tagged_optimizer_suffix_cases(trace(pre_tag=2))
        self.assertFalse(tagged)
        self.assertEqual(cuts["H01"], 1)


if __name__ == "__main__":
    unittest.main()
