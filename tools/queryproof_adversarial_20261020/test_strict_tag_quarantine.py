"""Adversarial controls for the deliberately conservative tag quarantine."""
import unittest

from strict_tag_quarantine import quarantine_tagged_cases


class StrictTagQuarantineTests(unittest.TestCase):
    def setUp(self):
        self.frame = {
            "status": "COMPLETE_FRAME_COMPARISON",
            "newly_frame_preserved_suffixes": ["H01", "H02"],
        }

    def record(self, case, tagged):
        return {"core": [case], "states": [[tagged, 0, 0, 3]]}

    def test_nonempty_tag_map_quarantines_accepted_case(self):
        report = quarantine_tagged_cases(
            self.frame, [self.record("H01", 1), self.record("H02", 0)], [])
        self.assertEqual(report["status"], "COMPLETE_STRICT_TAG_AUDIT")
        self.assertEqual(report["original_certificates_quarantined"], 1)
        self.assertEqual(report["strict_certified_suffix_cases"], ["H02"])

    def test_no_tags_keeps_original_positive(self):
        report = quarantine_tagged_cases(
            self.frame, [self.record("H01", 0), self.record("H02", 0)], [])
        self.assertEqual(report["strict_certified_changed_suffixes"], 2)

    def test_missing_or_bad_frame_fails_closed(self):
        report = quarantine_tagged_cases(self.frame, [], ["MISSING_FRAME"])
        self.assertEqual(report["status"], "INVALID_STRICT_TAG_AUDIT")
        self.assertEqual(report["strict_certified_suffix_cases"], [])

    def test_negative_count_fails_closed(self):
        report = quarantine_tagged_cases(self.frame, [self.record("H01", -1)], [])
        self.assertEqual(report["status"], "INVALID_STRICT_TAG_AUDIT")


if __name__ == "__main__":
    unittest.main()
