"""Independent falsification of QueryProof's SOURCE-PINNED frame observer.

This is not evidence that a Spark optimizer transformation is incorrect.
It proves only that the existing observation function cannot certify equality
of node-identity-indexed tag state across two plan snapshots.
"""
import hashlib
import unittest
from pathlib import Path

INSTRUMENT = (Path(__file__).resolve().parents[1] /
              "queryproof_state_frame_20261020/instrument_frame.py")


def spark_tag_string(tags):
    """Exact delimiter join used in instrument_frame.py for simple String tags."""
    return "|".join(sorted(k + ":" + v for k, v in tags.items()))


def observed_tag_multiset(assignments):
    """Actual Scala encoding: node simpleName + SHA256(tag string), sorted."""
    encoded = []
    for _node_identity, cls, tags in assignments:
        if tags:
            tag_fingerprint = hashlib.sha256(
                spark_tag_string(tags).encode("utf8")).hexdigest()
            encoded.append(cls + ":" + tag_fingerprint)
    return ",".join(sorted(encoded))


class FrameObserverNoninjectivity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = INSTRUMENT.read_text()
        # Fail closed if production instrumentation changes; a different
        # observer needs a fresh test and cannot inherit this conclusion.
        assert 'key.name + ":" + queryProofValue(value)' in source
        assert '}.sorted.mkString("|")' in source
        assert 'tags += n.getClass.getSimpleName + ":" +' in source
        assert 'queryProofHash(queryProofNodeTags(n))' in source
        assert 'Seq(tags.sorted.mkString(","), attributes.toSeq.sorted.map' in source

    def test_real_preimage_collision_is_not_sha256_collision(self):
        a = {"qpA": "one|qpB:two"}
        b = {"qpA": "one", "qpB": "two"}
        self.assertNotEqual(a, b)
        self.assertEqual(spark_tag_string(a), spark_tag_string(b))
        one = [("expr101", "AttributeReference", a)]
        two = [("expr101", "AttributeReference", b)]
        self.assertEqual(observed_tag_multiset(one),
                         observed_tag_multiset(two))

    def test_same_class_tags_can_be_swapped_between_distinct_nodes(self):
        # Nodes identified by different, unchanged ExprIds. The tag values
        # attached to those nodes are swapped, but the frame fingerprint is
        # identical. Metadata/output frame may be held unchanged.
        original = [("expr101", "AttributeReference", {"qp": "left"}),
                    ("expr202", "AttributeReference", {"qp": "right"})]
        swapped = [("expr101", "AttributeReference", {"qp": "right"}),
                   ("expr202", "AttributeReference", {"qp": "left"})]
        self.assertNotEqual({n: t for n, _, t in original},
                            {n: t for n, _, t in swapped})
        self.assertEqual(observed_tag_multiset(original),
                         observed_tag_multiset(swapped))

    def test_class_is_observed_but_not_instance_identity(self):
        # Positive detector control: when class labels differ, switching tag
        # values is observed (provided the preimage strings are distinct).
        original = [("expr101", "AttributeReference", {"qp": "left"}),
                    ("other", "Literal", {"qp": "right"})]
        swapped = [("expr101", "AttributeReference", {"qp": "right"}),
                   ("other", "Literal", {"qp": "left"})]
        self.assertNotEqual(observed_tag_multiset(original),
                            observed_tag_multiset(swapped))

    def test_snapshot_restoration_does_not_imply_no_transient_write(self):
        # This is an information-theoretic counterexample to snapshot-only
        # event tracing: before and after frames can be identical even if a
        # previously unobserved intra-rule mutation took place.
        before = [("expr101", "AttributeReference", {"qp": "one"})]
        transient = [("expr101", "AttributeReference", {"qp": "two"})]
        after = [("expr101", "AttributeReference", {"qp": "one"})]
        self.assertNotEqual(observed_tag_multiset(before),
                            observed_tag_multiset(transient))
        self.assertEqual(observed_tag_multiset(before),
                         observed_tag_multiset(after))


if __name__ == "__main__":
    unittest.main()
