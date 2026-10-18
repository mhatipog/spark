# QueryProof: cardinality × SQL Boolean observation (2026-10-18)

The causal trace, demand-aware candidate selection, and forced interventions had localized all three previously known historical Spark wrong-result queries to their first relevant optimizer rule. The tests are **in-sample**, not a blind benchmark. [End-to-end automation](https://github.com/mhatipog/spark/actions/runs/39106224959).

The empty global aggregate case B03 and the analogous nonempty N03 control demand the same full three-valued Boolean preservation and cross the same RewritePredicateSubquery rule. This branch tests a **second required property**: the original global-aggregate input cardinality, especially zero versus nonzero.

The narrow checker independently interprets a Catalyst Aggregate whose input is a small known Range, containing COUNT of a non-null literal and one uncorrelated scalar IN over another small Range. It derives range counts and subquery values from analyzed plan objects, never by parsing SQL text or asking Spark to execute an equivalent query. Explicit UNKNOWN is mandatory outside the supported grammar.

**Experimental record:** [FROZEN_PROTOCOL.json](FROZEN_PROTOCOL.json) predates implementation. The first matrix run failed closed on an implicit widening Cast of 1::int to BIGINT. A strict lossless-cast handler solved that restriction, but exposed a further harmless table-valued function Project wrapper. The follow-up now permits only identity-preserving Catalyst Projects/SubqueryAliases; arbitrary nontrivial Project/Filter/Join or unknown operators remain UNKNOWN. Each adaptation is documented and therefore the experiment is in-sample; it must **not** count as a new blind evaluation.

The product of observation type and source cardinality is standard program semantics, not original abstract algebra. The contribution, if any, must come from automatically validating optimizer transformations with independently produced result witnesses and showing useful generalization on genuinely unseen bug families.
