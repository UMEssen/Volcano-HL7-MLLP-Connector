# 1. Kafka record key is MSH-10, not a patient identifier

* Status: Accepted
* Date: 2026-09-12
* Deciders: Volcano connector maintainers
* Technical story: UMEssen/Volcano-HL7-MLLP-Connector#16, cross-referenced from
  the engine-wide ordering-key decision UMEssen/Volcano#5

## Context and Problem Statement

Every HL7 v2 message this connector receives is produced as exactly one Kafka
record. The record key decides the partition, and the partition decides what
ordering guarantees a consumer gets: Kafka orders records within a partition
and gives no guarantee at all across partitions.

Two candidate keys were available, and they optimise for different things:

* **MSH-10** (message control id) — unique per message.
* **A patient or encounter identifier** (PID-3, PV1-19) — shared by every
  message about the same patient.

Keying by patient would make all messages for one patient land on one
partition, which is the only way Kafka can preserve their relative order. But a
Kafka record key is not payload: it is stored in cleartext in the partition
log, printed by every Kafka CLI and UI tool, surfaced in consumer-lag and
dead-letter tooling, and it appears in this connector's own log lines
(`Main.processMessage` logs `key=`). A patient identifier in that position is a
PHI disclosure into operational surfaces that are not built to hold PHI —
including surfaces operated by people with no clinical-data authorisation.

The question is therefore not "which key gives better ordering" but "is
cross-message ordering worth putting a patient identifier into the operational
plane", and that trade-off belongs in a written decision rather than a code
comment.

## Decision Drivers

* PHI must not reach operational surfaces that are not treated as clinical
  systems (log aggregation, Kafka tooling, metrics, triage UIs).
* The pipeline is at-least-once: an ACK lost after a durable produce makes the
  sender resend the identical message, so consumers need a dedup handle.
* Whatever this connector does must be reconcilable with the engine-wide
  ordering decision, not silently overridden by it.

## Considered Options

* **Option A — MSH-10 (message control id).**
* **Option B — a patient/encounter identifier (PID-3 or PV1-19).**
* **Option C — a pseudonymised patient identifier** (e.g. a keyed hash of
  PID-3) as the key.

## Decision Outcome

**Chosen: Option A, MSH-10**, because it keeps patient identifiers out of the
operational plane entirely while still giving consumers a stable dedup handle,
and because the ordering it gives up has no established consumer in this
pipeline today. Fallback when MSH-10 is absent:
`{message type}.{trigger event}-{epoch millis}`.

This decision is deliberately narrow: it settles what *this connector* keys by.
The engine-wide question — whether anything downstream needs per-patient
ordering, and what it should cost — is open in UMEssen/Volcano#5. If that
decision concludes per-patient ordering is required, Option C is the way to
satisfy it without reintroducing the PHI leak, and this ADR should be
superseded rather than quietly contradicted.

### Positive Consequences

* No patient identifier in the partition log, in Kafka tooling output, or in
  this connector's logs.
* A resend after a lost ACK carries the same MSH-10, so it produces the same
  key, lands on the same partition, and is trivially deduplicated by a consumer
  that tracks recently seen keys.
* Per-connection *arrival* order is still preserved into Kafka, because the
  handler is fully synchronous: message N+1 is not read until message N has
  been produced and acknowledged.

### Negative Consequences

* MSH-10 is high-entropy relative to any one patient, so two messages about the
  same patient routinely land on different partitions of a multi-partition
  topic, and a consumer reading several partitions sees them in no defined
  relative order.
* Any downstream stage that needs "admission before discharge" ordering must
  establish it from message content (timestamps, MSH-10 sequencing, resource
  versioning) rather than from Kafka. Nothing in this repository asserts that
  no consumer needs it — that is exactly what UMEssen/Volcano#5 has to settle.
* Compaction is not usable as a dedup mechanism on these topics: with a
  per-message key, compaction would retain every message rather than collapse
  them.

## Pros and Cons of the Options

### Option A — MSH-10

* Good, because it is non-PHI and safe in cleartext operational surfaces.
* Good, because it is unique per message and therefore a correct dedup key for
  at-least-once resends.
* Good, because it is mandatory in every HL7 v2.x version, so one code path
  covers all senders.
* Bad, because it provides no per-patient ordering.
* Bad, because it is sender-assigned: a sender that reuses control ids across
  feeds weakens the dedup property (mitigated in practice by one topic per
  feed).

### Option B — patient/encounter identifier

* Good, because it gives per-patient ordering within a topic for free.
* Bad, because it writes a direct patient identifier into the partition log,
  Kafka tooling output, and connector logs — the disclosure this decision
  exists to prevent.
* Bad, because it is not unique per message, so it is useless as a dedup key.
* Bad, because PID-3 is absent or unreliable in several message types the
  connector must still route (and MSH-9 cannot be relied on to predict that).

### Option C — pseudonymised patient identifier

* Good, because it gives per-patient co-partitioning without exposing the
  identifier.
* Bad, because it introduces a keyed-hash secret that must be provisioned,
  rotated, and kept identical across every connector instance and any consumer
  that wants to correlate — a real operational burden for an ordering
  requirement nobody has yet stated.
* Bad, because a pseudonym is still personal data under GDPR: it narrows the
  disclosure but does not remove it, so the operational surfaces still need a
  handling story.
* Neutral: it remains the right answer *if* UMEssen/Volcano#5 concludes that
  per-patient ordering is genuinely required.

## Links

* Implementation: `HL7MessageProcessor.messageKey`
* Supersedes nothing; superseded by nothing.
* Related: UMEssen/Volcano#5 (engine-wide ordering-key decision) — this ADR is
  the connector-side input to it.
