# ADR-0004: Avro on the wire, single Schema Registry (lab simplification)

- **Status:** Accepted (lab); **production migration required** (see Consequences)
- **Date:** 2026-05-08

## Context

Event payloads need a wire format. Options:

1. **JSON** — easy to debug, larger, no schema enforcement.
2. **Protobuf** — compact, schemas-as-code, requires a generator step.
3. **Avro + Schema Registry** — production-realistic in the Kafka
   ecosystem, schemas-as-data, central registry.

Per-region Kafka clusters (ADR-0002) raise a follow-up: if we use SR,
do we run one per region or one global instance? Per-region is more
faithful to a real geo deployment but introduces schema-id divergence
between regions, which makes mirrored topics painful to deserialize
without consulting the *origin* region's SR.

## Decision

- **Use Avro + Confluent wire format** (magic byte + 4-byte schema id +
  Avro binary) for the event payload.
- **Run a single global Schema Registry** in the lab, backed by
  `kafka-us`'s `_schemas` topic. All regions resolve schemas through it.

The single-SR choice is a deliberate **lab simplification**. Production
geo would not do this.

## Alternatives considered

- **JSON** — too easy. Doesn't exercise the Avro/SR machinery that's
  ubiquitous in real Kafka shops. Rejected for educational realism.
- **Protobuf** — requires a `protoc` build step; Schema Registry's
  Protobuf support exists but is less canonical than Avro. Rejected
  to keep dependency count low.
- **Per-region Schema Registry, schemas registered identically at boot**
  — would work but each SR assigns its own ids, so a record produced
  in US has id=1 there but might be id=4 in EU's SR. Consumers
  reading mirrored topics would need to consult the origin region's
  SR. Rejected as too much complexity for what the lab is teaching.
- **Confluent Schema Linking** — the right answer in production but
  Confluent-licensed.

## Consequences

**Positive**

- Schema IDs are globally consistent. The materializer can decode any
  topic without provenance-aware lookup.
- The wire format matches what Confluent shops use; tooling like
  `kafka-avro-console-consumer` works out of the box.

**Negative**

- The single SR is a SPOF region. If `kafka-us` is the region that
  goes down, new instances anywhere in the system can't start until
  it recovers.
- This is **not how a real geo deployment** would do it. Promoting
  this lab toward production requires moving to one of:
  - Confluent Schema Linking (paid).
  - Per-region SRs with coordinated registration (we control the
    registration order so all SRs agree on ids; deserializers fall
    back to a global subject map).
  - Schema fingerprint in the message header instead of an SR id
    lookup — payloads are larger but the runtime SR dependency goes
    away.

**Obligations**

- Schema changes go through `BACKWARD` compatibility. Append-only
  edits to `schemas/event.avsc`; never rename or remove fields
  without bumping a major version and writing a migration ADR.
- Document the SR topology before changing it; this ADR will need to
  be superseded.
