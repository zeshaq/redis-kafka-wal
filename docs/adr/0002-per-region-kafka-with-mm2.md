# ADR-0002: Per-region Kafka clusters with MirrorMaker 2

- **Status:** Accepted
- **Date:** 2026-05-08
- **Builds on:** ADR-0001

## Context

Given Kafka is the source of truth (ADR-0001), where do its brokers
live? Three layouts apply:

1. **Single global cluster** — one Kafka, possibly in one region. All
   writes from every region cross the WAN.
2. **Stretch cluster** — one logical Kafka with brokers in each region;
   replicas spread by rack-awareness; writes pay cross-region quorum
   latency.
3. **Per-region clusters with replication between them** — local writes
   stay local; MirrorMaker 2 moves them between clusters.

## Decision

Each region runs its own single-node Kafka KRaft cluster. MirrorMaker 2
in standalone mode runs all six directional flows (`us↔eu`, `us↔ap`,
`eu↔ap`) so every cluster eventually holds the full global event
history. The replication policy is `IdentityReplicationPolicy` so the
topic name is identical across clusters; origin is encoded in the
topic name (see ADR-0006).

## Alternatives considered

- **Single global cluster** — every write pays cross-region latency.
  Defeats the point of having a local Redis. The cluster itself is a
  SPOF region. Rejected.
- **Stretch cluster** — strong consistency comes at the cost of
  cross-region quorum on every produce. Operationally heavy and not
  the right educational target for a lab. Rejected.
- **Confluent Cluster Linking** — better than MM2 in production
  (offset preservation, transactional support) but Confluent-licensed,
  not OSS. Out of scope. Future migration target if we keep this
  layout but go enterprise.

## Consequences

**Positive**

- Local writes are local. Producers in EU never wait on the WAN.
- Clusters are independently operable; one can be patched/restarted
  without touching the others.
- The MM2 lag becomes the visible "freshness budget" between regions
  — a single, observable quantity.

**Negative**

- More clusters to operate (3× everything).
- MM2 itself is a moving piece with its own failure modes
  (see [`docs/failure-modes.md`](../failure-modes.md)).
- Offsets are not portable across clusters: a consumer cannot fail
  over from kafka-eu to kafka-us and resume at the same offset
  without translation. We intentionally don't try (each region's
  consumer group is local-only).

**Obligations**

- Origin-region topics (`us.events` etc.) must only be produced into
  by their owning region. Enforced by convention in the producer CLI
  and assumed by the MM2 config; violating it creates loops.
- Internal MM2 topic replication factors are pinned to 1 in the lab
  (single-broker clusters). Bump these when going multi-broker.
