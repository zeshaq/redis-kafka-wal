# ADR-0001: Kafka as the source of truth, Redis as a materialized view

- **Status:** Accepted
- **Date:** 2026-05-08

## Context

We need a geo-distributed key/value store that is durable, auditable,
and replayable. The two natural shapes are:

1. **Redis-as-system** with replication built in (Redis Enterprise CRDB
   is the productized version of this).
2. **Kafka-as-log** with Redis as a downstream materialized view.

We also want the writes to be consumable by *other* systems —
analytics pipelines, search indices, fraud detection. That points
toward externalizing the log.

## Decision

Every write becomes an event appended to a Kafka topic before it is
visible anywhere. The Redis instance in each region is a materialized
view rebuilt by a consumer of those topics. Redis is fully derivable
from the Kafka log; the log is the only thing that "happened".

## Alternatives considered

- **Redis as source of truth, with Kafka CDC for downstream** — relies
  on a CDC mechanism Redis OSS doesn't have natively (no logical
  replication, no WAL exposed). Workable with keyspace notifications
  but lossy and racey.
- **Dual-write to Redis and Kafka from the application** — classic
  inconsistency footgun. Application can fail between the two writes;
  there's no transaction across them.
- **Redis Enterprise active-active** — solves the geo problem but the
  log is internal; we can't feed analytics from it without an
  additional CDC mechanism, and the licensing cost is real.

## Consequences

**Positive**

- Replay-from-zero is a first-class operation
  ([`scripts/replay.sh`](../../scripts/replay.sh)).
- Other systems (analytics, search, audit) can consume the same log
  without any change to the write path.
- Failure recovery is well-understood: Kafka guarantees durability,
  Redis is rebuildable.

**Negative**

- The write path has more hops: produce → Kafka commit → MM2 → consume
  → Lua apply → Redis. Latency is higher than a direct Redis-to-Redis
  protocol like Redis Enterprise.
- Two systems to operate (Kafka + Redis) where one would do.

**Obligations**

- All handlers in `pkg/crdt/` must be idempotent so replay is safe.
- New ops must be expressible as Avro union variants on the Event
  record; we never write to Redis without going through the log.
