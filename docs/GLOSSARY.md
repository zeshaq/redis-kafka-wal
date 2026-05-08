# Glossary

Terms used throughout the project, with one-sentence definitions and a
pointer to where they're discussed in depth.

**Active-active** — every region accepts both reads and writes; no
region is read-only or "secondary". Conflicting writes are reconciled
by the conflict-resolution layer rather than rejected. (ADR-0003)

**Avro** — a row-oriented data serialization format; here used for
event payloads on Kafka. The wire format is "Confluent": magic byte +
4-byte schema id + Avro binary. (ADR-0004)

**Confluent wire format** — the on-the-wire encoding produced by
Confluent's Avro/Protobuf serializers: a single 0x00 byte, a 4-byte
big-endian schema id, then the Avro binary payload. The id resolves
against a Schema Registry. (`pkg/event/event.go`)

**CRDT** — Conflict-free Replicated Data Type. A data structure
designed so that any sequence of operations from any replicas can be
merged to a consistent state without coordination. We implement
CRDT-style behavior per Redis op: LWW register, G-Counter, etc.
(ADR-0003, `docs/conflict-resolution.md`)

**Eventual consistency** — the guarantee that, in the absence of new
writes, all replicas converge to the same state. We provide *strong*
eventual consistency: once delivery completes, replicas are not just
eventually equal, they're equal *deterministically*. (`docs/consistency.md`)

**G-Counter** — a Grow-only counter CRDT. Increments are commutative
and idempotent (with seq dedup). Doesn't support decrements; for that
you'd use a PN-Counter. (`pkg/crdt/gcounter.go`)

**Hashtag (Redis)** — the substring of a key inside `{...}` that
Redis uses for cluster-shard routing. Multi-key Lua scripts in Redis
Cluster require all keys to share a hash tag. We don't run cluster in
the lab, so we don't use hashtags. (ADR-0007)

**HLC** — Hybrid Logical Clock. A 3-tuple `(physical_ms, logical,
region)` that gives a total order on events even with bounded clock
skew. (ADR-0005, `pkg/hlc/`)

**IdentityReplicationPolicy** — a MirrorMaker 2 replication policy
that preserves topic names across clusters (no source-cluster prefix).
We use it so the same topic name (`us.events`, etc.) appears on every
cluster. (ADR-0006)

**KRaft** — Kafka Raft, the consensus protocol Kafka uses since 3.x to
replace ZooKeeper for cluster metadata. Each KRaft cluster has a
`CLUSTER_ID` (a base64-encoded 16-byte UUID) used for storage
formatting. (`docs/RUNBOOK.md`)

**Last-Writer-Wins (LWW)** — a conflict-resolution rule where the
write with the largest (HLC) timestamp wins. Simple and convergent;
discards the loser. We use it for `SET`/`DEL`, set membership
(LWW-Element-Set), and per-`(key, member)` ZSET scores. (ADR-0003)

**Materializer** — the per-region Go service that consumes Kafka
events and applies them to local Redis. (`cmd/consumer/`)

**MirrorMaker 2 (MM2)** — Kafka's official tool for replicating
topics between Kafka clusters. We run it in standalone mode with all
six directional flows between our three clusters. (ADR-0002)

**Op (operation)** — one of the Redis verbs the system supports:
`SET`, `DEL`, `INCR`, `SADD`, `SREM`, `ZADD`, `XADD`. Each op has its
own conflict-resolution strategy. (`docs/conflict-resolution.md`)

**Origin region** — the region whose client produced a given event.
Encoded both in the event payload and in the topic name. (ADR-0006)

**OR-Set (Observed-Remove Set)** — a set CRDT where removes only
remove the *adds they observed*, so a concurrent add survives. We do
**not** implement this; we use LWW-Element-Set instead. (ADR-0010)

**PN-Counter** — a CRDT counter supporting both increments and
decrements. Implementation = two G-Counters (positive minus negative).
We only need a G-Counter currently.

**Producer** — the CLI tool (`cmd/producer/`) for issuing one event
per invocation against a chosen region's Kafka.

**Replay** — rebuilding Redis state from offset zero on Kafka. The
demonstration that Kafka really is the source of truth.
(`scripts/replay.sh`)

**Schema Registry (SR)** — Confluent's HTTP service that stores Avro
schemas and assigns numeric ids referenced in the wire format. The lab
uses one global SR; production needs a different topology (ADR-0004).

**Seq (sequence number)** — a monotonic-per-(origin, key) integer the
producer attaches to `INCR` events to make them idempotent on replay.
We pack the HLC into a single int for this purpose. (`pkg/crdt/gcounter.go`)

**Source of truth** — the durable store that, by definition, is
"what happened". Kafka in this system; Redis is a derived view.
(ADR-0001)

**Tombstone** — a marker indicating a key was deleted, kept around so
late-arriving SETs with smaller HLCs cannot resurrect the key. We
keep tombstones in the `<key>:meta` sidecar. (`pkg/crdt/lww.go`)

**ULID** — Universally Unique Lexicographically sortable IDentifier.
Used for `event_id`. Sorts naturally by time, which makes log scans
tidy. (`cmd/producer/main.go`)

**Vector clock** — `(region → counter)` map of last-seen counters per
region. Captures concurrency (can detect "concurrent" vs.
"happens-before"). We don't use them; HLC is sufficient for our op
mix. (ADR-0005)

**Watermark** — in stream processing, a "we won't see anything older
than this" guarantee. Not implemented in the lab; would be required
for a strict reorder-buffer fix to the XADD caveat (ADR-0008).
