# Architecture

## The two-layer model

The lab separates **durability + ordering** (Kafka) from **online query**
(Redis). Every write is first appended to its origin region's Kafka topic;
that append is the only thing that "happened". Redis is recomputed from
the log. If a Redis instance is wiped, it is rebuilt by replaying the
relevant topics from offset zero — `scripts/replay.sh` exercises exactly
this.

This is the same pattern as a Kafka-Streams KTable, an Elasticsearch
materialization fed by Debezium, or a CQRS event-sourcing read model.
The wrinkle in this lab is that the materialized view is *active-active
across three regions* and the materializer must converge regardless of
the order in which it receives events from peers.

## Topic design

Three topics, one per origin region:

- `us.events`  — only ever produced into by clients in US
- `eu.events`  — only ever produced into by clients in EU
- `ap.events`  — only ever produced into by clients in AP

Each topic has 12 partitions; partitioning is by `hash(key)` so all
events for a given Redis key on a given origin are totally ordered.
Cross-origin order is not guaranteed; that's what the conflict-resolution
layer handles.

MirrorMaker 2 is configured with `IdentityReplicationPolicy` so the topic
name is the same on every cluster. Origin is encoded *in the topic name*,
which has two nice properties:

1. **No mirror loops.** `us.events` is only produced into by US clients.
   When MM2 mirrors it to EU and AP, those clusters' MM2 connectors will
   not re-mirror it back because the source-side filter (`us->eu.topics =
   us\.events` etc.) only carries each origin's own topic outward.
2. **Origin recoverable from the topic name.** Useful in tooling but the
   payload also carries `origin_region` for clarity.

Six directional MM2 flows: `us→eu`, `us→ap`, `eu→us`, `eu→ap`, `ap→us`,
`ap→eu`. All of them run inside one MM2 standalone JVM in the lab; in
production you would typically run MM2 as a Connect cluster on the
*target* side of each flow.

## Materializer

One Go service per region. It:

1. Subscribes to all three `*.events` topics on the *local* Kafka cluster.
   For region EU that means EU-originated events plus the US- and
   AP-originated events that MM2 has replicated in.
2. Decodes each record from Confluent-framed Avro (magic byte + 4-byte
   schema id + Avro binary).
3. Routes the event to a per-op handler in `pkg/crdt`. Each handler is a
   Redis Lua script that atomically reads HLC sidecar state, decides
   whether to apply the event, and writes the new state.
4. Commits the Kafka offset only after Redis acknowledges. Crash before
   commit ⇒ replay; the handlers are idempotent so this is safe.

The same handlers are used for first-time writes from local clients and
for late-arriving remote writes from MM2. The materializer makes no
distinction.

## HLC: Hybrid Logical Clock

A pure logical clock loses physical context (you can't reason about
"recent" vs "old" without it). A pure wall clock breaks under skew. HLC
is the standard fix: every event carries `(physical_ms, logical, region)`
where physical_ms is `max(observed_phys, local_now)` and logical is a
counter that breaks ties when physical_ms hasn't advanced.

In this lab the HLC also embeds the region as a deterministic third
tiebreak so two clocks producing identical `(phys, logical)` still order
consistently across the system. Order: physical → logical → region.

## Schema Registry: lab simplification

There is **one** Schema Registry in the lab, backed by `kafka-us`'s
internal `_schemas` topic. All three regions' producers and consumers
hit `http://schema-registry:8081`. This means schema IDs are globally
consistent and consumers can decode any topic without per-origin lookup.

Real production with three independent regional Kafkas would use one of:

- **Confluent Schema Linking** — schemas synchronized between SRs the
  same way MM2 synchronizes data.
- **Per-region SR + coordinated registration** — register the same
  schema text in each SR; deserializers must consult the *origin*
  region's SR (URL discoverable from a header or topic-name convention).
- **Schema embedded in headers** — every message carries its full schema
  fingerprint; no SR runtime dependency, larger payloads.

## Why this layout vs. alternatives

- **Active-active without a global Kafka cluster.** Per-region clusters
  + MM2 give every region a local-write path. A single global cluster
  would force every write through one region's brokers — exactly what
  geo distribution is trying to avoid.
- **Origin-tagged topics, not a single shared topic.** A shared topic
  needs partition assignment that respects "which region wrote it" or
  it will fight MM2. Per-origin topics are the simplest correctness
  story.
- **Materializer per region, not a global one.** Each region's
  materializer can keep up independently; a global one would be a SPOF
  region and would itself need geo-redundancy.
