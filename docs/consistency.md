# Consistency model

## What the lab guarantees

- **Strong eventual consistency.** Once all events have been delivered
  and applied everywhere, all three regions will hold identical state
  for every key, regardless of the order of arrival. This is the CRDT
  / convergent-replicated-data-type property.
- **Per-key per-origin order preservation.** All writes to key `K` from
  region `R` are applied in the order they appeared on `R.events`. Kafka
  partitioning is by `hash(K)` and each partition is consumed serially.
- **Idempotent apply.** Replaying any event a second time is a no-op
  because every handler is HLC- or seq-gated.

## What it does NOT guarantee

- **Linearizability.** Reads from Redis-EU may legitimately be older
  than what Redis-US shows, until MM2 + the EU consumer catch up.
- **Cross-key transactions.** Two related keys can apply in different
  orders in different regions if their events are on different
  partitions or originate in different regions.
- **Read-your-writes by default.** A client that writes to Redis-US
  immediately reading from Redis-US may not yet see its own write,
  because the write goes to Kafka first and Redis is downstream.
  See "Read-your-writes" below for how to upgrade.
- **Total ordering across origins.** US write at HLC `100.0` and EU
  write at HLC `99.5` to the same key will be applied in HLC order on
  every region, but operations on *different* keys can interleave
  arbitrarily.
- **Bounded staleness.** Lag depends on MM2 throughput and network
  conditions. The lab does not implement any watermark or freshness SLA.

## Read-your-writes (RYW) within a region

Disabled by default. To add it without invasive changes:

1. After producing to Kafka, the client polls
   `<key>:meta` in local Redis until the stored HLC matches or exceeds
   the HLC it just produced.
2. Bound the wait so a stalled materializer doesn't hang the client.

This buys RYW within the local region only. Cross-region RYW would
require waiting for MM2 to deliver to the other region's consumer —
typically not worth the latency cost.

## Why HLC and not vector clocks

HLC is a near-drop-in for vector clocks but with a much smaller payload
(three values instead of one per region) and with physical-time meaning
preserved. Vector clocks are required when you need to detect concurrent
updates as concurrent (and merge them as such); LWW with HLC simply
picks one of two concurrent updates as the winner. For the use cases
this lab covers (cache, session, leaderboard, counter, set-membership),
LWW is the right call. If you need true concurrent-aware merge — e.g.
collaborative-editing CRDTs — replace the LWW handlers with vector-clock
ones; the rest of the lab is unaffected.
