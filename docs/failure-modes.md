# Failure modes

What the lab handles, what it doesn't, and what `scripts/*.sh` lets you
reproduce.

## Region down (Kafka, Redis, or consumer)

Reproduce: `./scripts/failover.sh` stops `kafka-eu` and `consumer-eu`.

What happens:

- Producers in EU fail their writes. Clients should fall back to a
  nearby region's bootstrap (the lab does not implement client-side
  failover).
- US and AP continue writing locally. Their writes mirror to each
  other through MM2 normally (EU is just one of several MM2 targets,
  and each direction is independent).
- When EU comes back, MM2 reconnects, drains its accumulated lag from
  US and AP, and `consumer-eu` materializes the catch-up into Redis-EU
  in HLC order.
- The materializer's HLC-gated handlers ensure that if a US write
  arrived in EU "after" a logically-newer EU write was already applied
  during the outage window, the older US write loses on apply.

## Network partition between regions

Reproduce: `./scripts/partition.sh` disconnects `kafka-eu` from the
docker network.

What happens:

- During the partition each side accepts writes locally. EU and the
  rest diverge.
- After healing, MM2 carries the accumulated EU events outward and
  the US/AP events inward. The CRDT handlers on each side reconcile.
- Final state is identical on all three regions.

A long partition means MM2 has a large backlog to drain; while it does,
read freshness on either side suffers proportionally.

## MirrorMaker lag

The lab uses single-broker Kafkas with RF=1 and a single MM2 process.
In a real deployment you would size MM2 by partition count and run it
with HA (a Kafka Connect cluster on the target side of each flow).

Symptoms of MM2 falling behind:

- `scripts/status.sh` shows growing end-offset gaps between origin and
  destination clusters for the same topic.
- Cross-region reads stay stale.
- `XADD` events from a slow region begin to be rejected on faster
  regions (their HLC ids are older than the stream tail). The events
  remain on Kafka; on a fresh replay the ids would still be older than
  whatever the stream's tail had before that replay started, so they
  would still be skipped. **This is a real correctness gap for
  streams** and is called out in `conflict-resolution.md`.

## Consumer crash

- Offsets are committed only after a successful Redis apply, so a
  crash-during-apply leaves the offset unmoved.
- On restart the consumer re-fetches from the last committed offset.
- Replayed events are no-ops: LWW handlers reject events whose HLC ≤
  stored, INCR handlers reject events whose `seq ≤ stored_seq`,
  XADD handlers fail-silently on duplicate ids.

## Redis loss

- Redis is rebuildable from Kafka. `scripts/replay.sh` exercises
  this: stop the consumer, FLUSHDB, reset the consumer group offsets
  to earliest, restart the consumer, and watch state rebuild.
- Replay fidelity is exact for SET/DEL/SADD/SREM/ZADD/INCR. For XADD
  there is a subtle point: if the producer-supplied HLC ids on the
  topic include some out-of-order entries that were dropped on the
  *first* materialization, they would still be dropped on replay,
  because the rebuilt stream's monotonic-id rule is the same. The
  events are present in Kafka either way; they are just not visible
  via `XRANGE`.

## Schema Registry loss

The lab uses one global SR. If it goes down:

- New producers cannot serialize until it returns (they fail at
  startup-time schema lookup).
- Existing consumers continue running because they cache the schema
  in memory after the first lookup.
- Existing producers running for a while are similarly unaffected.

Production should run SR with HA (multiple instances behind a load
balancer, sharing the `_schemas` topic).

## Schema evolution

Schemas are registered with `BACKWARD` compatibility. A consumer can
read records written under any older or current schema. To add a new
op or payload variant:

1. Edit `schemas/event.avsc` (only adds, no deletes/renames).
2. Re-run the schema-init step (or restart the `schemas-init` service).
3. Deploy new consumers (they understand both old and new).
4. Deploy new producers (they emit the new schema id).

Removing or renaming a field requires `FORWARD_TRANSITIVE` plus a
multi-step deploy; out of scope for the lab.

## Clock skew

HLC is robust to bounded skew: a region whose wall clock is, say, 30s
ahead will produce HLCs that pull every other region's HLC forward
when they receive its events. The "logical" counter absorbs the skew
without losing total order.

What HLC does *not* tolerate is unbounded skew. If region-eu's clock
is hours ahead, its writes will dominate LWW comparisons for hours
even if the underlying physical events are concurrent. Run NTP/PTP.
