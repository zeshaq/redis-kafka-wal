# ADR-0008: HLC-deterministic stream IDs for XADD

- **Status:** Accepted **with known caveat**
- **Date:** 2026-05-08
- **Supports:** ADR-0003 (XADD's convergence story)

## Context

`XADD` is structurally different from the other ops:

- Streams are append-only; there's no "concurrent overwrite" to resolve.
- Redis Streams enforce **monotonic stream IDs**: a new entry must have
  an id strictly greater than the last entry in that stream.
- We want the same logical event to produce the same Redis stream
  entry id on every region (so reading is deterministic and replay is
  idempotent).

Redis lets you either auto-assign an id (`*`) or supply an explicit
one. Auto-assign would give different ids on each region for the same
logical event — bad.

## Decision

Use explicit stream ids derived from the event's HLC:

```
stream_id = "<phys_ms>-<logical*10 + region_idx>"
where region_idx ∈ {us:0, eu:1, ap:2}
```

This format:

- Is deterministic across regions: same event, same id everywhere.
- Sorts in HLC order under Redis's stream-id comparison.
- Provides idempotency on replay: a duplicate `XADD` errors and we
  treat it as already-applied.

Implementation: `pkg/crdt/stream.go`. The Lua script wraps the `XADD`
in `pcall` so the duplicate-id error becomes a no-op.

## Alternatives considered

- **Auto-assigned ids** (`XADD key * field value...`) — different ids
  per region; cross-region correlation is impossible; replay creates
  duplicates. Rejected.
- **One stream per origin region** (`<key>:<region>`) — sidesteps
  monotonic-id conflicts entirely; reader merges across the three
  streams by HLC. Cleaner long-term answer. Deferred — see Caveat.
- **Reorder buffer with watermark** — consumer holds events back
  until all origins have advanced past time `T`, then applies in HLC
  order. Eliminates the dropping caveat but adds latency and a
  watermark-tracking subsystem. Rejected for the lab.

## Consequences

**Positive**

- Stream entries are HLC-ordered everywhere; `XRANGE` reads in causal
  order.
- Idempotency is structural (duplicate ids are rejected by Redis).

**Negative — known correctness caveat**

If MirrorMaker is significantly lagged, an old US event can arrive at
EU **after** EU has already XADDed a newer local event with a higher
HLC id. Redis rejects the older id (must be `> last entry`), and our
script silently drops the event.

Effect: a region under heavy MM2 lag sees a "thinned" stream view —
some entries from other regions are missing. The Kafka log still has
them; a fresh replay from offset 0 would show the same gap because
the stream's monotonic-id rule doesn't change on replay.

**Mitigations available** (not in lab):

- Switch to per-origin streams (the deferred alternative above).
- Add a reorder buffer with a watermark.
- Surface dropped XADDs to a metrics endpoint so operators see the lag.

**Obligations**

- Document the caveat anywhere `XADD` behavior is described
  ([`docs/conflict-resolution.md`](../conflict-resolution.md),
  [`docs/failure-modes.md`](../failure-modes.md)).
- If a workload depends on streams not being thinned, this ADR must
  be superseded *before* that workload ships.
