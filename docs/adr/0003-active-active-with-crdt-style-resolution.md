# ADR-0003: Active-active with CRDT-style conflict resolution

- **Status:** Accepted
- **Date:** 2026-05-08

## Context

In an active-active topology, two regions can write to the same key
"at the same time" (within MM2 latency). The system needs a deterministic
rule for what every region's Redis should converge to.

The two camps are:

1. **Last-Writer-Wins (LWW)** — pick one of the concurrent writes by
   timestamp (HLC in our case). Simple, convergent, drops the loser.
2. **Operation-based CRDTs** — design each operation so concurrent
   applications commute. Counters, OR-Sets, RGA-style sequences, etc.

## Decision

Use **op-appropriate convergent strategies** rather than a single rule:

- LWW register for `SET`/`DEL` (concurrent overwrites do happen, just
  pick one).
- G-Counter sharded by origin for `INCR` (commutative + idempotent
  via per-origin seq).
- LWW-Element-Set for `SADD`/`SREM` (see ADR-0010 for trade-off).
- LWW per (key, member) for `ZADD`.
- HLC-deterministic stream IDs for `XADD` (idempotent, ordered by HLC).

All of them rely on the HLC (ADR-0005) for ordering.

## Alternatives considered

- **Single-writer (active-passive)** — much simpler, no conflict
  resolution at all, but defeats geo-active-active; one region's
  outage blocks writes globally until failover. Rejected for the
  scope this lab covers.
- **Vector clocks for true causal ordering** — would let us
  distinguish "concurrent" from "happens-before", which OR-Set and
  similar genuinely need. Rejected for HLC because (a) payload size
  scales with regions, (b) most ops in this lab don't need it. See
  ADR-0010 for the LWW-Element-Set trade-off this implies.
- **Pure LWW everywhere** — simpler, but breaks counters: two
  concurrent INCRs on the same key would overwrite each other instead
  of summing. Rejected.

## Consequences

**Positive**

- Each op is convergent under any delivery order. Replay is safe.
- The choice of strategy per op type is documented and explicit
  ([`docs/conflict-resolution.md`](../conflict-resolution.md)).

**Negative**

- LWW silently drops the loser of concurrent updates. A user who
  wrote `name=Alice` from EU at time T+1ms while another wrote
  `name=Alicia` from US at T sees their write lost if HLC sorts
  the US write later. There is no merge UI for this.
- Mixed semantics across op types means contributors must understand
  *which* CRDT they're working with when they extend.

**Obligations**

- Every new op must come with a documented convergence story.
- Contributors should not mix op families on a single key (e.g. don't
  INCR a key that's also being SET; the meta sidecars don't compose).
