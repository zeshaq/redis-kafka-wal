# ADR-0005: Hybrid Logical Clocks for ordering

- **Status:** Accepted
- **Date:** 2026-05-08
- **Supports:** ADR-0003 (CRDT-style resolution needs an order)

## Context

Conflict resolution (ADR-0003) requires a deterministic order on
events. We can't trust wall clocks alone (skew), and we don't want
the payload size of vector clocks (O(regions)).

Hybrid Logical Clocks (Kulkarni et al., 2014) give a total order on
events that respects causality and reduces to wall-clock time when
clocks are well-synchronized.

## Decision

Every event carries a 3-tuple HLC: `(physical_ms, logical, region)`.

- `physical_ms` is `max(observed_phys, local_now)`.
- `logical` is incremented when `physical_ms` doesn't advance, used to
  distinguish events generated within the same millisecond.
- `region` is the producer's region, used as a deterministic third
  tiebreak so two regions producing identical `(phys, logical)` still
  order consistently across the system.

Comparison order: `physical_ms` then `logical` then `region`
(lexicographic). All conflict-resolution decisions in `pkg/crdt/`
compare HLCs this way.

## Alternatives considered

- **Wall clock only** — breaks under any clock skew. A region whose
  NTP slipped 5 seconds would lose every conflict for 5 seconds.
  Rejected.
- **Lamport clock only** — captures happens-before but not physical
  time. We lose the ability to garbage-collect tombstones by age,
  and on-call humans lose intuition about "is this event recent?"
  Rejected.
- **Vector clock** — captures concurrency. Required for true OR-Sets
  (which we explicitly don't implement, ADR-0010) and other
  causality-aware CRDTs. Rejected because (a) payload size grows with
  region count, (b) we don't need to detect concurrency for any of
  the CRDTs we picked.

## Consequences

**Positive**

- 16 bytes per event covers ordering for an arbitrary number of regions.
- Time-bounded GC of tombstones is possible (`stored_phys + skew
  budget < now ⇒ collectable`).
- Wall-clock-style intuition holds when NTP is healthy.

**Negative**

- HLC does not detect concurrency. Two writes that "happened at the
  same time" are simply ordered by region tiebreak; we can't merge
  them, only pick one. This is acceptable for the CRDTs we picked
  and unacceptable for the ones we deferred (ADR-0010).
- HLC is robust to *bounded* skew. Unbounded skew (a region's clock
  hours ahead) means its events dominate every comparison. Operate
  with NTP/PTP and monitor drift.

**Obligations**

- HLC implementation lives in `pkg/hlc/`. Any change to comparison
  rules invalidates already-stored meta sidecars; data migration
  required.
- New ops that want a different ordering rule must justify it; they
  cannot simply override the comparison.
