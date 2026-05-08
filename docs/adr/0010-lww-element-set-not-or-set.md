# ADR-0010: LWW-Element-Set for SADD/SREM (deferring true OR-Set)

- **Status:** Accepted **with explicit limitation**
- **Date:** 2026-05-08
- **Supersedes nothing; will be superseded if/when a workload demands true OR-Set semantics.**

## Context

The CRDT literature offers several convergent set types:

- **G-Set** — add-only.
- **2P-Set** — once removed, never re-addable.
- **LWW-Element-Set** — each member tracks "last add HLC" and "last
  remove HLC"; member is visible iff `add_hlc > remove_hlc`.
- **OR-Set (Observed-Remove Set)** — every add carries a unique tag;
  a remove only removes the *tagged adds it observed*. Concurrent
  add + remove ⇒ add wins.

The OR-Set is the strongest answer for collaborative use cases (a
shopping cart where one device adds an item while another removes it
should keep the add). It requires per-tag state and a notion of
causal observation — typically vector clocks.

Our HLC (ADR-0005) gives a total order, not concurrency detection.
Implementing a true OR-Set on top of HLC would either:

(a) blow up to vector clocks just for sets, OR
(b) approximate OR-Set semantics with a heuristic (concurrent ⇒ add
wins) that's still wrong in some cases.

## Decision

Implement **LWW-Element-Set** for `SADD`/`SREM`:

- Per-member sidecar `<key>:smeta` stores the last decisive op
  (`SADD` or `SREM`) and its HLC.
- Latest HLC wins. Concurrent `SADD` from EU and `SREM` from US
  resolves to whichever has the larger HLC (with region tiebreak).

Documented as such in `pkg/crdt/orset.go` and
`docs/conflict-resolution.md`. The filename retains "orset" as a
historical reminder.

## Alternatives considered

- **True OR-Set with vector clocks** — correct but pulls vector
  clocks into the rest of the system. Most other ops (LWW register,
  G-Counter, ZSET, Stream) don't need them. Rejected as scope creep.
- **OR-Set with HLC tags as "unique"** — HLC tags are unique per
  producer, so we could treat them as OR-Set tags, but we'd still
  lack the "remove only the adds it observed" rule without something
  causal-aware on the side. Rejected as semantic theater.
- **Skip set ops entirely** — the user asked for all 5 op families
  (KV, counter, set, zset, stream). Rejected.

## Consequences

**Positive**

- Convergent, idempotent, simple to reason about.
- One sidecar per member; storage scales linearly with set size.

**Negative — explicit limitation**

In active-active, a concurrent `SADD m` and `SREM m` resolves to one
of them by HLC, not by causal observation. A user who removed `m`
from EU at HLC 100 and another who added `m` from US at HLC 99 will
see the member *removed*, even though the EU remove couldn't have
"observed" the US add. In OR-Set semantics, the add would survive.

**When this matters**

- Shopping carts under bursty cross-region edits.
- Tag sets where the human intent is "add wins on conflict".

**When it doesn't matter**

- Feature flag sets, admin-managed config, slowly-changing tag
  catalogs — anywhere the human writers are coordinating out-of-band.

**Obligations**

- If a workload appears that requires OR-Set semantics, this ADR is
  superseded before that workload ships. The replacement work
  involves either pulling in vector clocks for sets (and updating
  `pkg/hlc/` callers) or building a separate vector-clock-aware
  set CRDT alongside the existing one.
