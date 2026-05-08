# Conflict resolution per op

Each op type uses a different convergent strategy. All of them are
implemented as Redis Lua scripts (see `pkg/crdt/`) so check-and-apply is
atomic against concurrent applies on the same key.

## SET / DEL — LWW register

- Sidecar key `<key>:meta` is a hash with `phys`, `logc`, `reg`, `op`
  describing the last accepted op.
- Incoming op wins iff its HLC sorts strictly after the stored HLC
  (lexicographic on `(phys, logc, reg)`).
- DEL is a tombstone: the key is deleted but its meta is preserved, so
  a late SET with a smaller HLC cannot resurrect it.

Trade-off: a concurrent `SET v1` and `SET v2` from two regions resolves
to whichever has the higher HLC; the other write is silently discarded.
In active-active LWW, that's the rule.

## INCR — sharded G-Counter with seq dedup

- Single counter at `<key>` for ergonomic reads.
- Per-origin dedup state at `<key>:seq:<region>`. Producer attaches a
  monotonic `seq` (HLC packed into one int).
- Apply iff `seq > stored_seq[origin]`. INCRBY is commutative, so any
  apply order produces the same total — but it is *not* idempotent
  without the seq gate.

Trade-off: works for additive deltas only. INCR + DEL or INCR + SET
together don't compose cleanly; use one op family per key.

## SADD / SREM — LWW-Element-Set

- Per-member sidecar `<key>:smeta` (Redis hash, member → packed HLC).
- Each new SADD/SREM for member `m` wins iff its HLC is greater than
  what's stored for `m`.
- The Redis set `<key>` is the "currently visible" projection.

Trade-off: this is *not* a true OR-Set. In a real OR-Set, a concurrent
SADD always survives a concurrent SREM that didn't observe it; here a
slightly-later SREM (by HLC) wins regardless of causal observation.
For most "tag set" / "feature flag set" use cases LWW-Element-Set is
fine; for shopping carts where "I added this in another region while
you removed it" should preserve the add, switch to a per-tag OR-Set.

## ZADD — LWW per (key, member)

- Per-member sidecar `<key>:zmeta` (Redis hash, member → packed HLC).
- ZADD wins iff its HLC > stored HLC for that member.
- The Redis sorted set `<key>` reflects the latest accepted score per
  member.

Trade-off: scores are not additive. Two regions both writing scores for
`alice` will keep the highest-HLC one, not the highest score. If you
want "highest score wins", swap the comparison in `lwwset.go` to
`accept = score > stored_score`; this is also convergent.

## XADD — HLC-deterministic stream IDs

- Stream entry id = `<phys_ms>-<logical*10 + region_idx>` where
  `region_idx` maps `us=0 eu=1 ap=2`.
- Same event applied twice produces the same id; Redis rejects the
  duplicate, giving us idempotency.
- Reading entries in stream-id order yields HLC order, which is the
  closest thing to a meaningful global timeline this system can offer.

Trade-off: a remote event arriving with an id older than the current
stream tail is rejected (Redis enforces monotonic ids). In practice this
happens when MirrorMaker is significantly lagged. The dropped event is
*not* lost on Kafka; it stays on the topic and would replay on a fresh
materializer. But the live stream view skips it. Two ways to harden:

1. Maintain one stream per origin region and merge in the reader.
2. Run a small reorder buffer with a watermark in the consumer that
   waits for events from all origins up to time `T` before applying.

Both are more code than fits in a lab; see `docs/failure-modes.md`.

## Tombstone GC

The lab does not garbage-collect `*:meta` sidecars. In a long-running
deployment you would expire them with a TTL once `now > stored_phys +
max_clock_skew + max_replication_lag`. The window must exceed both
clock skew and the worst-case "remote write that arrives very late"
window, or you risk resurrecting deleted keys.
