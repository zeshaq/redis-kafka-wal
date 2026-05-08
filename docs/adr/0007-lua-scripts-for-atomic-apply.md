# ADR-0007: Lua scripts for atomic conflict-resolving apply

- **Status:** Accepted
- **Date:** 2026-05-08
- **Supports:** ADR-0003

## Context

Each Redis op needs to: read its meta sidecar, decide whether the
incoming HLC sorts later than the stored one, and either apply the
change (and update meta) or skip. This is **read-decide-write** and
must be atomic against concurrent applies — a single materializer
process consuming three origin topics will interleave events for the
same key.

Redis offers three atomicity primitives:

1. **`WATCH/MULTI/EXEC`** — optimistic locking with retry on conflict.
2. **`MULTI/EXEC`** alone — atomic, but no read-then-decide.
3. **Lua scripting (`EVAL`/`EVALSHA`)** — entire script runs as a
   single atomic unit, full read-decide-write inside the server.

## Decision

Implement each op handler as a Redis Lua script in `pkg/crdt/`. Each
script:

- takes the relevant key(s) in `KEYS` and HLC + payload in `ARGV`,
- reads the meta sidecar,
- compares HLCs,
- writes value + new meta only if the incoming HLC wins,
- returns 0 (rejected) or 1 (applied).

The Go client uses `redis.NewScript(...)`'s `Run` method, which falls
back from `EVALSHA` to `EVAL` automatically on cache miss.

## Alternatives considered

- **`WATCH/MULTI/EXEC`** — works but means every contended apply
  retries. With three origins interleaving on the same key, contention
  on hot keys would be measurable. Lua avoids the retry loop entirely.
  Rejected.
- **Read in Go, then `MULTI/EXEC`** — same retry problem; even the
  initial read is racing against another consumer's write. Rejected.
- **Server-side modules (RedisGears, Redis Functions)** — more
  expressive but introduces a server-side runtime and module-load
  ceremony. Lua is built in. Rejected for now; reconsider if scripts
  outgrow Lua.

## Consequences

**Positive**

- Apply is atomic by construction. No retries, no race window between
  read and write.
- The CRDT logic lives in one place per op (one Lua script + one Go
  shim). Easy to audit.
- Lua scripts are cached server-side via SHA; the wire cost is
  effectively zero after the first invocation.

**Negative**

- Lua's string handling is awkward; we use `string.match` with a
  pipe-delimited packed format for nested HLCs in hash values
  (e.g. `pkg/crdt/orset.go`). A future contributor may misread the
  format.
- Lua scripts block Redis's single thread. Long scripts hurt
  throughput. Ours are O(1) Redis calls each, which is fine.
- Scripts run on a single shard. Multi-key scripts in Redis Cluster
  require all keys to share a hash tag. We don't run cluster in the
  lab; keys + their meta sidecars use the natural `<key>` and
  `<key>:meta` form. Going to Redis Cluster requires adding hash
  tags (e.g. `{<key>}:meta`).

**Obligations**

- New op handlers must:
  - Be expressed as a single Lua script.
  - Read all sidecar state at the top, decide, write at the bottom.
  - Return 0 or 1 explicitly (not pcall results, not Redis errors).
- Scripts must pass all keys via `KEYS` (not interpolate them in the
  source). This is a Redis Cluster compatibility rule we honor even
  though the lab uses single-node Redis.
