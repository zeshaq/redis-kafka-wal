# Roadmap

What's deliberately not built and where to extend.

## Known correctness gaps

### XADD entries dropped on lag

**Problem.** `XADD` uses HLC-deterministic stream ids (ADR-0008). When
MirrorMaker is significantly lagged, an old remote event can arrive
*after* the local stream has already advanced past its HLC; Redis
rejects the older id and the materializer drops it.

**Fix sketches**:

- **Per-origin streams + reader-side merge** — store `<key>:us`,
  `<key>:eu`, `<key>:ap` separately; readers merge by HLC. No
  monotonic-id conflicts because each stream is single-writer in
  effect. ETA: ~1 day for the data path; readers are an API change.
- **Reorder buffer with watermark** — consumer holds events back
  until all origins have advanced past time `T`, then applies in
  HLC order. Adds latency, gains correctness. ETA: ~2 days.

### LWW-Element-Set instead of OR-Set

**Problem.** Concurrent `SADD m` and `SREM m` resolves to the
larger-HLC op, not the causally-newer op (ADR-0010). For
collaborative use cases this is wrong.

**Fix sketches**:

- **Per-tag OR-Set** — every `SADD` carries a unique tag; remove only
  removes tags the remover observed. Requires vector-clock-style
  observation tracking on the producer or a separate "observed" set
  in Redis. Significant work; one of the few places where vector
  clocks would actually be earned.

## Production-readiness gaps

### Authentication, TLS, ACLs

The lab is PLAINTEXT throughout. Production needs:

- **Kafka**: SASL/SCRAM or mTLS on inter-broker and client listeners.
  Use separate listeners for client traffic vs. replication traffic.
- **Redis**: ACLs (Redis 6+) and TLS.
- **Schema Registry**: HTTP basic auth or OIDC.
- **MirrorMaker**: encrypted client connections to both source and
  target Kafkas; per-flow credentials.

### Observability

The lab has `scripts/status.sh`. Production needs:

- Metrics: Kafka JMX → Prometheus, materializer's own metrics
  (records-per-second, decode-error-rate, apply-latency,
  events-dropped-by-lww-rejection), MM2's per-flow lag.
- Logs: structured JSON instead of `log.Printf`, shipped to a
  central log store.
- Traces: optional, but adding OpenTelemetry to the materializer
  would be a couple hundred lines.

### Multi-broker Kafka per region

The lab runs a single broker per region (RF=1). Production needs at
minimum 3 brokers per region with `replication.factor=3`,
`min.insync.replicas=2`, `acks=all`. The compose stack assumes RF=1
in many places (MM2 internal topics, `_schemas`, application topics);
each one is a one-line change but they have to be consistent.

### Backups

Kafka tiered storage to S3 (KIP-405) is now in 3.6+. Combined with a
weekly snapshot of `_schemas`, this gets you durable beyond the
cluster's lifetime. Out of scope here.

### CI/CD

No GitHub Actions workflow currently. A reasonable starter:

- `go test ./...` on every push.
- `docker compose build` smoke test.
- `kubectl --dry-run=client apply -k infra/k8s/overlays/us` to
  validate manifests.

Nothing fancy; just enough to catch breakage.

## Feature extensions

### More Redis ops

The lab covers `SET`/`DEL`/`INCR`/`SADD`/`SREM`/`ZADD`/`XADD`. Not
covered:

- **`HSET`/`HDEL`** — could be implemented as LWW per (key, field).
  Direct extension of `pkg/crdt/lwwset.go`.
- **`LPUSH`/`RPUSH`** — lists are notoriously hard CRDTs (RGA, Yjs,
  …). Recommendation: don't try; if you need ordered append, use a
  stream.
- **`PFADD` (HyperLogLog)** — HLL is naturally a CRDT (set-union
  merge). One Lua script, ~30 lines.
- **TTL semantics** — partially supported (`SET ... ttl_ms`); needs
  a clearer story for "TTL was set on key X, and a later SET
  without TTL arrived — what happens?" Currently the new SET clears
  the TTL implicitly (Redis behavior). Document or harden.

### TTL-aware tombstone GC

`<key>:meta` tombstones live forever in the lab. A real deployment
needs to GC them after `now > stored_phys + max_skew + max_lag`.
~50 lines if you have a known upper bound on lag.

### Read-your-writes within region

After `produce`, the producer could poll `<key>:meta` until its HLC is
visible. Add an `--await-rww` flag. ~30 lines.

### Demo extensions

- A web UI showing live state from each region in side-by-side panels.
- A Grafana dashboard JSON file.
- A scenario harness that runs a sequence of writes and validates
  convergence with a deadline (i.e. an integration test).

## Architectural escape hatches

If you outgrow this lab, here's the shape of the next architectures:

- **Need lower latency?** Move to Redis Enterprise active-active and
  use Kafka only as a CDC sink for downstream consumers (the
  inverse of what we do).
- **Need stronger consistency?** Run a single-region single-writer
  CP store (FoundationDB, CockroachDB, Spanner) as the source of
  truth and use Redis only as a regional cache.
- **Need a queryable global view?** Add a Materialize / RisingWave /
  Flink job consuming the same Kafka topics; it gives you SQL over
  the global stream, separate from Redis.

Pick one when the lab's premises (active-active, Redis is the only
view) stop matching reality.
