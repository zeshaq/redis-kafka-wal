# Project memory for Claude (and other agents)

This file is loaded automatically into Claude Code's context. It exists
so a new agent landing in this repo can be productive in 60 seconds
without re-reading every file.

## What this project is

A runnable lab demonstrating an **active-active geo-distributed system**
where Kafka is the durable source of truth and per-region Redis OSS
instances are materialized views. Three regions (`us`, `eu`, `ap`),
each with its own Kafka KRaft cluster + Redis. MirrorMaker 2 replicates
origin-region topics between every cluster pair. A Go materializer in
each region applies events to local Redis using Lua scripts that
implement HLC-based LWW or G-Counter semantics per Redis op type.

It is a **lab**, not production. Two intentional simplifications are
flagged throughout: a single global Schema Registry, and LWW-Element-Set
semantics for Redis sets (not a true OR-Set).

## Layout

```
docker-compose.yml             local 3-region stack
docker/                        Dockerfile + MM2 properties
schemas/event.avsc             Avro schema for the event payload
scripts/                       demo, failover, partition, replay, status
pkg/hlc/                       Hybrid Logical Clock
pkg/event/                     Avro codec + Schema Registry HTTP client
pkg/crdt/                      per-op handlers (Lua-based atomic apply)
cmd/producer/                  cobra CLI: one op per invocation
cmd/consumer/                  materializer service
docs/                          architecture, consistency, conflicts, runbook, glossary
docs/adr/                      Architecture Decision Records
infra/k8s/                     Kustomize manifests (base + per-region overlays)
infra/terraform/               sketch for AWS multi-region (MSK + ElastiCache)
ARCHITECTURE.md                top-level, with diagrams
ROADMAP.md                     known gaps and where to extend
```

## Common commands

```bash
# Local lab
docker compose up -d --build         # build + start everything
./scripts/status.sh                  # offsets, lag, dbsize per region
./scripts/demo.sh                    # exercise all 7 ops, dump per-region state
./scripts/failover.sh                # region-down scenario
./scripts/partition.sh               # netsplit and heal
./scripts/replay.sh us               # wipe a region's Redis, replay from Kafka

# Issue arbitrary ops
docker compose run --rm \
  -e REGION=us -e BOOTSTRAP=kafka-us:29092 \
  producer-tool set --key foo --value bar

# Tests
go test ./...
```

## Conventions for changes

- **Architecture changes need an ADR.** When a load-bearing decision
  changes (transport, conflict-resolution strategy, schema topology,
  ordering model), add a new file under `docs/adr/` using the template
  in `docs/adr/0000-template.md`. Number sequentially. Reference the
  ADR from `ARCHITECTURE.md` if relevant.

- **No comments stating WHAT the code does.** Names already do that.
  Only add comments when the WHY is non-obvious — a constraint, an
  invariant, a workaround, or a CRDT-correctness subtlety.

- **Avro schema is append-only in this lab.** Compatibility is set to
  `BACKWARD`. To add a new op or payload variant: append a new record
  to the union, register, then deploy consumers before producers.

- **Idempotent handlers are mandatory.** Every new op in `pkg/crdt/`
  must be safe to apply twice. The reason is at-least-once delivery
  through Kafka + MirrorMaker.

- **Per-key per-origin order is the only ordering guarantee.** Don't
  write code that assumes anything stronger. If you need cross-key
  atomicity, do it inside a Lua script keyed by hash tag.

## Pitfalls (where past sessions tripped)

- KRaft `CLUSTER_ID` must be a base64-encoded 16-byte UUID — exactly 22
  chars, no padding. Anything else and the broker crash-loops with a
  "too long to be decoded as a base64 UUID" error. There are valid IDs
  in `docker-compose.yml`; if you generate new ones use:
  `python3 -c "import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode())"`

- MirrorMaker 2 with `IdentityReplicationPolicy` keeps topic names. It
  is critical that each `<source>-><target>.topics` only matches the
  *source's* origin topic (e.g. `us\.events`), or you'll create a loop.

- The producer/consumer images use `go mod tidy` inside the Docker
  build so a host Go install isn't required. Adding a new dep means
  re-running `docker compose build`, not edits to a checked-in
  `go.sum` (which is gitignored).

- Hamba/Avro v2's union representation for records uses the full
  namespaced name as the discriminator: `lab.geo.SetVal`, etc. See
  `pkg/event/event.go`.

## Where to read next

- `ARCHITECTURE.md` — system-level, with diagrams
- `docs/architecture.md` — event-flow + topology details
- `docs/consistency.md` — what the system guarantees and what it doesn't
- `docs/conflict-resolution.md` — CRDT math per op
- `docs/failure-modes.md` — incident catalog
- `docs/RUNBOOK.md` — what to do when things break
- `docs/adr/` — why the architecture is shaped this way
- `ROADMAP.md` — what's deliberately not built yet
