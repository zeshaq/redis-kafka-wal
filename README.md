# redis-kafka-wal-lab

A runnable lab demonstrating an active-active geo-distributed system where:

- **Kafka is the source of truth.** Every write is appended to its origin
  region's events topic. The Kafka log is durable and replayable.
- **Redis is a per-region materialized view.** A consumer in each region
  reads the local + mirrored topics and applies events to local Redis
  using CRDT-style handlers so all regions converge.
- **MirrorMaker 2** carries each region's events to the other two so all
  three Kafka clusters end up holding the full global history.

```
US region              EU region              AP region
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ Kafka-US    │⇄ │ Kafka-EU    │⇄ │ Kafka-AP    │
│  us.events  │   │  us.events  │   │  us.events  │
│  eu.events  │   │  eu.events  │   │  eu.events  │
│  ap.events  │   │  ap.events  │   │  ap.events  │
└──────┬──────┘   └──────┬──────┘   └──────┬──────┘
       │                 │                 │
       ▼                 ▼                 ▼
   Redis-US          Redis-EU          Redis-AP
```

The lab covers seven Redis ops, each with a different conflict-resolution
strategy: `SET`, `DEL`, `INCR`, `SADD`, `SREM`, `ZADD`, `XADD`.

## Documentation map

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — system-level view with diagrams
- [`docs/architecture.md`](docs/architecture.md) — event-flow + topic topology
- [`docs/consistency.md`](docs/consistency.md) — what we guarantee (and don't)
- [`docs/conflict-resolution.md`](docs/conflict-resolution.md) — CRDT math per op
- [`docs/failure-modes.md`](docs/failure-modes.md) — what breaks when
- [`docs/RUNBOOK.md`](docs/RUNBOOK.md) — operational playbook
- [`docs/GLOSSARY.md`](docs/GLOSSARY.md) — terminology
- [`docs/adr/`](docs/adr/) — Architecture Decision Records (10 of them)
- [`infra/`](infra/) — deploy targets: docker-compose (here), Kustomize, Terraform sketch
- [`ROADMAP.md`](ROADMAP.md) — known gaps and where to extend
- [`CLAUDE.md`](CLAUDE.md) — agent project memory (auto-loaded by Claude Code)

## Prerequisites

- Docker Engine 24+ and `docker compose` v2
- ~4 GB RAM free (3 Kafkas + 3 Redises + Schema Registry + MM2 + 3 consumers)
- Optional: Go 1.23+ if you want to run unit tests on the host

The producer/consumer are built inside Docker, so a host Go install is not
required to use the lab.

## Quickstart

```bash
docker compose up -d --build
docker compose ps                  # everything should be healthy
./scripts/status.sh                # per-region offsets + Redis sizes

./scripts/demo.sh                  # exercise every op type, then dump state
```

You should see all three regions report identical values for the LWW key,
identical totals for the counter, identical set members, identical
sorted-set scores, and identical stream lengths.

### Try a failure scenario

```bash
./scripts/failover.sh              # stop EU's Kafka, write from US/AP, restart, observe catch-up
./scripts/partition.sh             # disconnect EU's Kafka from the network, then heal
./scripts/replay.sh us             # wipe Redis-US and rebuild it from Kafka offset 0
```

### Hit it manually

```bash
# Issue any op against any region:
docker compose run --rm \
  -e REGION=us -e BOOTSTRAP=kafka-us:29092 \
  producer-tool set --key foo --value bar

# Read from each Redis:
for r in us eu ap; do
  port=$(case $r in us)echo 16379;; eu)echo 16380;; ap)echo 16381;; esac)
  echo "$r: $(redis-cli -p $port get foo)"
done
```

(`redis-cli` here uses the host port mapping; `docker compose exec redis-us
redis-cli get foo` works without a host install.)

## Repo layout

```
.
├── docker-compose.yml           full stack
├── docker/
│   ├── go.Dockerfile            multi-stage build for producer + consumer
│   └── mm2/mm2.properties       MirrorMaker 2 config (6 directional flows)
├── schemas/event.avsc           Avro schema for the Event record
├── scripts/                     demo + ops helpers
├── pkg/
│   ├── hlc/                     Hybrid Logical Clock
│   ├── event/                   Avro codec + Schema Registry client
│   └── crdt/                    per-op handlers (Lua scripts in Redis)
├── cmd/
│   ├── producer/                CLI: produce one op per invocation
│   └── consumer/                materializer service (one per region)
└── docs/                        architecture + consistency notes
```

## What the lab does NOT show

- Linearizable cross-region reads. The model is eventual consistency; see
  [`docs/consistency.md`](docs/consistency.md).
- A production-grade Schema Registry topology. The lab uses one global SR
  to keep the focus on Redis/Kafka. Production geo would use Confluent
  Schema Linking or per-region SRs with coordinated registration.
- Authentication, TLS, ACLs, or quotas. PLAINTEXT and no auth throughout.
- Real-time clock sync. The HLC is robust to skew but assumes wall clocks
  are roughly in the same hour.

## Cleanup

```bash
docker compose down -v
```

## License

Lab code; do whatever you want with it.
