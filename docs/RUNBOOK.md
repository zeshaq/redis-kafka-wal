# Runbook

What to do when things break, in the order you'll typically diagnose.

## Quick triage

```bash
./scripts/status.sh       # offsets per region, MM2 lag, Redis dbsize
docker compose ps         # which services are healthy
docker compose logs --tail=50 <service>
```

If `status.sh` shows similar end-offsets across regions and small lag,
the system is healthy.

---

## Symptom: a region's Redis is stale

**Diagnosis**

1. `./scripts/status.sh` — what's the consumer-group lag for that
   region (`materializer-<region>`)?
2. If lag is small but Redis is stale: the materializer is running
   but not applying. Check `docker compose logs consumer-<region>`
   for `apply error` lines.
3. If lag is large: events aren't reaching the local Kafka. Check
   MM2.

**Fixes**

- Materializer apply errors → see "Materializer crash-looping" below.
- MM2 lag → see "MirrorMaker lag spiking" below.

---

## Symptom: regions disagree on a key's value

**Diagnosis**

1. Read `<key>:meta` from each region's Redis. Compare HLCs.
2. The region with the highest HLC has the "winning" value. If
   another region shows a different value but a *lower* HLC stored,
   it hasn't received the winning event yet.

```bash
for r in us eu ap; do
  port=$(case $r in us)echo 16379;; eu)echo 16380;; ap)echo 16381;; esac)
  echo "$r: $(redis-cli -p $port get mykey)"
  echo "    meta: $(redis-cli -p $port hgetall mykey:meta | xargs)"
done
```

**Expected**: HLCs converge within MM2 lag time. If they don't,
something in the replication chain is broken — see MM2 / consumer
sections below.

---

## Symptom: Kafka broker crash-loops with "invalid base64 UUID"

**Diagnosis**

```
kafka-us | Cluster ID string ... does not appear to be a valid UUID
```

**Cause**: the `CLUSTER_ID` env var doesn't decode to exactly 16 bytes
(22 chars of unpadded base64).

**Fix**

Generate a valid id:

```bash
python3 -c "import base64,uuid; print(base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b'=').decode())"
```

Update `docker-compose.yml` for the affected region. Then:

```bash
docker compose down -v        # the existing data dir was formatted with the bad ID
docker compose up -d
```

(The `-v` removes named/anonymous volumes. The Kafka data volume must
be re-formatted with the new ID.)

---

## Symptom: MirrorMaker lag spiking

**Diagnosis**

```bash
docker compose logs mirrormaker | grep -i 'lag\|warn\|error' | tail -30
```

Things that show up here:

- **Connection errors** to a peer Kafka: usually means that peer is
  down or the network is partitioned. MM2 will reconnect when the
  peer returns; lag drains over time.
- **Schema errors** ("magic byte" / "schema id"): the consumer side
  of MM2 doesn't deserialize Avro, so this isn't from MM2 itself.
  It's the materializer — see below.
- **Slow source consumer**: source-side MM2 connector is reading
  from one cluster and writing to another. If the *target* cluster
  is overloaded, ingest slows. Check the target cluster's broker
  metrics.

**Fixes**

- Restart MM2 if it's stuck: `docker compose restart mirrormaker`.
- Persistent lag → MM2 is undersized for the throughput. In the lab
  that's irrelevant; in production, run MM2 as a Kafka Connect
  cluster on the *target* side of each flow with multiple workers.

---

## Symptom: materializer crash-looping

**Diagnosis**

```bash
docker compose logs consumer-<region> --tail=100
```

Common causes:

- **Schema id mismatch** — the consumer was started before the
  Schema Registry had registered the schema. The retry policy on
  startup waits 60s for SR; if SR took longer, the consumer fails.
  → Restart the consumer: `docker compose restart consumer-<region>`.
- **Avro decode error on a poisoned message** — extremely unlikely in
  a lab but possible if you write to the topic with a non-Avro
  client. Currently the consumer fails-fast on decode errors. Two
  options:
  1. Reset the consumer group to skip past the offset:
     `kafka-consumer-groups --group materializer-<region> --reset-offsets --to-offset <offset+1> --execute`
  2. Land a fix that routes decode failures to a DLQ topic instead
     of failing.
- **Redis unreachable** — the consumer waits 30s for Redis at boot.
  If Redis is down longer, restart the consumer after Redis is up.

---

## Symptom: stream entries missing on one region

(This is the documented `XADD` caveat — see ADR-0008.)

**Cause**: an event with HLC older than the current stream tail was
rejected on apply. The event is still on Kafka, but Redis enforces
monotonic stream ids and the materializer drops the older one.

**Diagnosis**

Compare `XLEN <key>` and `XRANGE <key> - +` between regions. If one
region has fewer entries, the missing ones likely come from an
origin region whose MM2 was lagged when the local origin's events
were already ahead in HLC.

**Fix options**

- Live with it (lab default).
- Replay from offset 0 in the affected region (`scripts/replay.sh`)
  and accept that the same gap reappears (the rule is the same).
- Switch to per-origin streams with reader-side merge (architectural
  change; requires superseding ADR-0008).

---

## Symptom: schema registration fails on first boot

**Diagnosis**

```bash
docker compose logs schemas-init
```

If the SR wasn't ready when `schemas-init` ran (despite the
healthcheck), the registration POST returns an error.

**Fix**

```bash
docker compose run --rm schemas-init
```

(Re-runs the one-shot. Idempotent — re-registering the same schema
returns the existing schema id.)

---

## Symptom: a region needs to be rebuilt from scratch

**Procedure**

```bash
./scripts/replay.sh <region>
```

What it does:

1. Stops `consumer-<region>`.
2. `FLUSHDB` on the region's Redis.
3. Resets the `materializer-<region>` consumer group offsets to
   earliest on all three `*.events` topics.
4. Restarts the consumer.

The materializer rebuilds Redis state by reading all events from
every origin region in HLC order (modulo MM2 delivery order — but
HLC-gated handlers ensure the final state is correct regardless).

Caveat: streams may be "thinned" the same way they are during
normal operation (ADR-0008).

---

## Symptom: Schema Registry is down

**Impact**

- New producer/consumer instances cannot boot (they fetch the schema
  at startup).
- Existing producers and consumers continue running because the
  schema is cached in memory.

**Fix**

- Bring SR back up: `docker compose restart schema-registry`.
- If the SR's backing Kafka topic (`_schemas` on `kafka-us`) is
  corrupted, restore from a backup (you do back it up, right?) or
  re-register the schemas from `schemas/event.avsc` after recreating
  the topic. New schema ids will be assigned, breaking decoding of
  any in-flight events with the old id.

---

## Symptom: I broke something I can't diagnose

**Nuclear option**

```bash
docker compose down -v
docker compose up -d --build
./scripts/status.sh
```

`-v` wipes all volumes. You lose every event ever produced in the
lab and rebuild from zero. Always safe in the lab; never safe in
production.
