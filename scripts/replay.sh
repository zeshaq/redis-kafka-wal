#!/usr/bin/env bash
# Replay-from-zero: wipe a region's Redis, reset its consumer group, and
# rebuild state from the Kafka log. This is the exercise that proves
# Kafka really is the source of truth.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_lib.sh

REGION="${1:-us}"
GROUP="materializer-${REGION}"

hdr "Stopping consumer-${REGION}"
docker compose stop "consumer-${REGION}"

hdr "FLUSHDB on redis-${REGION}"
rcli "$REGION" FLUSHDB

hdr "Resetting consumer group offsets to earliest"
for topic in us.events eu.events ap.events; do
  docker compose exec -T "kafka-${REGION}" \
    kafka-consumer-groups --bootstrap-server localhost:29092 \
    --group "$GROUP" --topic "$topic" \
    --reset-offsets --to-earliest --execute || true
done

hdr "Restarting consumer-${REGION}"
docker compose start "consumer-${REGION}"

echo "Tailing consumer logs for 10s; ^C earlier if you're satisfied"
sleep 1
timeout 10s docker compose logs -f --tail=20 "consumer-${REGION}" || true

hdr "Final state in redis-${REGION}"
echo "  dbsize = $(rcli "$REGION" DBSIZE)"
