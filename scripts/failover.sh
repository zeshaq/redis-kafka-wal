#!/usr/bin/env bash
# Demonstrates region failure: stop EU's Kafka and consumer, keep writing
# from US and AP, observe that surviving regions still converge with each
# other. Then restart EU and watch MirrorMaker catch it up.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_lib.sh

KEY="failover:demo"

hdr "Pre-failure: write from EU, confirm everywhere"
prod eu set --key "$KEY" --value "before-failure (EU)"
sleep 2
for r in "${REGIONS[@]}"; do
  echo "  $r: $(rcli "$r" GET "$KEY")"
done

hdr "Stopping kafka-eu and consumer-eu"
docker compose stop kafka-eu consumer-eu

hdr "Writes from US and AP (EU is dark)"
prod us set --key "$KEY" --value "during-failure (US)"
sleep 1
prod ap set --key "$KEY" --value "during-failure (AP, slightly later)"
sleep 3

hdr "US and AP converged with each other"
echo "  us: $(rcli us GET "$KEY")"
echo "  ap: $(rcli ap GET "$KEY")"
echo "  eu: <region down>"

hdr "Restoring EU"
docker compose start kafka-eu consumer-eu
echo "Waiting 15s for MM2 to reconnect and catch up..."
sleep 15

hdr "EU caught up via MirrorMaker"
for r in "${REGIONS[@]}"; do
  echo "  $r: $(rcli "$r" GET "$KEY")"
done
