#!/usr/bin/env bash
# Simulate a network partition between EU and the rest by disconnecting
# the EU kafka container from the lab network. Local writes in EU still
# succeed (Kafka-EU is up); they just don't propagate. After healing,
# MirrorMaker should reconcile both directions.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_lib.sh

NET="redis-kafka-wal-lab"
KEY="partition:demo"

hdr "Healthy: a SET in US is visible in EU and AP"
prod us set --key "$KEY" --value "pre-partition"
sleep 2
for r in "${REGIONS[@]}"; do
  echo "  $r: $(rcli "$r" GET "$KEY")"
done

hdr "Partitioning EU: disconnect kafka-eu from lab network"
docker network disconnect "$NET" kafka-eu

hdr "Concurrent writes during partition"
# Both will eventually conflict on apply; the later HLC wins after heal.
prod us set --key "$KEY" --value "during-partition (US)"
prod eu set --key "$KEY" --value "during-partition (EU, will lose)" || \
  echo "(EU producer may fail if it can't reach kafka-eu via the disconnected network)"

sleep 2
hdr "Pre-heal state"
echo "  us: $(rcli us GET "$KEY")"
echo "  ap: $(rcli ap GET "$KEY")"
echo "  eu: $(rcli eu GET "$KEY")  (only sees its own writes)"

hdr "Healing partition"
docker network connect "$NET" kafka-eu
echo "Waiting 20s for MM2 to reconnect..."
sleep 20

hdr "Post-heal — all three regions should converge"
for r in "${REGIONS[@]}"; do
  echo "  $r: $(rcli "$r" GET "$KEY")"
done
