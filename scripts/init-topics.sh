#!/usr/bin/env bash
# Create the three origin-region topics in each cluster. We pre-create on
# every cluster (rather than letting auto-create handle it) so that:
#   1. Partition count is identical everywhere — important because we
#      partition by hash(key) and want a key to land on the same partition
#      number across regions.
#   2. MirrorMaker has a target topic to write into immediately.
set -euo pipefail

PARTITIONS=12
RF=1

create_topics() {
  local bootstrap="$1"
  local label="$2"
  echo "[$label] creating topics on $bootstrap"
  for region in us eu ap; do
    kafka-topics --bootstrap-server "$bootstrap" \
      --create --if-not-exists \
      --topic "${region}.events" \
      --partitions "$PARTITIONS" \
      --replication-factor "$RF"
  done
  kafka-topics --bootstrap-server "$bootstrap" --list
}

create_topics kafka-us:29092 us
create_topics kafka-eu:29092 eu
create_topics kafka-ap:29092 ap

echo "topics-init: done"
