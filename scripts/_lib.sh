#!/usr/bin/env bash
# Shared helpers for ops scripts.

REGIONS=(us eu ap)

# prod <region> <producer-args...>
# Runs the producer container against the chosen region's Kafka.
prod() {
  local region="$1"; shift
  docker compose run --rm -T \
    -e REGION="$region" \
    -e BOOTSTRAP="kafka-${region}:29092" \
    producer-tool "$@"
}

# rcli <region> <redis-cli args...>
rcli() {
  local region="$1"; shift
  docker compose exec -T "redis-${region}" redis-cli "$@"
}

# kcli <region> <kafka-topics/kafka-consumer-groups args...>
kcli() {
  local region="$1"; shift
  docker compose exec -T "kafka-${region}" "$@"
}

hr() { printf '%s\n' "------------------------------------------------------------"; }
hdr() { hr; printf '== %s\n' "$*"; hr; }
