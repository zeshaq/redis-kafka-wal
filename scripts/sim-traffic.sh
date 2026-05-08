#!/usr/bin/env bash
# Continuous traffic simulator. Generates realistic mixed flash-sale
# traffic against the bridge HTTP API, indefinitely, until ^C.
#
# Use this when showing the dashboard to people: leave it running so the
# event stream is never idle and the per-region columns are always
# moving in some way.
#
# Usage:
#   ./scripts/sim-traffic.sh                # ~2 events/sec
#   ./scripts/sim-traffic.sh --rate 5       # ~5 events/sec
#   API=http://localhost:18080 ./scripts/sim-traffic.sh
#
# Behaviour:
#   * Each event picks a region uniformly at random
#   * Op mix:  50% purchase (incr+zadd+xadd+incr-inventory)
#              20% cart add  (sadd)
#              10% cart drop (srem)
#              10% VIP join  (sadd)
#               5% inventory restock (set inventory:laptop bumped)
#               5% concurrent inventory write (LWW race)
set -euo pipefail
cd "$(dirname "$0")/.."

API="${API:-http://localhost:18080}"
TOKEN=$(grep '^BRIDGE_WRITE_TOKEN=' .env | cut -d= -f2-)
[ -n "$TOKEN" ] || { echo "BRIDGE_WRITE_TOKEN missing from .env"; exit 1; }

RATE=2
while [ $# -gt 0 ]; do
  case "$1" in
    --rate) RATE="$2"; shift 2;;
    --rate=*) RATE="${1#*=}"; shift;;
    -h|--help)
      cat <<USAGE
sim-traffic.sh [--rate N]   default rate=2 events/sec
               API=...      bridge base URL (default http://localhost:18080)
USAGE
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
SLEEP=$(awk -v r="$RATE" 'BEGIN { printf "%.3f\n", 1.0 / r }')

REGIONS=(us eu ap)
US=(alice bryan claire dan elena frank grace henry isaac jane)
EU=(lucia marco nora oscar pia quinn raul sven tilda umberto)
AP=(kenji ling minh ngozi otis priya rin satoshi taj uma)
SKUS=(laptop phone tablet headphones watch keyboard monitor)

rand_user() {
  case "$1" in
    us) printf "%s.%s\n" "${US[$RANDOM % 10]}" "$RANDOM";;
    eu) printf "%s.%s\n" "${EU[$RANDOM % 10]}" "$RANDOM";;
    ap) printf "%s.%s\n" "${AP[$RANDOM % 10]}" "$RANDOM";;
  esac
}

produce() {
  curl -sS -o /dev/null -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    "$API/api/produce" -d "$1" || true
}

# Session-local set of "active carts" we'll churn through.
ACTIVE_CARTS=()

trap 'echo; echo "stopped after $count events"; exit 0' INT TERM

count=0
echo "sim-traffic: ${RATE} events/sec → $API"
echo "  ^C to stop"
echo

while true; do
  region="${REGIONS[$RANDOM % 3]}"
  user=$(rand_user "$region")
  roll=$((RANDOM % 100))

  if   [ "$roll" -lt 50 ]; then
    # purchase
    sku="${SKUS[$RANDOM % ${#SKUS[@]}]}"
    price=$((RANDOM % 1200 + 200))
    produce "{\"region\":\"$region\",\"op\":\"INCR\",\"key\":\"sales:total\",\"delta\":1}"
    produce "{\"region\":\"$region\",\"op\":\"INCR\",\"key\":\"inventory:$sku\",\"delta\":-1}"
    produce "{\"region\":\"$region\",\"op\":\"ZADD\",\"key\":\"leaderboard:spenders\",\"member\":\"$user\",\"score\":$price}"
    produce "{\"region\":\"$region\",\"op\":\"XADD\",\"key\":\"orders:feed\",\"fields\":{\"sku\":\"$sku\",\"price\":\"$price\",\"buyer\":\"$user\"}}"
    [ $((RANDOM % 3)) -eq 0 ] && {
      # buyer was in cart, remove them
      [ ${#ACTIVE_CARTS[@]} -gt 0 ] && {
        idx=$((RANDOM % ${#ACTIVE_CARTS[@]}))
        produce "{\"region\":\"$region\",\"op\":\"SREM\",\"key\":\"cart:active\",\"member\":\"${ACTIVE_CARTS[$idx]}\"}"
        unset 'ACTIVE_CARTS[idx]'
        ACTIVE_CARTS=("${ACTIVE_CARTS[@]}")
      }
    }
  elif [ "$roll" -lt 70 ]; then
    # cart add
    produce "{\"region\":\"$region\",\"op\":\"SADD\",\"key\":\"cart:active\",\"member\":\"$user\"}"
    ACTIVE_CARTS+=("$user")
    # keep memory bounded
    [ ${#ACTIVE_CARTS[@]} -gt 50 ] && ACTIVE_CARTS=("${ACTIVE_CARTS[@]:1}")
  elif [ "$roll" -lt 80 ]; then
    # cart drop (an existing one if we have any, otherwise random)
    if [ ${#ACTIVE_CARTS[@]} -gt 0 ]; then
      idx=$((RANDOM % ${#ACTIVE_CARTS[@]}))
      produce "{\"region\":\"$region\",\"op\":\"SREM\",\"key\":\"cart:active\",\"member\":\"${ACTIVE_CARTS[$idx]}\"}"
      unset 'ACTIVE_CARTS[idx]'
      ACTIVE_CARTS=("${ACTIVE_CARTS[@]}")
    else
      produce "{\"region\":\"$region\",\"op\":\"SREM\",\"key\":\"cart:active\",\"member\":\"$user\"}"
    fi
  elif [ "$roll" -lt 90 ]; then
    # VIP join
    produce "{\"region\":\"$region\",\"op\":\"SADD\",\"key\":\"vip:customers\",\"member\":\"$user@$region.example.com\"}"
  elif [ "$roll" -lt 95 ]; then
    # restock
    new=$((RANDOM % 500 + 100))
    produce "{\"region\":\"$region\",\"op\":\"SET\",\"key\":\"inventory:laptop\",\"value\":\"$new\"}"
  else
    # concurrent inventory rewrite — LWW race
    note="$region thinks: $((RANDOM % 100)) units left"
    produce "{\"region\":\"$region\",\"op\":\"SET\",\"key\":\"inventory:lastSeen\",\"value\":\"$note\"}"
  fi

  count=$((count + 1))
  printf "\r  events produced: %d   carts tracked: %d   " "$count" "${#ACTIVE_CARTS[@]}"
  sleep "$SLEEP"
done
