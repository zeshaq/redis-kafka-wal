#!/usr/bin/env bash
# Narrated flash-sale scenario.
#
# Tells a story over ~40 seconds: COMPTECH (fictional retailer) is running a
# 24-hour global flash sale across three regions. Watch the dashboard in
# parallel to see live convergence:
#
#   https://redis-kafka-wal.pages.dev
#
# This script hits the bridge HTTP API directly (orders of magnitude faster
# than spinning up a producer-tool container per event).
#
# Usage:
#   ./scripts/scenario.sh              # run the demo, leave existing state
#   ./scripts/scenario.sh --reset      # FLUSHDB on all 3 regions first
#   API=http://localhost:18080 ./scripts/scenario.sh
#
# Reads BRIDGE_WRITE_TOKEN from .env (gitignored) — the same token the
# dashboard uses for the produce panel.
set -euo pipefail
cd "$(dirname "$0")/.."

API="${API:-http://localhost:18080}"
TOKEN=$(grep '^BRIDGE_WRITE_TOKEN=' .env | cut -d= -f2-)
[ -n "$TOKEN" ] || { echo "BRIDGE_WRITE_TOKEN missing from .env"; exit 1; }

# --- color helpers -------------------------------------------------------
if [ -t 1 ]; then
  C_DIM=$'\033[2m'; C_RST=$'\033[0m'; C_HEAD=$'\033[1;36m'
  C_US=$'\033[1;32m'; C_EU=$'\033[1;34m'; C_AP=$'\033[1;35m'
  C_NOTE=$'\033[1;33m'
else
  C_DIM=""; C_RST=""; C_HEAD=""; C_US=""; C_EU=""; C_AP=""; C_NOTE=""
fi
phase() { printf "\n%s== %s ==%s\n" "$C_HEAD" "$*" "$C_RST"; }
say()   { printf "%s%s%s\n" "$C_DIM" "$*" "$C_RST"; }
note()  { printf "%s>> %s%s\n" "$C_NOTE" "$*" "$C_RST"; }

# --- API helpers ---------------------------------------------------------
produce() {
  local region="$1" body="$2"
  curl -sS -o /dev/null -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    "$API/api/produce" \
    -d "$body" \
    || echo "  (produce failed: $body)"
}
set_v()  { produce "$1" "{\"region\":\"$1\",\"op\":\"SET\",\"key\":\"$2\",\"value\":\"$3\"}"; }
del_v()  { produce "$1" "{\"region\":\"$1\",\"op\":\"DEL\",\"key\":\"$2\"}"; }
incr_v() { produce "$1" "{\"region\":\"$1\",\"op\":\"INCR\",\"key\":\"$2\",\"delta\":$3}"; }
sadd_v() { produce "$1" "{\"region\":\"$1\",\"op\":\"SADD\",\"key\":\"$2\",\"member\":\"$3\"}"; }
srem_v() { produce "$1" "{\"region\":\"$1\",\"op\":\"SREM\",\"key\":\"$2\",\"member\":\"$3\"}"; }
zadd_v() { produce "$1" "{\"region\":\"$1\",\"op\":\"ZADD\",\"key\":\"$2\",\"member\":\"$3\",\"score\":$4}"; }
xadd_v() { produce "$1" "{\"region\":\"$1\",\"op\":\"XADD\",\"key\":\"$2\",\"fields\":$3}"; }

# --- optional reset ------------------------------------------------------
if [ "${1:-}" = "--reset" ]; then
  phase "Reset — FLUSHDB on all three regions"
  for r in us eu ap; do
    docker compose exec -T "redis-${r}" redis-cli FLUSHDB | sed "s/^/  redis-${r}: /"
  done
  sleep 1
fi

# --- characters ----------------------------------------------------------
US_USERS=(alice bryan claire dan elena frank grace henry isaac jane)
EU_USERS=(lucia marco nora oscar pia quinn raul sven tilda umberto)
AP_USERS=(kenji ling minh ngozi otis priya rin satoshi taj uma)

rand_user() {
  case "$1" in
    us) printf "%s.%s\n" "${US_USERS[$RANDOM % ${#US_USERS[@]}]}" "$RANDOM";;
    eu) printf "%s.%s\n" "${EU_USERS[$RANDOM % ${#EU_USERS[@]}]}" "$RANDOM";;
    ap) printf "%s.%s\n" "${AP_USERS[$RANDOM % ${#AP_USERS[@]}]}" "$RANDOM";;
  esac
}

# --- Phase 1: setup ------------------------------------------------------
phase "Phase 1 — pre-sale setup"
say  "    US ops sets initial inventory and seeds VIP customers"

note "  US sets inventory: 100 laptops, 250 phones"
set_v us inventory:laptop "100"
set_v us inventory:phone  "250"
sleep 0.4

note "  Each region tags its founding VIP"
sadd_v us vip:customers "alice@comptech.us"
sadd_v eu vip:customers "lucia@comptech.eu"
sadd_v ap vip:customers "kenji@comptech.ap"
sleep 0.4

note "  Announcement event"
xadd_v us orders:feed '{"event":"sale_opening","msg":"Doors open in 60 seconds"}'
sleep 1

# --- Phase 2: gentle ramp ------------------------------------------------
phase "Phase 2 — sale opens, US morning shoppers wake up"
say  "    Light traffic, mostly US, some EU. Stock starts ticking down."

for i in 1 2 3 4 5 6 7 8 9 10; do
  region=us
  [ $((i % 3)) -eq 0 ] && region=eu

  user=$(rand_user "$region")
  case $((i % 4)) in
    0)
      sadd_v "$region" cart:active "$user"
      ;;
    1)
      # purchase a laptop
      price=$((RANDOM % 600 + 1200))
      incr_v "$region" sales:total 1
      incr_v us inventory:laptop -1
      zadd_v "$region" leaderboard:spenders "$user" "$price"
      xadd_v "$region" orders:feed "{\"sku\":\"laptop\",\"price\":\"$price\",\"buyer\":\"$user\"}"
      ;;
    2)
      # purchase a phone
      price=$((RANDOM % 400 + 600))
      incr_v "$region" sales:total 1
      incr_v us inventory:phone -1
      zadd_v "$region" leaderboard:spenders "$user" "$price"
      xadd_v "$region" orders:feed "{\"sku\":\"phone\",\"price\":\"$price\",\"buyer\":\"$user\"}"
      ;;
    3)
      sadd_v "$region" cart:active "$user"
      ;;
  esac
  sleep 0.55
done

# --- Phase 3: peak hour --------------------------------------------------
phase "Phase 3 — peak hour, all three regions hammering"
say  "    Concurrent writes — watch the live event log + per-region columns"

for i in $(seq 1 28); do
  region=$(case $((i % 3)) in 0) echo us;; 1) echo eu;; 2) echo ap;; esac)
  user=$(rand_user "$region")

  case $((i % 7)) in
    0|1|2)
      # purchase
      sku=$([ $((i % 2)) -eq 0 ] && echo laptop || echo phone)
      price=$((RANDOM % 1200 + 600))
      incr_v "$region" sales:total 1
      incr_v "$region" "inventory:$sku" -1
      zadd_v "$region" leaderboard:spenders "$user" "$price"
      xadd_v "$region" orders:feed "{\"sku\":\"$sku\",\"price\":\"$price\",\"buyer\":\"$user\",\"origin\":\"$region\"}"
      ;;
    3)
      # cart add
      sadd_v "$region" cart:active "$user"
      ;;
    4)
      # cart abandon (random existing user)
      victim=$(rand_user "$region")
      srem_v "$region" cart:active "$victim"
      ;;
    5)
      # VIP join
      sadd_v "$region" vip:customers "$user@$region.example.com"
      ;;
    6)
      # concurrent inventory rewrite — race with another region's INCR
      stock=$((RANDOM % 80 + 10))
      set_v "$region" inventory:lastSeen "$stock units (seen by $region)"
      ;;
  esac
  sleep 0.42
done

# --- Phase 4: convergence ------------------------------------------------
phase "Phase 4 — letting MM2 + materializers settle"
sleep 4

phase "Final per-region state"
printf "%s%-30s %-7s%s  %s%s%s  %s%s%s  %s%s%s\n" \
  "$C_DIM" "key" "type" "$C_RST" "$C_US" "redis-us" "$C_RST" \
  "$C_EU" "redis-eu" "$C_RST" "$C_AP" "redis-ap" "$C_RST"

for k in inventory:laptop inventory:phone sales:total "cart:active" leaderboard:spenders orders:feed vip:customers; do
  vals=()
  type=""
  for r in us eu ap; do
    resp=$(curl -sS "$API/api/regions/$r/key/$k?type=auto")
    type=$(echo "$resp" | jq -r '.type // "none"')
    case "$type" in
      string)
        v=$(echo "$resp" | jq -r '.value // ""')
        vals+=("$v")
        ;;
      set)
        n=$(echo "$resp" | jq '.value | length')
        vals+=("[$n members]")
        ;;
      zset)
        n=$(echo "$resp" | jq '.value | length')
        top=$(echo "$resp" | jq -r '.value[0] | "\(.member)=\(.score)"' 2>/dev/null || echo "?")
        vals+=("[$n] top=$top")
        ;;
      stream)
        n=$(echo "$resp" | jq '.xlen // 0')
        vals+=("XLEN=$n")
        ;;
      *)
        vals+=("(none)")
        ;;
    esac
  done
  printf "%-30s %-7s  %s%-22s%s  %s%-22s%s  %s%-22s%s\n" \
    "$k" "$type" \
    "$C_US" "${vals[0]}" "$C_RST" \
    "$C_EU" "${vals[1]}" "$C_RST" \
    "$C_AP" "${vals[2]}" "$C_RST"
done

phase "Done"
say  "    Every column above should match across regions."
say  "    Open the dashboard to see this live: https://redis-kafka-wal.pages.dev"
