#!/usr/bin/env bash
# Seed the live-event app's initial state: title, speaker, topic, status,
# poll question and options. Idempotent — re-running just overwrites.
#
# Usage:
#   ./scripts/seed-event.sh           # default content (defaults below)
#   TITLE=... SPEAKER=... ./scripts/seed-event.sh
#   POLL_Q=... POLL_OPTS="A,B,C" ./scripts/seed-event.sh
set -euo pipefail
cd "$(dirname "$0")/.."

API="${API:-http://localhost:18080}"
TOKEN=$(grep '^BRIDGE_WRITE_TOKEN=' .env | cut -d= -f2-)
[ -n "$TOKEN" ] || { echo "BRIDGE_WRITE_TOKEN missing from .env"; exit 1; }

TITLE="${TITLE:-All-Hands Q4 2026}"
SPEAKER="${SPEAKER:-Sarah CEO}"
TOPIC="${TOPIC:-Roadmap & open Q&A}"
STATUS="${STATUS:-live}"
ADMIN_REGION="${ADMIN_REGION:-us}"

POLL_Q="${POLL_Q:-Best new feature this quarter?}"
POLL_OPTS="${POLL_OPTS:-Dark mode,AI assistant,Performance,Mobile app}"

set_v() {
  # printf '%s' avoids the trailing newline that bash <<< would add.
  local jval
  jval=$(printf '%s' "$2" | jq -Rsa .)
  curl -sS -o /dev/null -X POST \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    "$API/api/produce" \
    -d "{\"region\":\"$ADMIN_REGION\",\"op\":\"SET\",\"key\":\"$1\",\"value\":$jval}"
}

echo "==> seeding event header"
set_v event:title   "$TITLE"
set_v event:speaker "$SPEAKER"
set_v event:topic   "$TOPIC"
set_v event:status  "$STATUS"

echo "==> seeding poll question"
set_v event:poll:question "$POLL_Q"

echo "==> seeding poll options"
IFS=',' read -ra OPTS <<< "$POLL_OPTS"
for i in "${!OPTS[@]}"; do
  opt=$(echo "${OPTS[$i]}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
  set_v "event:poll:option:$i" "$opt"
  echo "  option $i = $opt"
done

# Clear out any previous higher-index option keys the new poll doesn't use.
for ((i=${#OPTS[@]}; i<8; i++)); do
  curl -sS -o /dev/null -X POST \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    "$API/api/produce" \
    -d "{\"region\":\"$ADMIN_REGION\",\"op\":\"DEL\",\"key\":\"event:poll:option:$i\"}"
done

echo
echo "DONE. Open https://redis-kafka-wal.pages.dev/live to view."
echo "Title:   $TITLE"
echo "Speaker: $SPEAKER"
echo "Poll:    $POLL_Q"
echo "Options: $POLL_OPTS"
