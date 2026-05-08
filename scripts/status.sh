#!/usr/bin/env bash
# Snapshot of lab health: per-region topic offsets, consumer-group lag,
# Redis dbsize. Run any time to see how regions are tracking.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_lib.sh

for r in "${REGIONS[@]}"; do
  hdr "Region ${r}"

  echo "[topics on kafka-${r}]"
  kcli "$r" kafka-topics --bootstrap-server localhost:29092 --list \
    | grep -E '^(us|eu|ap)\.events$' || true

  echo
  echo "[end offsets on kafka-${r}]"
  for topic in us.events eu.events ap.events; do
    kcli "$r" kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list localhost:29092 --topic "$topic" --time -1 2>/dev/null \
      | awk -v t="$topic" '{ printf "  %s p%s end=%s\n", t, $2, $3 }'
  done

  echo
  echo "[consumer group lag: materializer-${r}]"
  kcli "$r" kafka-consumer-groups --bootstrap-server localhost:29092 \
    --group "materializer-${r}" --describe 2>/dev/null \
    | awk 'NR==1 || /us\.events|eu\.events|ap\.events/ { printf "  %s\n", $0 }' || true

  echo
  echo "[redis-${r} dbsize] $(rcli "$r" DBSIZE)"
done
