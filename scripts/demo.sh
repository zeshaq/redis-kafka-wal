#!/usr/bin/env bash
# Walks through every Op type from multiple regions and dumps Redis state
# from each region so you can see active-active convergence.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/_lib.sh

hdr "1. LWW register: concurrent SET on same key from US and EU"
prod us set --key user:42:name --value "Alice (set in US)"
prod eu set --key user:42:name --value "Alice (set in EU)"

hdr "2. G-Counter: 5 INCRs from each region (15 total)"
for _ in 1 2 3 4 5; do
  prod us incr --key counter:hits --delta 1
  prod eu incr --key counter:hits --delta 1
  prod ap incr --key counter:hits --delta 1
done

hdr "3. LWW-Element-Set: concurrent SADD/SREM"
prod us sadd --key tags --member python
prod eu sadd --key tags --member rust
prod ap sadd --key tags --member go
# A removal that may or may not win against the SADD depending on HLC order
prod us srem --key tags --member python

hdr "4. ZSET (LWW per member): leaderboard with score updates"
prod us zadd --key leaderboard --member alice --score 100
prod eu zadd --key leaderboard --member alice --score 150
prod us zadd --key leaderboard --member bob   --score 80
prod ap zadd --key leaderboard --member carol --score 120

hdr "5. Stream: XADDs from multiple regions, deterministic HLC ids"
prod us xadd --key feed --field user=alice --field msg="hello"
prod eu xadd --key feed --field user=bob   --field msg="hi"
prod ap xadd --key feed --field user=carol --field msg="howdy"

echo
echo "Sleeping 5s for MirrorMaker + consumers to converge..."
sleep 5

hdr "Per-region Redis state (should match across regions)"
for r in "${REGIONS[@]}"; do
  hdr "redis-${r}"
  echo "  user:42:name   = $(rcli "$r" GET user:42:name)"
  echo "  counter:hits   = $(rcli "$r" GET counter:hits)"
  echo "  tags           = $(rcli "$r" SMEMBERS tags | tr '\n' ' ')"
  echo "  leaderboard    ="
  rcli "$r" ZREVRANGE leaderboard 0 -1 WITHSCORES | sed 's/^/    /'
  echo "  feed (XLEN)    = $(rcli "$r" XLEN feed)"
  echo "  feed entries   ="
  rcli "$r" XRANGE feed - + | sed 's/^/    /'
done
