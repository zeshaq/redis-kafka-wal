package crdt

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
)

// Set membership uses LWW-Element-Set semantics rather than a true OR-Set.
// Each member carries the HLC of the last decisive op; the latest op wins.
// This is simpler than per-tag OR-Set state, idempotent, and convergent —
// the trade-off is that a concurrent SADD can lose to a slightly-later
// SREM that did not "see" it. docs/conflict-resolution.md walks through
// when that matters.
//
// Per-member sidecar: <key>:smeta is a Redis hash member->packed-hlc.
// We pack as "phys|logc|reg|op" since Redis has no nested hashes.

var sAddScript = redis.NewScript(`
-- KEYS[1] = key, KEYS[2] = key:smeta
-- ARGV[1..3] = phys, logc, reg, ARGV[4] = member
local raw = redis.call('HGET', KEYS[2], ARGV[4])
local accept = false
if not raw then
  accept = true
else
  local cphys, clogc, creg = string.match(raw, '^(%-?%d+)|(%-?%d+)|([^|]+)|')
  cphys = tonumber(cphys); clogc = tonumber(clogc)
  local iphys = tonumber(ARGV[1]); local ilogc = tonumber(ARGV[2]); local ireg = ARGV[3]
  if     iphys > cphys then accept = true
  elseif iphys == cphys and ilogc > clogc then accept = true
  elseif iphys == cphys and ilogc == clogc and ireg > creg then accept = true
  end
end
if not accept then return 0 end
redis.call('SADD', KEYS[1], ARGV[4])
redis.call('HSET', KEYS[2], ARGV[4], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3] .. '|SADD')
return 1
`)

var sRemScript = redis.NewScript(`
-- KEYS[1] = key, KEYS[2] = key:smeta
-- ARGV[1..3] = phys, logc, reg, ARGV[4] = member
local raw = redis.call('HGET', KEYS[2], ARGV[4])
local accept = false
if not raw then
  accept = true
else
  local cphys, clogc, creg = string.match(raw, '^(%-?%d+)|(%-?%d+)|([^|]+)|')
  cphys = tonumber(cphys); clogc = tonumber(clogc)
  local iphys = tonumber(ARGV[1]); local ilogc = tonumber(ARGV[2]); local ireg = ARGV[3]
  if     iphys > cphys then accept = true
  elseif iphys == cphys and ilogc > clogc then accept = true
  elseif iphys == cphys and ilogc == clogc and ireg > creg then accept = true
  end
end
if not accept then return 0 end
redis.call('SREM', KEYS[1], ARGV[4])
redis.call('HSET', KEYS[2], ARGV[4], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3] .. '|SREM')
return 1
`)

func smetaKey(k string) string { return k + ":smeta" }

func ApplySAdd(ctx context.Context, r *redis.Client, e *event.Event) error {
	if e.SAdd == nil {
		return fmt.Errorf("SADD event without payload: %s", e)
	}
	_, err := sAddScript.Run(ctx, r,
		[]string{e.Key, smetaKey(e.Key)},
		e.HLC.PhysicalMs, e.HLC.Logical, e.HLC.Region, e.SAdd.Member,
	).Result()
	return err
}

func ApplySRem(ctx context.Context, r *redis.Client, e *event.Event) error {
	if e.SRem == nil {
		return fmt.Errorf("SREM event without payload: %s", e)
	}
	_, err := sRemScript.Run(ctx, r,
		[]string{e.Key, smetaKey(e.Key)},
		e.HLC.PhysicalMs, e.HLC.Logical, e.HLC.Region, e.SRem.Member,
	).Result()
	return err
}
