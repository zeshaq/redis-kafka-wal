package crdt

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
)

// ZADD uses LWW per (key, member). Sidecar <key>:zmeta hash member->HLC.
// A score update wins iff its HLC is greater than the stored one.
var zAddScript = redis.NewScript(`
-- KEYS[1] = key, KEYS[2] = key:zmeta
-- ARGV[1..3] = phys, logc, reg, ARGV[4] = member, ARGV[5] = score
local raw = redis.call('HGET', KEYS[2], ARGV[4])
local accept = false
if not raw then
  accept = true
else
  local cphys, clogc, creg = string.match(raw, '^(%-?%d+)|(%-?%d+)|([^|]+)$')
  cphys = tonumber(cphys); clogc = tonumber(clogc)
  local iphys = tonumber(ARGV[1]); local ilogc = tonumber(ARGV[2]); local ireg = ARGV[3]
  if     iphys > cphys then accept = true
  elseif iphys == cphys and ilogc > clogc then accept = true
  elseif iphys == cphys and ilogc == clogc and ireg > creg then accept = true
  end
end
if not accept then return 0 end
redis.call('ZADD', KEYS[1], ARGV[5], ARGV[4])
redis.call('HSET', KEYS[2], ARGV[4], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3])
return 1
`)

func zmetaKey(k string) string { return k + ":zmeta" }

func ApplyZAdd(ctx context.Context, r *redis.Client, e *event.Event) error {
	if e.ZAdd == nil {
		return fmt.Errorf("ZADD event without payload: %s", e)
	}
	_, err := zAddScript.Run(ctx, r,
		[]string{e.Key, zmetaKey(e.Key)},
		e.HLC.PhysicalMs, e.HLC.Logical, e.HLC.Region,
		e.ZAdd.Member, strconv.FormatFloat(e.ZAdd.Score, 'g', -1, 64),
	).Result()
	return err
}
