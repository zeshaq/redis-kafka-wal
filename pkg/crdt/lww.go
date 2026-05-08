// Package crdt holds the materializer-side handlers. Each op type is an
// atomic Lua script: read-the-meta, decide-to-apply, write-meta-and-value.
// Atomic application is required because the same key can be updated by
// events from up to three origin regions interleaved on the consumer.
package crdt

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
	"github.com/ze/redis-kafka-wal-lab/pkg/hlc"
)

// LWW register for SET / DEL.
//
// Sidecar key <key>:meta is a hash with the HLC of the last accepted op.
// Incoming op wins if its HLC sorts strictly after the stored one. DEL is
// kept as a tombstone in meta so a late SET with an older HLC cannot
// resurrect a deleted key.
var lwwScript = redis.NewScript(`
-- KEYS[1] = key, KEYS[2] = meta
-- ARGV[1..3] = phys, logc, reg     (incoming HLC)
-- ARGV[4]   = op                   (SET|DEL)
-- ARGV[5]   = value                (only for SET)
-- ARGV[6]   = ttl_ms               (only for SET; "0" = none)
local cur = redis.call('HMGET', KEYS[2], 'phys', 'logc', 'reg')
local cphys = tonumber(cur[1]) or -1
local clogc = tonumber(cur[2]) or -1
local creg  = cur[3] or ''
local iphys = tonumber(ARGV[1])
local ilogc = tonumber(ARGV[2])
local ireg  = ARGV[3]

local accept = false
if     iphys > cphys then accept = true
elseif iphys == cphys and ilogc > clogc then accept = true
elseif iphys == cphys and ilogc == clogc and ireg > creg then accept = true
end
if not accept then return 0 end

if ARGV[4] == 'SET' then
  redis.call('SET', KEYS[1], ARGV[5])
  local ttl = tonumber(ARGV[6])
  if ttl and ttl > 0 then redis.call('PEXPIRE', KEYS[1], ttl) end
elseif ARGV[4] == 'DEL' then
  redis.call('DEL', KEYS[1])
end

redis.call('HSET', KEYS[2], 'phys', iphys, 'logc', ilogc, 'reg', ireg, 'op', ARGV[4])
return 1
`)

func metaKey(k string) string { return k + ":meta" }

func ApplySet(ctx context.Context, r *redis.Client, e *event.Event) error {
	if e.Set == nil {
		return fmt.Errorf("SET event without payload: %s", e)
	}
	ttl := int64(0)
	if e.Set.TTLMs != nil {
		ttl = *e.Set.TTLMs
	}
	_, err := lwwScript.Run(ctx, r,
		[]string{e.Key, metaKey(e.Key)},
		e.HLC.PhysicalMs, e.HLC.Logical, e.HLC.Region,
		"SET", e.Set.Value, ttl,
	).Result()
	return err
}

func ApplyDel(ctx context.Context, r *redis.Client, e *event.Event) error {
	_, err := lwwScript.Run(ctx, r,
		[]string{e.Key, metaKey(e.Key)},
		e.HLC.PhysicalMs, e.HLC.Logical, e.HLC.Region,
		"DEL", "", 0,
	).Result()
	return err
}

// HLCFromMeta is exposed for tests / debugging — reads the current LWW
// timestamp stored alongside a key.
func HLCFromMeta(ctx context.Context, r *redis.Client, key string) (hlc.Timestamp, bool, error) {
	res, err := r.HMGet(ctx, metaKey(key), "phys", "logc", "reg").Result()
	if err != nil {
		return hlc.Timestamp{}, false, err
	}
	if res[0] == nil {
		return hlc.Timestamp{}, false, nil
	}
	var t hlc.Timestamp
	if s, ok := res[0].(string); ok {
		fmt.Sscanf(s, "%d", &t.PhysicalMs)
	}
	if s, ok := res[1].(string); ok {
		var v int32
		fmt.Sscanf(s, "%d", &v)
		t.Logical = v
	}
	if s, ok := res[2].(string); ok {
		t.Region = s
	}
	return t, true, nil
}
