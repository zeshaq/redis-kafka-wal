package crdt

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
)

// G-Counter style INCR.
//
// Plain INCRBY is commutative across regions but NOT idempotent — replaying
// a Kafka event a second time would double-count. We dedup per origin
// region using a monotonically-increasing seq the producer attaches:
//
//	<key>           INT total counter
//	<key>:seq:<reg> max seq we have already applied from <reg>
//
// Only events with seq > stored max are applied; the rest are dropped.
var incrScript = redis.NewScript(`
-- KEYS[1] = key, KEYS[2] = key:seq:<region>
-- ARGV[1] = delta (signed), ARGV[2] = seq
local cur_seq = tonumber(redis.call('GET', KEYS[2]) or '-1')
local in_seq  = tonumber(ARGV[2])
if in_seq <= cur_seq then return 0 end
redis.call('INCRBY', KEYS[1], tonumber(ARGV[1]))
redis.call('SET', KEYS[2], in_seq)
return 1
`)

func ApplyIncr(ctx context.Context, r *redis.Client, e *event.Event) error {
	if e.Incr == nil {
		return fmt.Errorf("INCR event without payload: %s", e)
	}
	seqKey := e.Key + ":seq:" + e.OriginRegion
	_, err := incrScript.Run(ctx, r,
		[]string{e.Key, seqKey},
		e.Incr.Delta, e.Incr.Seq,
	).Result()
	return err
}
