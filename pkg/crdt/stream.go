package crdt

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
)

// XADD with deterministic, HLC-derived stream IDs.
//
// Stream-id format:  <phys_ms>-<logical*10 + region_idx>
//   region_idx:      us=0, eu=1, ap=2
//
// Why deterministic IDs:
//   - Same event applied twice (replay, redelivery) produces the same id,
//     and Redis rejects the duplicate XADD. That's our idempotency.
//   - The id is also the HLC, so a stream consumer reads entries in HLC
//     order naturally, regardless of which region's events arrived first.
//
// Trade-off: if a remote event with an older HLC arrives AFTER a newer
// local event has already been XADDed, Redis rejects the older id (must
// be > last entry id). The event is dropped. In the lab this happens
// only when MirrorMaker is significantly lagged; we log it.
//
// The XADD itself stores the original origin region and event id as
// fields so a downstream reader can reconstruct provenance.

var xAddScript = redis.NewScript(`
-- KEYS[1] = key (stream)
-- ARGV[1] = stream_id (e.g. "1700000000000-12")
-- ARGV[2] = origin_region
-- ARGV[3] = event_id
-- ARGV[4..] = alternating field, value pairs
local args = {KEYS[1], ARGV[1]}
args[#args+1] = '__origin'
args[#args+1] = ARGV[2]
args[#args+1] = '__event_id'
args[#args+1] = ARGV[3]
for i=4,#ARGV,2 do
  args[#args+1] = ARGV[i]
  args[#args+1] = ARGV[i+1]
end
local ok, err = pcall(function() return redis.call('XADD', unpack(args)) end)
if ok then return 1 else return 0 end
`)

var regionIdx = map[string]int{"us": 0, "eu": 1, "ap": 2}

func streamID(physMs int64, logical int32, region string) string {
	idx, ok := regionIdx[region]
	if !ok {
		idx = 9
	}
	seq := int64(logical)*10 + int64(idx)
	return strconv.FormatInt(physMs, 10) + "-" + strconv.FormatInt(seq, 10)
}

func ApplyXAdd(ctx context.Context, r *redis.Client, e *event.Event) error {
	if e.XAdd == nil {
		return fmt.Errorf("XADD event without payload: %s", e)
	}
	id := streamID(e.HLC.PhysicalMs, e.HLC.Logical, e.HLC.Region)
	args := []any{id, e.OriginRegion, e.EventID}
	for k, v := range e.XAdd.Fields {
		if strings.HasPrefix(k, "__") {
			continue // reserved
		}
		args = append(args, k, v)
	}
	_, err := xAddScript.Run(ctx, r, []string{e.Key}, args...).Result()
	return err
}
