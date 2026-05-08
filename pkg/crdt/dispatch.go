package crdt

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
)

// Apply routes an event to the right per-op handler.
func Apply(ctx context.Context, r *redis.Client, e *event.Event) error {
	switch e.Op {
	case event.OpSET:
		return ApplySet(ctx, r, e)
	case event.OpDEL:
		return ApplyDel(ctx, r, e)
	case event.OpINCR:
		return ApplyIncr(ctx, r, e)
	case event.OpSADD:
		return ApplySAdd(ctx, r, e)
	case event.OpSREM:
		return ApplySRem(ctx, r, e)
	case event.OpZADD:
		return ApplyZAdd(ctx, r, e)
	case event.OpXADD:
		return ApplyXAdd(ctx, r, e)
	default:
		return fmt.Errorf("unknown op %q", e.Op)
	}
}
