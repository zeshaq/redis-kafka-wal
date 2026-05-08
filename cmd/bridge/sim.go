package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
)

// simManager tracks the state of the in-bridge traffic simulator. The
// dashboard exposes start/stop/rate controls that drive this struct.
type simManager struct {
	mu       sync.Mutex
	running  bool
	rate     int       // intended events/sec
	started  time.Time // when last started
	cancel   context.CancelFunc
	done     chan struct{} // closed when the goroutine exits; recreated on each start
	produced atomic.Int64
}

type simState struct {
	Running  bool      `json:"running"`
	Rate     int       `json:"rate"`
	Started  time.Time `json:"started,omitempty"`
	UptimeS  int64     `json:"uptime_s"`
	Produced int64     `json:"produced"`
}

func (sm *simManager) state() simState {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := simState{
		Running:  sm.running,
		Rate:     sm.rate,
		Produced: sm.produced.Load(),
	}
	if sm.running {
		out.Started = sm.started.UTC()
		out.UptimeS = int64(time.Since(sm.started).Seconds())
	}
	return out
}

// start atomically transitions to running. Returns false if already running.
// run is the goroutine body; it must respect ctx and exit when canceled.
func (sm *simManager) start(parent context.Context, rate int, run func(ctx context.Context, rate int)) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.running {
		return false
	}
	ctx, cancel := context.WithCancel(parent)
	sm.running = true
	sm.rate = rate
	sm.started = time.Now()
	sm.cancel = cancel
	sm.done = make(chan struct{})
	sm.produced.Store(0)
	done := sm.done
	go func() {
		defer close(done)
		run(ctx, rate)
		sm.mu.Lock()
		sm.running = false
		sm.cancel = nil
		sm.mu.Unlock()
	}()
	return true
}

// stop signals the running goroutine to exit and waits up to 2s for it to
// actually finish, so the response reflects the post-stop state. Returns
// false if not running.
func (sm *simManager) stop() bool {
	sm.mu.Lock()
	if !sm.running || sm.cancel == nil {
		sm.mu.Unlock()
		return false
	}
	sm.cancel()
	done := sm.done
	sm.mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}
	return true
}

// updateRate adjusts the intended rate while running. The current ticker
// interval gets re-evaluated on the next loop iteration.
func (sm *simManager) updateRate(rate int) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.running {
		return false
	}
	sm.rate = rate
	return true
}

// readRate is racy on purpose; the loop reads it each tick to pick up
// rate changes without restarting.
func (sm *simManager) readRate() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.rate
}

// --- traffic generation -------------------------------------------------

var (
	simRegions = []string{"us", "eu", "ap"}
	simSKUs    = []string{"laptop", "phone", "tablet", "headphones", "watch", "keyboard", "monitor"}
	simUsers   = map[string][]string{
		"us": {"alice", "bryan", "claire", "dan", "elena", "frank", "grace", "henry", "isaac", "jane"},
		"eu": {"lucia", "marco", "nora", "oscar", "pia", "quinn", "raul", "sven", "tilda", "umberto"},
		"ap": {"kenji", "ling", "minh", "ngozi", "otis", "priya", "rin", "satoshi", "taj", "uma"},
	}
)

// runSim is the simulator goroutine body. It fires one simStep per tick,
// adjusting the ticker if the rate is changed externally.
func (s *server) runSim(ctx context.Context, initialRate int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rate := initialRate
	ticker := time.NewTicker(intervalFor(rate))
	defer ticker.Stop()

	carts := make([]string, 0, 64)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// pick up rate changes without restart
			if r := s.sim.readRate(); r != rate {
				rate = r
				ticker.Reset(intervalFor(rate))
			}
			s.simStep(ctx, rng, &carts)
			s.sim.produced.Add(1)
		}
	}
}

func intervalFor(rate int) time.Duration {
	if rate < 1 {
		rate = 1
	}
	return time.Second / time.Duration(rate)
}

// simStep picks a region, an op, and produces. Produce errors are logged
// but do not stop the loop — transient Kafka issues shouldn't kill the
// simulator.
func (s *server) simStep(ctx context.Context, rng *rand.Rand, carts *[]string) {
	region := simRegions[rng.Intn(len(simRegions))]
	user := simRandUser(rng, region)
	roll := rng.Intn(100)

	mustProduce := func(req produceReq) {
		if _, err := s.doProduce(ctx, req); err != nil {
			// soft-fail; the simulator should be resilient
			_ = err
		}
	}

	switch {
	case roll < 50:
		// purchase: increment sales, decrement inventory, leaderboard, order event
		sku := simSKUs[rng.Intn(len(simSKUs))]
		price := rng.Intn(1200) + 200
		mustProduce(produceReq{Region: region, Op: event.OpINCR, Key: "sales:total", Delta: 1})
		mustProduce(produceReq{Region: region, Op: event.OpINCR, Key: "inventory:" + sku, Delta: -1})
		mustProduce(produceReq{Region: region, Op: event.OpZADD, Key: "leaderboard:spenders", Member: user, Score: float64(price)})
		mustProduce(produceReq{Region: region, Op: event.OpXADD, Key: "orders:feed", Fields: map[string]string{
			"sku":   sku,
			"price": fmt.Sprintf("%d", price),
			"buyer": user,
		}})
	case roll < 70:
		// cart add
		mustProduce(produceReq{Region: region, Op: event.OpSADD, Key: "cart:active", Member: user})
		*carts = append(*carts, user)
		if len(*carts) > 50 {
			*carts = (*carts)[1:]
		}
	case roll < 80:
		// cart drop
		if len(*carts) > 0 {
			idx := rng.Intn(len(*carts))
			victim := (*carts)[idx]
			*carts = append((*carts)[:idx], (*carts)[idx+1:]...)
			mustProduce(produceReq{Region: region, Op: event.OpSREM, Key: "cart:active", Member: victim})
		} else {
			mustProduce(produceReq{Region: region, Op: event.OpSREM, Key: "cart:active", Member: user})
		}
	case roll < 90:
		// VIP join
		mustProduce(produceReq{Region: region, Op: event.OpSADD, Key: "vip:customers", Member: user + "@" + region + ".example.com"})
	case roll < 95:
		// restock
		stock := rng.Intn(500) + 100
		mustProduce(produceReq{Region: region, Op: event.OpSET, Key: "inventory:laptop", Value: fmt.Sprintf("%d", stock)})
	default:
		// concurrent inventory rewrite — deliberate LWW race
		note := fmt.Sprintf("%s thinks: %d units left", region, rng.Intn(100))
		mustProduce(produceReq{Region: region, Op: event.OpSET, Key: "inventory:lastSeen", Value: note})
	}
}

func simRandUser(rng *rand.Rand, region string) string {
	pool := simUsers[region]
	return fmt.Sprintf("%s.%d", pool[rng.Intn(len(pool))], rng.Intn(99999))
}
