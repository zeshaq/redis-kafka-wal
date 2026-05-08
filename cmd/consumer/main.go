// consumer is the materializer service. Per region, it subscribes to all
// three origin topics on the local Kafka cluster (the local one and the
// two MirrorMaker-replicated ones), decodes Avro, and applies each event
// idempotently to the local Redis via the per-op CRDT handlers.
//
// Offsets are committed only after Redis apply succeeds, so a crash
// before commit replays the events safely (LWW/seq-dedup makes that a
// no-op when state is already current).
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ze/redis-kafka-wal-lab/pkg/crdt"
	"github.com/ze/redis-kafka-wal-lab/pkg/event"
)

func main() {
	region := envOr("REGION", "us")
	bootstrap := envOr("BOOTSTRAP", "kafka-"+region+":29092")
	redisAddr := envOr("REDIS_ADDR", "redis-"+region+":6379")
	srURL := envOr("SR_URL", "http://schema-registry:8081")

	log.Printf("consumer starting region=%s bootstrap=%s redis=%s sr=%s",
		region, bootstrap, redisAddr, srURL)

	ctx, cancel := signalContext()
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := waitRedis(ctx, rdb); err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer rdb.Close()

	sr := event.NewSRClient(srURL)
	if err := sr.WaitReady(60 * time.Second); err != nil {
		log.Fatalf("sr: %v", err)
	}
	// Subscriptions can carry events under any of the three subjects;
	// they share one schema id in this lab so we resolve once.
	id, schemaJSON, err := sr.LatestForSubject("us.events-value")
	if err != nil {
		log.Fatalf("sr lookup: %v", err)
	}
	codec, err := event.NewCodec(schemaJSON, id)
	if err != nil {
		log.Fatalf("codec: %v", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumerGroup("materializer-"+region),
		kgo.ConsumeTopics("us.events", "eu.events", "ap.events"),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(20*time.Second),
	)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer cl.Close()

	log.Printf("consumer ready, subscribed to us.events,eu.events,ap.events")

	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) {
					return
				}
				log.Printf("fetch error topic=%s partition=%d: %v", fe.Topic, fe.Partition, fe.Err)
			}
		}

		// All-or-nothing per poll: if any apply fails we do NOT commit
		// and exit so the container is restarted. Idempotent handlers
		// make replay of already-applied records a no-op.
		failed := false
		count := 0
		fetches.EachRecord(func(rec *kgo.Record) {
			if failed {
				return
			}
			e, err := codec.Decode(rec.Value)
			if err != nil {
				log.Printf("decode error topic=%s partition=%d offset=%d: %v",
					rec.Topic, rec.Partition, rec.Offset, err)
				failed = true
				return
			}
			if err := crdt.Apply(ctx, rdb, e); err != nil {
				log.Printf("apply error topic=%s offset=%d %s: %v",
					rec.Topic, rec.Offset, e, err)
				failed = true
				return
			}
			count++
			log.Printf("applied %s topic=%s offset=%d", e, rec.Topic, rec.Offset)
		})
		if failed {
			log.Fatalf("aborting poll without commit; container will restart")
		}
		if count > 0 {
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				log.Printf("commit error: %v", err)
			}
		}
		cl.AllowRebalance()
	}
}

func waitRedis(ctx context.Context, rdb *redis.Client) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("redis ping timeout")
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
