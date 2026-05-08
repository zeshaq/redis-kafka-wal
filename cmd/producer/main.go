// producer is a CLI for issuing writes against a chosen origin region's
// Kafka cluster. Each subcommand maps to one Op in the Avro schema.
//
// At startup the producer fetches the latest schema for the topic
// "<region>.events-value" from the Schema Registry, then produces
// Confluent-framed Avro records keyed by the Redis key (so partitioning
// preserves per-key order within an origin).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
	"github.com/ze/redis-kafka-wal-lab/pkg/hlc"
)

type rootFlags struct {
	region    string
	bootstrap string
	srURL     string
}

var defaultBootstrap = map[string]string{
	"us": "localhost:19092",
	"eu": "localhost:19093",
	"ap": "localhost:19094",
}

func main() {
	var rf rootFlags
	cmd := &cobra.Command{
		Use:   "producer",
		Short: "Produce events to a region's local Kafka cluster.",
	}
	cmd.PersistentFlags().StringVarP(&rf.region, "region", "r", envOr("REGION", "us"), "origin region (us|eu|ap)")
	cmd.PersistentFlags().StringVarP(&rf.bootstrap, "bootstrap", "b", os.Getenv("BOOTSTRAP"), "Kafka bootstrap servers (defaults by region)")
	cmd.PersistentFlags().StringVar(&rf.srURL, "sr", envOr("SR_URL", "http://localhost:8081"), "Schema Registry URL")

	cmd.AddCommand(setCmd(&rf), delCmd(&rf), incrCmd(&rf), sAddCmd(&rf), sRemCmd(&rf), zAddCmd(&rf), xAddCmd(&rf))

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// --- subcommands ---------------------------------------------------------

func setCmd(rf *rootFlags) *cobra.Command {
	var key, value string
	var ttlMs int64
	c := &cobra.Command{
		Use:   "set",
		Short: "SET key=value (LWW register)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			payload := &event.SetVal{Value: []byte(value)}
			if ttlMs > 0 {
				payload.TTLMs = &ttlMs
			}
			return runOp(rf, key, event.OpSET, func(e *event.Event) { e.Set = payload })
		},
	}
	c.Flags().StringVarP(&key, "key", "k", "", "Redis key (required)")
	c.Flags().StringVarP(&value, "value", "v", "", "value")
	c.Flags().Int64Var(&ttlMs, "ttl-ms", 0, "TTL in milliseconds (0 = none)")
	c.MarkFlagRequired("key")
	return c
}

func delCmd(rf *rootFlags) *cobra.Command {
	var key string
	c := &cobra.Command{
		Use:   "del",
		Short: "DEL key (LWW tombstone)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runOp(rf, key, event.OpDEL, func(e *event.Event) { e.Del = &event.DelVal{} })
		},
	}
	c.Flags().StringVarP(&key, "key", "k", "", "Redis key (required)")
	c.MarkFlagRequired("key")
	return c
}

func incrCmd(rf *rootFlags) *cobra.Command {
	var key string
	var delta int64
	c := &cobra.Command{
		Use:   "incr",
		Short: "INCR key by delta (G-counter, idempotent via seq)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runOp(rf, key, event.OpINCR, func(e *event.Event) {
				// seq monotonic per origin: HLC packed into one int.
				seq := e.HLC.PhysicalMs*1_000_000 + int64(e.HLC.Logical)
				e.Incr = &event.IncrVal{Delta: delta, Seq: seq}
			})
		},
	}
	c.Flags().StringVarP(&key, "key", "k", "", "Redis key (required)")
	c.Flags().Int64VarP(&delta, "delta", "d", 1, "increment by")
	c.MarkFlagRequired("key")
	return c
}

func sAddCmd(rf *rootFlags) *cobra.Command {
	var key, member string
	c := &cobra.Command{
		Use:   "sadd",
		Short: "SADD key member (LWW-Element-Set add)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runOp(rf, key, event.OpSADD, func(e *event.Event) { e.SAdd = &event.SAddVal{Member: member} })
		},
	}
	c.Flags().StringVarP(&key, "key", "k", "", "Redis key (required)")
	c.Flags().StringVarP(&member, "member", "m", "", "set member (required)")
	c.MarkFlagRequired("key")
	c.MarkFlagRequired("member")
	return c
}

func sRemCmd(rf *rootFlags) *cobra.Command {
	var key, member string
	c := &cobra.Command{
		Use:   "srem",
		Short: "SREM key member (LWW-Element-Set remove)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runOp(rf, key, event.OpSREM, func(e *event.Event) { e.SRem = &event.SRemVal{Member: member} })
		},
	}
	c.Flags().StringVarP(&key, "key", "k", "", "Redis key (required)")
	c.Flags().StringVarP(&member, "member", "m", "", "set member (required)")
	c.MarkFlagRequired("key")
	c.MarkFlagRequired("member")
	return c
}

func zAddCmd(rf *rootFlags) *cobra.Command {
	var key, member string
	var score float64
	c := &cobra.Command{
		Use:   "zadd",
		Short: "ZADD key member score (LWW per (key,member))",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runOp(rf, key, event.OpZADD, func(e *event.Event) {
				e.ZAdd = &event.ZAddVal{Member: member, Score: score}
			})
		},
	}
	c.Flags().StringVarP(&key, "key", "k", "", "Redis key (required)")
	c.Flags().StringVarP(&member, "member", "m", "", "ZSET member (required)")
	c.Flags().Float64VarP(&score, "score", "s", 0, "score")
	c.MarkFlagRequired("key")
	c.MarkFlagRequired("member")
	return c
}

func xAddCmd(rf *rootFlags) *cobra.Command {
	var key string
	var fields []string
	c := &cobra.Command{
		Use:   "xadd",
		Short: "XADD key field=value [field=value]... (HLC-id stream append)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			fm := map[string]string{}
			for _, kv := range fields {
				p := strings.SplitN(kv, "=", 2)
				if len(p) != 2 {
					return fmt.Errorf("bad --field %q (want k=v)", kv)
				}
				fm[p[0]] = p[1]
			}
			return runOp(rf, key, event.OpXADD, func(e *event.Event) {
				e.XAdd = &event.XAddVal{Fields: fm}
			})
		},
	}
	c.Flags().StringVarP(&key, "key", "k", "", "Redis key (required)")
	c.Flags().StringSliceVarP(&fields, "field", "f", nil, "stream field as key=value (repeat)")
	c.MarkFlagRequired("key")
	c.MarkFlagRequired("field")
	return c
}

// --- runtime -------------------------------------------------------------

func runOp(rf *rootFlags, key, op string, fill func(*event.Event)) error {
	region := rf.region
	if !validRegion(region) {
		return fmt.Errorf("invalid --region %q (want us|eu|ap)", region)
	}
	bootstrap := rf.bootstrap
	if bootstrap == "" {
		bootstrap = defaultBootstrap[region]
	}
	topic := region + ".events"
	subject := topic + "-value"

	sr := event.NewSRClient(rf.srURL)
	if err := sr.WaitReady(30 * time.Second); err != nil {
		return err
	}
	id, schemaJSON, err := sr.LatestForSubject(subject)
	if err != nil {
		return fmt.Errorf("schema lookup: %w", err)
	}
	codec, err := event.NewCodec(schemaJSON, id)
	if err != nil {
		return err
	}

	clk := hlc.New(region, nil)
	ts := clk.Tick()
	e := &event.Event{
		EventID:      ulid.Make().String(),
		OriginRegion: region,
		HLC:          ts,
		Key:          key,
		Op:           op,
	}
	fill(e)

	encoded, err := codec.Encode(e)
	if err != nil {
		return err
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.DefaultProduceTopic(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(0),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res := cl.ProduceSync(ctx, &kgo.Record{
		Key:   e.PartitionKey(),
		Value: encoded,
		Topic: topic,
	})
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	r := res[0].Record
	log.Printf("ok topic=%s partition=%d offset=%d %s", r.Topic, r.Partition, r.Offset, e)
	return nil
}

func validRegion(r string) bool {
	_, ok := defaultBootstrap[r]
	return ok
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
