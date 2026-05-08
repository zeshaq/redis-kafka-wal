// bridge is an HTTP service that exposes the lab's live state to a web UI.
//
//   GET  /api/config                          public lab config (publicToken if set)
//   GET  /api/health                          liveness
//   GET  /api/regions/:region/state           snapshot of demo keys
//   GET  /api/regions/:region/key/:key?type=<type>   single-key inspector
//   GET  /api/events                          SSE: every event observed on every region's Kafka
//   GET  /api/event-state?region=:r           live-event app state aggregator
//   POST /api/produce                         produce an op (Bearer token: write OR public)
//   GET  /api/sim                             traffic simulator status
//   POST /api/sim/start                       start simulator (admin Bearer token; body: {rate})
//   POST /api/sim/stop                        stop simulator (admin Bearer token)
//   POST /api/sim/rate                        change rate while running (admin Bearer token; body: {rate})
//
// The bridge runs three independent Kafka consumers, one per region's local
// cluster. Each event flows through the broadcaster annotated with which
// region's Kafka it was observed on, so the UI can show propagation
// visually (the same event appears at us-kafka first, then eu-kafka after
// MM2 lag, then ap-kafka).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ze/redis-kafka-wal-lab/pkg/event"
	"github.com/ze/redis-kafka-wal-lab/pkg/hlc"
)

type config struct {
	listen      string
	srURL       string
	writeToken  string // admin: required for sim control, accepted for produce
	publicToken string // public lab token: accepted for produce only; exposed via /api/config
	allowOrigin string
	regions     map[string]regionCfg
}

type regionCfg struct {
	bootstrap string
	redisAddr string
}

func loadConfig() config {
	return config{
		listen:      envOr("LISTEN", ":8080"),
		srURL:       envOr("SR_URL", "http://schema-registry:8081"),
		writeToken:  os.Getenv("BRIDGE_WRITE_TOKEN"),
		publicToken: os.Getenv("BRIDGE_PUBLIC_TOKEN"),
		allowOrigin: envOr("ALLOW_ORIGIN", "*"),
		regions: map[string]regionCfg{
			"us": {bootstrap: envOr("KAFKA_US", "kafka-us:29092"), redisAddr: envOr("REDIS_US", "redis-us:6379")},
			"eu": {bootstrap: envOr("KAFKA_EU", "kafka-eu:29092"), redisAddr: envOr("REDIS_EU", "redis-eu:6379")},
			"ap": {bootstrap: envOr("KAFKA_AP", "kafka-ap:29092"), redisAddr: envOr("REDIS_AP", "redis-ap:6379")},
		},
	}
}

func main() {
	cfg := loadConfig()
	if cfg.writeToken == "" {
		log.Printf("warning: BRIDGE_WRITE_TOKEN unset — write endpoint will reject all requests")
	}

	ctx, cancel := signalContext()
	defer cancel()

	sr := event.NewSRClient(cfg.srURL)
	if err := sr.WaitReady(60 * time.Second); err != nil {
		log.Fatalf("schema registry: %v", err)
	}
	id, schemaJSON, err := sr.LatestForSubject("us.events-value")
	if err != nil {
		log.Fatalf("schema lookup: %v", err)
	}
	codec, err := event.NewCodec(schemaJSON, id)
	if err != nil {
		log.Fatalf("codec: %v", err)
	}

	rdb := map[string]*redis.Client{}
	for name, rc := range cfg.regions {
		rdb[name] = redis.NewClient(&redis.Options{Addr: rc.redisAddr})
	}
	defer func() {
		for _, c := range rdb {
			_ = c.Close()
		}
	}()

	// One producer client per region for /api/produce.
	producers := map[string]*kgo.Client{}
	for name, rc := range cfg.regions {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(rc.bootstrap),
			kgo.AllowAutoTopicCreation(),
			kgo.ProducerLinger(0),
		)
		if err != nil {
			log.Fatalf("kafka producer %s: %v", name, err)
		}
		producers[name] = cl
	}
	defer func() {
		for _, p := range producers {
			p.Close()
		}
	}()

	clocks := map[string]*hlc.Clock{
		"us": hlc.New("us", nil),
		"eu": hlc.New("eu", nil),
		"ap": hlc.New("ap", nil),
	}

	bc := newBroadcaster()

	// Spawn one consumer per region's local Kafka. Each tags events with
	// the region whose Kafka observed them, so the UI can render
	// MM2 propagation across all three.
	for name, rc := range cfg.regions {
		go runConsumer(ctx, name, rc.bootstrap, codec, bc)
	}

	srv := newServer(ctx, cfg, codec, clocks, producers, rdb, bc)
	httpSrv := &http.Server{Addr: cfg.listen, Handler: srv}

	go func() {
		log.Printf("bridge listening on %s (CORS allow-origin=%q)", cfg.listen, cfg.allowOrigin)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	_ = httpSrv.Shutdown(shutdownCtx)
}

// --- Kafka consumer -----------------------------------------------------

func runConsumer(ctx context.Context, observedAt, bootstrap string, codec *event.Codec, bc *broadcaster) {
	groupID := "bridge-" + observedAt + "-" + ulid.Make().String()[:8]
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics("us.events", "eu.events", "ap.events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(15*time.Second),
	)
	if err != nil {
		log.Printf("[%s] kafka client: %v", observedAt, err)
		return
	}
	defer cl.Close()
	log.Printf("[%s] consuming via group %s", observedAt, groupID)

	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) {
					return
				}
				log.Printf("[%s] fetch error: %v", observedAt, fe.Err)
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			ev, err := codec.Decode(rec.Value)
			if err != nil {
				log.Printf("[%s] decode: %v", observedAt, err)
				return
			}
			bc.publish(observedEvent{
				Event:      ev,
				ObservedAt: observedAt,
				Topic:      rec.Topic,
				Partition:  rec.Partition,
				Offset:     rec.Offset,
				SeenAt:     time.Now().UTC(),
			})
		})
	}
}

// --- Broadcaster: fan out events to all SSE subscribers -----------------

type observedEvent struct {
	Event      *event.Event
	ObservedAt string
	Topic      string
	Partition  int32
	Offset     int64
	SeenAt     time.Time
}

type broadcaster struct {
	mu      sync.RWMutex
	clients map[chan observedEvent]struct{}
	count   atomic.Int64
}

func newBroadcaster() *broadcaster {
	return &broadcaster{clients: make(map[chan observedEvent]struct{})}
}

func (b *broadcaster) subscribe() (<-chan observedEvent, func()) {
	ch := make(chan observedEvent, 256)
	b.mu.Lock()
	b.clients[ch] = struct{}{}
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		if _, ok := b.clients[ch]; ok {
			delete(b.clients, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
	return ch, cancel
}

func (b *broadcaster) publish(e observedEvent) {
	b.count.Add(1)
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.clients {
		select {
		case ch <- e:
		default:
			// drop on slow consumer rather than block the whole pipeline
		}
	}
}

// --- HTTP server --------------------------------------------------------

type server struct {
	ctx       context.Context // parent ctx for long-running spawned goroutines (e.g. simulator)
	cfg       config
	codec     *event.Codec
	clocks    map[string]*hlc.Clock
	producers map[string]*kgo.Client
	rdb       map[string]*redis.Client
	bc        *broadcaster
	sim       *simManager
}

func newServer(ctx context.Context, cfg config, codec *event.Codec, clocks map[string]*hlc.Clock, producers map[string]*kgo.Client, rdb map[string]*redis.Client, bc *broadcaster) *server {
	return &server{ctx: ctx, cfg: cfg, codec: codec, clocks: clocks, producers: producers, rdb: rdb, bc: bc, sim: &simManager{}}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.cors(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch {
	case r.URL.Path == "/api/config":
		s.config(w, r)
	case r.URL.Path == "/api/health":
		s.health(w, r)
	case r.URL.Path == "/api/events":
		s.events(w, r)
	case r.URL.Path == "/api/produce" && r.Method == http.MethodPost:
		s.produce(w, r)
	case r.URL.Path == "/api/event-state":
		s.eventState(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/sim"):
		s.simRoute(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/regions/"):
		s.regionRoute(w, r)
	default:
		http.NotFound(w, r)
	}
}

// config exposes the public lab token (if BRIDGE_PUBLIC_TOKEN is set) so
// the live-event page can write reactions/votes without an admin paste.
// Anyone who can reach the bridge can fetch this token; for production,
// gate /live behind Cloudflare Access and don't set BRIDGE_PUBLIC_TOKEN.
func (s *server) config(w http.ResponseWriter, _ *http.Request) {
	out := map[string]any{
		"regions": []string{"us", "eu", "ap"},
	}
	if s.cfg.publicToken != "" {
		out["publicToken"] = s.cfg.publicToken
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) cors(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", s.cfg.allowOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
	w.Header().Set("Access-Control-Max-Age", "600")
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":             true,
		"events_total":   s.bc.count.Load(),
		"regions_known":  []string{"us", "eu", "ap"},
		"timestamp":      time.Now().UTC(),
	})
}

// /api/regions/:region/state                  snapshot of demo keys
// /api/regions/:region/key/:key?type=string   inspect one key
func (s *server) regionRoute(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/regions/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	region := parts[0]
	rdb, ok := s.rdb[region]
	if !ok {
		http.Error(w, "unknown region", http.StatusNotFound)
		return
	}
	if len(parts) == 2 && parts[1] == "state" {
		s.regionState(w, r, region, rdb)
		return
	}
	if len(parts) == 3 && parts[1] == "key" {
		s.regionKey(w, r, region, rdb, parts[2])
		return
	}
	http.NotFound(w, r)
}

// regionState: a snapshot of the lab's "live" keys, themed around a
// global flash-sale scenario so the dashboard tells a story:
//
//   inventory:laptop      LWW string   stock count for the headline SKU
//   sales:total           G-Counter    total orders across all regions
//   cart:active           Set          users with an active cart right now
//   leaderboard:spenders  ZSET         top spenders this sale
//   orders:feed           Stream       per-order events
//
// The fixed key list keeps the dashboard simple; the /api/regions/:r/key/:k
// endpoint exists for poking at any other key.
func (s *server) regionState(w http.ResponseWriter, r *http.Request, region string, rdb *redis.Client) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	out := map[string]any{
		"region":    region,
		"timestamp": time.Now().UTC(),
		"dbsize":    ignoreErr(rdb.DBSize(ctx).Result()),
	}

	// LWW string — current inventory of the headline SKU
	val, _ := rdb.Get(ctx, "inventory:laptop").Result()
	meta, _ := rdb.HGetAll(ctx, "inventory:laptop:meta").Result()
	out["inventory:laptop"] = map[string]any{"value": val, "meta": meta}

	// G-Counter — total sales
	cnt, _ := rdb.Get(ctx, "sales:total").Result()
	seqs := map[string]string{}
	for _, reg := range []string{"us", "eu", "ap"} {
		v, _ := rdb.Get(ctx, "sales:total:seq:"+reg).Result()
		seqs[reg] = v
	}
	out["sales:total"] = map[string]any{"value": cnt, "seqs": seqs}

	// Set — currently active carts
	mems, _ := rdb.SMembers(ctx, "cart:active").Result()
	out["cart:active"] = map[string]any{"members": mems}

	// ZSET — leaderboard of top spenders
	zs, _ := rdb.ZRevRangeWithScores(ctx, "leaderboard:spenders", 0, 9).Result()
	zlist := make([]map[string]any, 0, len(zs))
	for _, z := range zs {
		zlist = append(zlist, map[string]any{"member": z.Member, "score": z.Score})
	}
	out["leaderboard:spenders"] = map[string]any{"entries": zlist}

	// Stream — order events
	xlen, _ := rdb.XLen(ctx, "orders:feed").Result()
	xs, _ := rdb.XRevRangeN(ctx, "orders:feed", "+", "-", 5).Result()
	out["orders:feed"] = map[string]any{"xlen": xlen, "recent": xs}

	writeJSON(w, http.StatusOK, out)
}

// regionKey: generic single-key inspector. type=string|hash|set|zset|stream|auto.
func (s *server) regionKey(w http.ResponseWriter, r *http.Request, region string, rdb *redis.Client, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	t := r.URL.Query().Get("type")
	if t == "" || t == "auto" {
		t, _ = rdb.Type(ctx, key).Result()
	}
	out := map[string]any{"region": region, "key": key, "type": t}
	switch t {
	case "string":
		v, err := rdb.Get(ctx, key).Result()
		out["value"] = v
		if err == redis.Nil {
			out["exists"] = false
		} else {
			out["exists"] = err == nil
		}
	case "hash":
		out["value"], _ = rdb.HGetAll(ctx, key).Result()
	case "set":
		out["value"], _ = rdb.SMembers(ctx, key).Result()
	case "zset":
		zs, _ := rdb.ZRevRangeWithScores(ctx, key, 0, -1).Result()
		zlist := make([]map[string]any, 0, len(zs))
		for _, z := range zs {
			zlist = append(zlist, map[string]any{"member": z.Member, "score": z.Score})
		}
		out["value"] = zlist
	case "stream":
		xs, _ := rdb.XRange(ctx, key, "-", "+").Result()
		out["value"] = xs
		out["xlen"], _ = rdb.XLen(ctx, key).Result()
	case "none", "":
		out["exists"] = false
	default:
		out["error"] = "unsupported type " + t
	}
	writeJSON(w, http.StatusOK, out)
}

// /api/events: SSE stream. Browser uses EventSource.
func (s *server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // nginx & friends
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch, cancel := s.bc.subscribe()
	defer cancel()

	// Heartbeat every 15s so proxies don't kill the connection.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case ev, ok := <-ch:
			if !ok {
				return
			}
			line := map[string]any{
				"event_id":      ev.Event.EventID,
				"origin_region": ev.Event.OriginRegion,
				"observed_at":   ev.ObservedAt,
				"topic":         ev.Topic,
				"partition":     ev.Partition,
				"offset":        ev.Offset,
				"key":           ev.Event.Key,
				"op":            ev.Event.Op,
				"hlc": map[string]any{
					"phys":    ev.Event.HLC.PhysicalMs,
					"logical": ev.Event.HLC.Logical,
					"region":  ev.Event.HLC.Region,
				},
				"payload":   payloadSummary(ev.Event),
				"seen_at":   ev.SeenAt,
			}
			b, _ := json.Marshal(line)
			fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func payloadSummary(e *event.Event) any {
	switch {
	case e.Set != nil:
		v := map[string]any{"value": string(e.Set.Value)}
		if e.Set.TTLMs != nil {
			v["ttl_ms"] = *e.Set.TTLMs
		}
		return v
	case e.Del != nil:
		return map[string]any{}
	case e.Incr != nil:
		return map[string]any{"delta": e.Incr.Delta, "seq": e.Incr.Seq}
	case e.SAdd != nil:
		return map[string]any{"member": e.SAdd.Member}
	case e.SRem != nil:
		return map[string]any{"member": e.SRem.Member}
	case e.ZAdd != nil:
		return map[string]any{"member": e.ZAdd.Member, "score": e.ZAdd.Score}
	case e.XAdd != nil:
		return map[string]any{"fields": e.XAdd.Fields}
	}
	return nil
}

// produceReq is the JSON body shape for /api/produce and the in-process
// type the simulator uses to talk to doProduce.
type produceReq struct {
	Region string            `json:"region"`
	Op     string            `json:"op"`
	Key    string            `json:"key"`
	Value  string            `json:"value,omitempty"`
	TTLMs  int64             `json:"ttl_ms,omitempty"`
	Delta  int64             `json:"delta,omitempty"`
	Member string            `json:"member,omitempty"`
	Score  float64           `json:"score,omitempty"`
	Fields map[string]string `json:"fields,omitempty"`
}

type produceRes struct {
	EventID   string  `json:"event_id"`
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Offset    int64   `json:"offset"`
	HLC       hlcView `json:"hlc"`
}

type hlcView struct {
	Phys    int64  `json:"phys"`
	Logical int32  `json:"logical"`
	Region  string `json:"region"`
}

// doProduce builds the event, encodes it, and produces to Kafka. Shared
// between the HTTP handler and the in-bridge traffic simulator.
func (s *server) doProduce(ctx context.Context, body produceReq) (*produceRes, error) {
	clock, ok := s.clocks[body.Region]
	if !ok {
		return nil, fmt.Errorf("unknown region %q", body.Region)
	}
	prod, ok := s.producers[body.Region]
	if !ok {
		return nil, fmt.Errorf("no producer for region %q", body.Region)
	}
	if body.Key == "" {
		return nil, errors.New("key required")
	}

	ts := clock.Tick()
	e := &event.Event{
		EventID:      ulid.Make().String(),
		OriginRegion: body.Region,
		HLC:          ts,
		Key:          body.Key,
		Op:           body.Op,
	}
	switch body.Op {
	case event.OpSET:
		sv := &event.SetVal{Value: []byte(body.Value)}
		if body.TTLMs > 0 {
			t := body.TTLMs
			sv.TTLMs = &t
		}
		e.Set = sv
	case event.OpDEL:
		e.Del = &event.DelVal{}
	case event.OpINCR:
		seq := ts.PhysicalMs*1_000_000 + int64(ts.Logical)
		delta := body.Delta
		if delta == 0 {
			delta = 1
		}
		e.Incr = &event.IncrVal{Delta: delta, Seq: seq}
	case event.OpSADD:
		if body.Member == "" {
			return nil, errors.New("member required")
		}
		e.SAdd = &event.SAddVal{Member: body.Member}
	case event.OpSREM:
		if body.Member == "" {
			return nil, errors.New("member required")
		}
		e.SRem = &event.SRemVal{Member: body.Member}
	case event.OpZADD:
		if body.Member == "" {
			return nil, errors.New("member required")
		}
		e.ZAdd = &event.ZAddVal{Member: body.Member, Score: body.Score}
	case event.OpXADD:
		if len(body.Fields) == 0 {
			return nil, errors.New("fields required")
		}
		e.XAdd = &event.XAddVal{Fields: body.Fields}
	default:
		return nil, fmt.Errorf("unknown op %q", body.Op)
	}

	encoded, err := s.codec.Encode(e)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	topic := body.Region + ".events"
	r := prod.ProduceSync(ctx, &kgo.Record{
		Key:   e.PartitionKey(),
		Value: encoded,
		Topic: topic,
	})
	if err := r.FirstErr(); err != nil {
		return nil, fmt.Errorf("produce: %w", err)
	}
	rec := r[0].Record
	return &produceRes{
		EventID:   e.EventID,
		Topic:     rec.Topic,
		Partition: rec.Partition,
		Offset:    rec.Offset,
		HLC:       hlcView{Phys: e.HLC.PhysicalMs, Logical: e.HLC.Logical, Region: e.HLC.Region},
	}, nil
}

// /api/produce: takes a JSON body describing a write op.
func (s *server) produce(w http.ResponseWriter, r *http.Request) {
	if !s.authorizedWrite(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var body produceReq
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad body: "+err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	res, err := s.doProduce(ctx, body)
	if err != nil {
		// Lab-grade error mapping: produce failures are upstream issues,
		// everything else looks like client error from the request body.
		status := http.StatusBadRequest
		if strings.HasPrefix(err.Error(), "produce:") || strings.HasPrefix(err.Error(), "encode:") {
			status = http.StatusBadGateway
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":        true,
		"event_id":  res.EventID,
		"topic":     res.Topic,
		"partition": res.Partition,
		"offset":    res.Offset,
		"hlc":       res.HLC,
	})
}

// authorizedAdmin returns true only for the admin write token. Used to
// gate destructive / lab-control actions like the simulator endpoints.
func (s *server) authorizedAdmin(r *http.Request) bool {
	return s.checkToken(r, s.cfg.writeToken)
}

// authorizedWrite returns true for either the admin write token OR the
// public lab token. Used to gate /api/produce so the public live-event
// page can submit reactions/votes without an admin token paste.
func (s *server) authorizedWrite(r *http.Request) bool {
	if s.checkToken(r, s.cfg.writeToken) {
		return true
	}
	if s.cfg.publicToken != "" && s.checkToken(r, s.cfg.publicToken) {
		return true
	}
	return false
}

func (s *server) checkToken(r *http.Request, expected string) bool {
	if expected == "" {
		return false
	}
	auth := r.Header.Get("Authorization")
	const p = "Bearer "
	if !strings.HasPrefix(auth, p) {
		return false
	}
	return subtleEq(auth[len(p):], expected)
}

// /api/sim                 GET  status
// /api/sim/start           POST {rate} -> start
// /api/sim/stop            POST -> stop
// /api/sim/rate            POST {rate} -> change rate while running
func (s *server) simRoute(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/sim":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, s.sim.state())
	case "/api/sim/start":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authorizedAdmin(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var body struct {
			Rate int `json:"rate"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Rate < 1 || body.Rate > 50 {
			http.Error(w, "rate must be 1..50", http.StatusBadRequest)
			return
		}
		if !s.sim.start(s.ctx, body.Rate, s.runSim) {
			http.Error(w, "already running", http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, s.sim.state())
	case "/api/sim/stop":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authorizedAdmin(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if !s.sim.stop() {
			http.Error(w, "not running", http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, s.sim.state())
	case "/api/sim/rate":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authorizedAdmin(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var body struct {
			Rate int `json:"rate"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Rate < 1 || body.Rate > 50 {
			http.Error(w, "rate must be 1..50", http.StatusBadRequest)
			return
		}
		if !s.sim.updateRate(body.Rate) {
			http.Error(w, "not running", http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, s.sim.state())
	default:
		http.NotFound(w, r)
	}
}

// --- helpers ------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func ignoreErr[T any](v T, _ error) T { return v }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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

// constant-time string compare for the token check
func subtleEq(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := 0; i < len(a); i++ {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
