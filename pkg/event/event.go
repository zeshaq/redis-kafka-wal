// Package event holds the Avro schema bindings and op-specific payloads.
//
// Wire format is Confluent's:
//
//	[0x00][4-byte big-endian schema ID][Avro-binary payload]
//
// We keep Go structs typed and ergonomic; the union<SetVal,DelVal,...>
// is converted to and from hamba's map representation in (Event).toAvro
// and fromAvro. This keeps the rest of the codebase free of map juggling.
package event

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"

	"github.com/ze/redis-kafka-wal-lab/pkg/hlc"
)

const namespace = "lab.geo"

// Op codes mirror the Avro enum.
const (
	OpSET  = "SET"
	OpDEL  = "DEL"
	OpINCR = "INCR"
	OpSADD = "SADD"
	OpSREM = "SREM"
	OpZADD = "ZADD"
	OpXADD = "XADD"
)

type Event struct {
	EventID      string
	OriginRegion string
	HLC          hlc.Timestamp
	Key          string
	Op           string

	// Exactly one of these is non-nil, matching Op.
	Set  *SetVal
	Del  *DelVal
	Incr *IncrVal
	SAdd *SAddVal
	SRem *SRemVal
	ZAdd *ZAddVal
	XAdd *XAddVal
}

type SetVal struct {
	Value []byte
	TTLMs *int64 // nil = no TTL
}

type DelVal struct{}

type IncrVal struct {
	Delta int64
	// Seq is monotonic per (origin_region, key). The materializer dedups
	// commutative INCRs by tracking max(Seq) per (origin, key).
	Seq int64
}

type SAddVal struct{ Member string }
type SRemVal struct{ Member string }
type ZAddVal struct {
	Member string
	Score  float64
}
type XAddVal struct {
	Fields map[string]string
}

// Codec wraps a parsed Avro schema and a Schema Registry client. One
// Codec is shared by producer and consumer.
type Codec struct {
	schema   avro.Schema
	schemaID uint32
}

// NewCodec parses the schema text and stores the SR-assigned id.
func NewCodec(schemaJSON string, schemaID uint32) (*Codec, error) {
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("parse avro: %w", err)
	}
	return &Codec{schema: s, schemaID: schemaID}, nil
}

func (c *Codec) Schema() avro.Schema { return c.schema }
func (c *Codec) SchemaID() uint32    { return c.schemaID }

// Encode produces a Confluent-framed Avro message.
func (c *Codec) Encode(e *Event) ([]byte, error) {
	m, err := e.toAvro()
	if err != nil {
		return nil, err
	}
	body, err := avro.Marshal(c.schema, m)
	if err != nil {
		return nil, fmt.Errorf("avro marshal: %w", err)
	}
	out := make([]byte, 5+len(body))
	out[0] = 0x00
	binary.BigEndian.PutUint32(out[1:5], c.schemaID)
	copy(out[5:], body)
	return out, nil
}

// Decode parses a Confluent-framed Avro message. The schema id in the
// header must match the codec's; in production you'd resolve via SR
// lookup, but for the lab there's a single registered schema.
func (c *Codec) Decode(b []byte) (*Event, error) {
	if len(b) < 5 || b[0] != 0x00 {
		return nil, errors.New("not a confluent-framed avro record")
	}
	id := binary.BigEndian.Uint32(b[1:5])
	if id != c.schemaID {
		return nil, fmt.Errorf("schema id mismatch: got %d want %d", id, c.schemaID)
	}
	var m map[string]any
	if err := avro.Unmarshal(c.schema, b[5:], &m); err != nil {
		return nil, fmt.Errorf("avro unmarshal: %w", err)
	}
	return fromAvro(m)
}

// --- conversion ----------------------------------------------------------

func (e *Event) toAvro() (map[string]any, error) {
	out := map[string]any{
		"event_id":      e.EventID,
		"origin_region": e.OriginRegion,
		"hlc": map[string]any{
			"physical_ms": e.HLC.PhysicalMs,
			"logical":     e.HLC.Logical,
			"region":      e.HLC.Region,
		},
		"key": e.Key,
		"op":  e.Op,
	}

	// Union: hamba expects { "<full-name>": <record-map> } or nil for null.
	switch {
	case e.Set != nil:
		v := map[string]any{"value": e.Set.Value}
		if e.Set.TTLMs != nil {
			v["ttl_ms"] = map[string]any{"long": *e.Set.TTLMs}
		} else {
			v["ttl_ms"] = nil
		}
		out["payload"] = map[string]any{namespace + ".SetVal": v}
	case e.Del != nil:
		out["payload"] = map[string]any{namespace + ".DelVal": map[string]any{}}
	case e.Incr != nil:
		out["payload"] = map[string]any{namespace + ".IncrVal": map[string]any{
			"delta": e.Incr.Delta,
			"seq":   e.Incr.Seq,
		}}
	case e.SAdd != nil:
		out["payload"] = map[string]any{namespace + ".SAddVal": map[string]any{
			"member": e.SAdd.Member,
		}}
	case e.SRem != nil:
		out["payload"] = map[string]any{namespace + ".SRemVal": map[string]any{
			"member": e.SRem.Member,
		}}
	case e.ZAdd != nil:
		out["payload"] = map[string]any{namespace + ".ZAddVal": map[string]any{
			"member": e.ZAdd.Member,
			"score":  e.ZAdd.Score,
		}}
	case e.XAdd != nil:
		fields := make(map[string]any, len(e.XAdd.Fields))
		for k, v := range e.XAdd.Fields {
			fields[k] = v
		}
		out["payload"] = map[string]any{namespace + ".XAddVal": map[string]any{
			"fields": fields,
		}}
	default:
		out["payload"] = nil
	}
	return out, nil
}

func fromAvro(m map[string]any) (*Event, error) {
	getStr := func(k string) (string, error) {
		v, ok := m[k]
		if !ok {
			return "", fmt.Errorf("missing %q", k)
		}
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("%q not string: %T", k, v)
		}
		return s, nil
	}

	id, err := getStr("event_id")
	if err != nil {
		return nil, err
	}
	region, err := getStr("origin_region")
	if err != nil {
		return nil, err
	}
	key, err := getStr("key")
	if err != nil {
		return nil, err
	}
	op, err := getStr("op")
	if err != nil {
		return nil, err
	}

	hlcM, ok := m["hlc"].(map[string]any)
	if !ok {
		return nil, errors.New("hlc not record")
	}
	pms, _ := hlcM["physical_ms"].(int64)
	logical, _ := hlcM["logical"].(int32)
	hlcR, _ := hlcM["region"].(string)

	e := &Event{
		EventID:      id,
		OriginRegion: region,
		HLC:          hlc.Timestamp{PhysicalMs: pms, Logical: logical, Region: hlcR},
		Key:          key,
		Op:           op,
	}

	if m["payload"] == nil {
		return e, nil
	}
	pm, ok := m["payload"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("payload not map: %T", m["payload"])
	}
	for tname, raw := range pm {
		body, _ := raw.(map[string]any)
		switch tname {
		case namespace + ".SetVal":
			sv := &SetVal{}
			if v, ok := body["value"].([]byte); ok {
				sv.Value = v
			}
			if u, ok := body["ttl_ms"].(map[string]any); ok {
				if l, ok := u["long"].(int64); ok {
					sv.TTLMs = &l
				}
			}
			e.Set = sv
		case namespace + ".DelVal":
			e.Del = &DelVal{}
		case namespace + ".IncrVal":
			delta, _ := body["delta"].(int64)
			seq, _ := body["seq"].(int64)
			e.Incr = &IncrVal{Delta: delta, Seq: seq}
		case namespace + ".SAddVal":
			mem, _ := body["member"].(string)
			e.SAdd = &SAddVal{Member: mem}
		case namespace + ".SRemVal":
			mem, _ := body["member"].(string)
			e.SRem = &SRemVal{Member: mem}
		case namespace + ".ZAddVal":
			mem, _ := body["member"].(string)
			sc, _ := body["score"].(float64)
			e.ZAdd = &ZAddVal{Member: mem, Score: sc}
		case namespace + ".XAddVal":
			fm, _ := body["fields"].(map[string]any)
			f := make(map[string]string, len(fm))
			for k, v := range fm {
				if s, ok := v.(string); ok {
					f[k] = s
				}
			}
			e.XAdd = &XAddVal{Fields: f}
		default:
			return nil, fmt.Errorf("unknown payload type %q", tname)
		}
	}
	return e, nil
}

// EncodeKey returns the bytes used for Kafka partitioning. We partition by
// Redis key so per-key ordering is preserved within an origin's topic.
func (e *Event) PartitionKey() []byte { return []byte(e.Key) }

// Compact debug string used in logs.
func (e *Event) String() string {
	var b bytes.Buffer
	fmt.Fprintf(&b, "%s %s key=%q hlc=%d.%d@%s id=%s",
		e.OriginRegion, e.Op, e.Key, e.HLC.PhysicalMs, e.HLC.Logical, e.HLC.Region, e.EventID)
	return b.String()
}
