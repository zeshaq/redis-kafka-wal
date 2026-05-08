# ADR-0009: Pure-Go Kafka client stack (franz-go)

- **Status:** Accepted
- **Date:** 2026-05-08

## Context

Two practical choices for a Go Kafka client:

1. **`confluent-kafka-go`** — wraps `librdkafka` (C). Most feature-complete,
   matches the C client's semantics exactly. Requires CGo, complicates
   builds (especially for Alpine + small images), needs `librdkafka` at
   runtime.
2. **`twmb/franz-go`** — pure-Go, modern Kafka protocol implementation,
   actively maintained, smaller community than confluent's.

We also need an Avro codec. Two pure-Go choices: `linkedin/goavro` and
`hamba/avro/v2`. Hamba has cleaner generics-friendly APIs and decent
union support.

## Decision

- **Kafka:** `github.com/twmb/franz-go` + `kgo` package.
- **Avro:** `github.com/hamba/avro/v2`.
- **Schema Registry:** thin custom HTTP client in `pkg/event/sr.go`
  (we only need `latest-by-subject`, `by-id`, and `register`).
- **Redis:** `github.com/redis/go-redis/v9` (de-facto standard).
- **CLI:** `github.com/spf13/cobra`.
- **ULIDs:** `github.com/oklog/ulid/v2`.

Result: a CGo-free build that produces a static binary in a ~10MB
Alpine image.

## Alternatives considered

- **`confluent-kafka-go`** — best feature parity with the C ecosystem,
  but CGo complicates the multi-stage Docker build, and the Alpine
  base needs `librdkafka-dev` at build time. The franz-go feature set
  is more than enough for this lab. Rejected.
- **`segmentio/kafka-go`** — pure-Go but older API design, no
  consumer-group rebalancing UX as polished as franz-go's. Rejected.
- **A typed Avro generator** (e.g., `github.com/heetch/avro`) — would
  produce Go structs from the schema. Cleaner than hamba's
  `map[string]any` for the union, but adds a build step. Reconsider
  if the schema grows.

## Consequences

**Positive**

- `docker compose build` finishes faster, the runtime image has no
  C library dependencies, cross-compilation is trivial.
- Reproducibility: `go mod tidy` is the only build-time network
  dependency.

**Negative**

- Hamba's union representation (`map[string]any` keyed by full type
  name) is awkward in code (`pkg/event/event.go`). A future
  contributor may struggle until they read the codec helpers.
- franz-go's API differs from confluent-kafka-go; teams used to the
  latter need a small adjustment.

**Obligations**

- Don't introduce a CGo dependency without bumping this ADR. Adding
  one would change the Docker build and image size meaningfully.
