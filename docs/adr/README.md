# Architecture Decision Records

Each ADR captures a load-bearing design decision: the context, what we
decided, what we considered, and what the decision implies going forward.

When you change a load-bearing piece of the architecture, **add a new
ADR**. Don't edit accepted ones except to mark them Superseded — the
historical record of what we used to think is itself useful.

## Index

| # | Title | Status |
| --- | --- | --- |
| [0001](0001-kafka-as-source-of-truth.md) | Kafka as the source of truth, Redis as a materialized view | Accepted |
| [0002](0002-per-region-kafka-with-mm2.md) | Per-region Kafka clusters with MirrorMaker 2 | Accepted |
| [0003](0003-active-active-with-crdt-style-resolution.md) | Active-active with CRDT-style conflict resolution | Accepted |
| [0004](0004-avro-with-single-schema-registry.md) | Avro on the wire, single Schema Registry (lab) | Accepted (lab); production migration required |
| [0005](0005-hybrid-logical-clocks.md) | Hybrid Logical Clocks for ordering | Accepted |
| [0006](0006-origin-region-in-topic-name.md) | Origin region encoded in the topic name | Accepted |
| [0007](0007-lua-scripts-for-atomic-apply.md) | Lua scripts for atomic conflict-resolving apply | Accepted |
| [0008](0008-hlc-deterministic-stream-ids.md) | HLC-deterministic stream IDs for XADD | Accepted with caveat |
| [0009](0009-pure-go-stack.md) | Pure-Go Kafka client stack (franz-go) | Accepted |
| [0010](0010-lww-element-set-not-or-set.md) | LWW-Element-Set for SADD/SREM (deferring true OR-Set) | Accepted with limitation |

## Authoring

1. Copy [`0000-template.md`](0000-template.md) to the next available
   number.
2. Fill in Context (forces and constraints), Decision (one declarative
   sentence then expand), Alternatives considered, Consequences.
3. Update the index above.
4. If your decision affects what `ARCHITECTURE.md` describes, link the
   ADR from there too.

## Status values

- **Proposed** — drafted, not yet decided.
- **Accepted** — current.
- **Accepted with caveat / limitation** — known sharp edges documented
  in the ADR itself.
- **Deprecated** — still describes how things work today but we don't
  want to keep doing this.
- **Superseded by ADR-NNNN** — a later ADR replaces this one. Do not
  delete the old ADR; future readers need to understand the path.
