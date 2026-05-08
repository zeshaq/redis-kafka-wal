# ADR-0006: Origin region encoded in the topic name

- **Status:** Accepted
- **Date:** 2026-05-08
- **Supports:** ADR-0002

## Context

Active-active replication has a real failure mode called the **mirror
loop**: cluster A mirrors topic T to B, B mirrors T to C, C mirrors T
back to A. If the system can't tell A's copy of T apart from C's
mirror of B's mirror of A's, the loop runs forever and amplifies.

MirrorMaker 2's default `DefaultReplicationPolicy` solves this by
prefixing mirrored topics with the source cluster alias
(`us` becomes `us.<topic>` when seen from EU). That works but it
forces consumers to subscribe to the cross-product of every cluster
alias and every topic name.

## Decision

- Use **`IdentityReplicationPolicy`** so topic names are preserved
  across clusters.
- Use **three origin-region topics**: `us.events`, `eu.events`,
  `ap.events`. Each topic is **only ever produced into by clients in
  its owning region**.
- Configure each MM2 directional flow to mirror **only the source
  region's own topic**: `us->eu.topics = us\.events` (etc.).

Origin region is therefore knowable from two places that always agree:
the topic name, and the `origin_region` field on the event payload.

## Alternatives considered

- **DefaultReplicationPolicy with prefixed topic names** — works for
  loop prevention but consumers see `us.events`, `us.us.events`
  (mirror of US's mirror), etc. Subscription patterns get hairy.
  Rejected.
- **One global `events` topic, origin in payload** — needs explicit
  filtering on every flow to prevent loops. The filter would be on a
  payload field, which MM2 doesn't natively support; we'd need a
  Single Message Transform (SMT). Brittle. Rejected.
- **Topic-per-region-pair** (e.g. `us-to-eu.events`) — multiplies
  topic count for no extra clarity. Rejected.

## Consequences

**Positive**

- Mirror loops are **structurally impossible**: each MM2 connector's
  topic filter only matches the source's own origin topic.
- Subscription pattern is uniform: every consumer subscribes to
  `us.events`, `eu.events`, `ap.events` regardless of which cluster
  it's reading from.
- Origin is recoverable from the topic name in tooling
  (`kafka-console-consumer --topic eu.events` is unambiguous).

**Negative**

- Producers are required to know their region to pick the right
  topic. We enforce this in the producer CLI; misuse would write
  events to the wrong origin's topic and break the loop-prevention
  invariant.
- Adding a new region means adding a new topic everywhere, plus six
  new MM2 directional flows (existing 6 + 6 new for the new region's
  pairs). Operationally a real change, not a config tweak.

**Obligations**

- Producers must always produce to `<own_region>.events`. Code review
  any change that touches the producer's topic-selection logic.
- New MM2 flows added for a new region must include
  `replication.policy.class = IdentityReplicationPolicy` and a
  `topics` filter that only matches the source's own origin topic.
