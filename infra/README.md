# Infrastructure

Three deployment targets, in increasing order of realism:

| Target | Where | When to use |
| --- | --- | --- |
| **docker-compose** | repo root: [`docker-compose.yml`](../docker-compose.yml) | Local laptop, hacking on the lab. The default. |
| **Kubernetes (Kustomize)** | [`k8s/`](k8s/) | Single cluster, three namespaces simulating regions. A bridge between docker-compose and real geo. |
| **Cloud, multi-region** | [`terraform/`](terraform/) | Sketch only. The shape you'd actually ship: AWS MSK + ElastiCache, MM2 between regions over private networking. |

All three implement the same architecture (see [`../ARCHITECTURE.md`](../ARCHITECTURE.md)).
The differences are operational realism, not design.

## Choosing a target

Pick the one that matches your goal:

- **Learning the failure modes hands-on** → docker-compose. It runs in
  a minute, lets you `docker network disconnect` a region with one
  command, and the tail of every container log is on the same host.
- **Validating the design on a real K8s cluster** → `k8s/`. Deploy
  three namespaces, point your apps at the right one, exercise the
  same ops scripts adapted to `kubectl exec`. You'll hit real
  Kubernetes problems (image pulls, PVC sizing, service DNS) but not
  cross-region networking — those still happen "inside" one cluster.
- **Going to production** → `terraform/` is a pointer at the right
  shape. Real production needs decisions this lab doesn't make:
  authentication, secrets management, observability stack, CI/CD,
  capacity planning. Treat the Terraform sketch as the **first 10%**
  of what you'd actually write.

## Cross-target invariants

These are properties that must hold regardless of where you deploy:

1. **Three origin-region topics** with the same name on every cluster
   (`us.events`, `eu.events`, `ap.events`). Configured by MM2's
   `IdentityReplicationPolicy`. (See ADR-0006.)
2. **MirrorMaker 2 with six directional flows** between every cluster
   pair, each filtering only the source's own origin topic.
3. **Schema Registry reachable from every region** at boot time. The
   lab uses one global SR; production should use Schema Linking or
   per-region SRs (ADR-0004).
4. **Materializer per region**, configured with `REGION`, `BOOTSTRAP`
   (local), `REDIS_ADDR` (local), `SR_URL` (global or local mirror).
5. **Kafka KRaft `CLUSTER_ID`** must be a base64-encoded 16-byte UUID
   (22 chars, no padding) per cluster. Each region needs a unique one.

If your deployment violates any of these, expect mirror loops, schema
mismatches, or KRaft crash-loops.

## What's not in any target

- Authentication, TLS, ACLs.
- Observability stack (metrics + logs + traces).
- Backups (Kafka tiered storage / snapshot to S3).
- Disaster-recovery drills.
- CI/CD pipelines.

Add these as needed; the architecture is independent of them.
