# Kubernetes manifests (sketch)

These manifests deploy the same lab to a single Kubernetes cluster
using **one namespace per region** to simulate geo-distribution. The
cross-region behavior is modeled by routing between namespaces;
nothing is actually multi-cluster.

For a real geo deployment you would run **three separate clusters** in
three regions, with Kafka exposed externally for the MirrorMaker 2
connectors that cross regions. The per-region manifests here are
useful as a starting point for that work — copy `base/` into each
cluster's manifest tree, set the right external endpoints, and the
shape is the same.

```
infra/k8s/
├── base/                  per-region stack (Kafka + Redis + materializer)
│   ├── kustomization.yaml
│   ├── kafka.yaml
│   ├── redis.yaml
│   └── materializer.yaml
├── overlays/              one overlay per simulated region
│   ├── us/
│   ├── eu/
│   └── ap/
└── shared/                Schema Registry + MM2 (single instances for the lab)
    ├── kustomization.yaml
    ├── schema-registry.yaml
    └── mirrormaker.yaml
```

## Deploy

```bash
# All three regions + the shared services
kubectl apply -k infra/k8s/overlays/us
kubectl apply -k infra/k8s/overlays/eu
kubectl apply -k infra/k8s/overlays/ap
kubectl apply -k infra/k8s/shared

kubectl get pods -A -l app.kubernetes.io/part-of=redis-kafka-wal
```

Schemas and topics are not bootstrapped automatically here — the
docker-compose lab uses `schemas-init` and `topics-init` one-shots; the
K8s sketch leaves that to the user. Either translate those scripts into
`Job` manifests or run them once via `kubectl run`.

## What's intentionally not in this sketch

- **TLS, SASL, NetworkPolicies.** Production geo Kafka requires all
  three. Use `cert-manager` for certificates and Strimzi if you want
  declarative Kafka management.
- **Persistent volumes sized for real workloads.** The StatefulSets
  request 5Gi each — fine for the lab, way too small for prod.
- **Multi-cluster connectivity.** Real geo needs cluster-to-cluster
  networking (Submariner, Cilium Cluster Mesh, public Kafka listeners
  with mTLS, …). Out of scope here.
- **HPA / VPA / PDBs.** None of this is autoscaled.
- **Strimzi or the Kafka operator.** The manifests here run vanilla
  KRaft Kafka in a StatefulSet, the same way the docker-compose stack
  does. For production, switch to Strimzi.

## Recommended production path

1. Replace the StatefulSet-based Kafka with **Strimzi** (`Kafka` CR).
2. Replace the Redis StatefulSet with the **Bitnami Redis chart** or
   a managed offering (ElastiCache, Memorystore).
3. Move MirrorMaker to **Strimzi's `KafkaMirrorMaker2` CR**, which is
   the supported way to run MM2 on K8s.
4. Move Schema Registry to one instance per cluster, with **Confluent
   Schema Linking** for cross-cluster sync (or apply ADR-0004's
   migration plan).
5. Apply NetworkPolicies that allow cluster-to-cluster traffic only
   on the Kafka external listener port.

See [`infra/terraform/`](../terraform/) for an AWS-flavored sketch of
the same shape using MSK + ElastiCache.
