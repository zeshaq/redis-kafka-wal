# Terraform sketch — multi-region AWS

This is a **sketch**, not a working module. Real production Terraform
for this architecture is hundreds to thousands of lines (VPC peering,
Transit Gateway, security groups, KMS, CloudWatch, IAM roles, MSK
configurations, ElastiCache parameter groups, …) and depends entirely
on your AWS account topology.

What's here is the **shape**: which AWS resources implement which
architectural pieces, and where the cross-region wiring lives.

## Mapping

| Lab piece | AWS service | Notes |
| --- | --- | --- |
| Per-region Kafka cluster | **MSK** (Managed Streaming for Kafka) | One cluster per region. KRaft mode. Multi-AZ within region. |
| Per-region Redis | **ElastiCache for Redis OSS** | Cluster mode disabled for simplicity; multi-AZ with replication. (Active-active *across* regions is what this lab provides; ElastiCache Global Datastore is a separate feature you don't need here.) |
| MirrorMaker 2 | **MSK Connect** | Run MM2 connectors as MSK Connect plugins. One MSK Connect cluster per region (or one global one in a hub VPC). |
| Schema Registry | **AWS Glue Schema Registry** OR self-hosted Confluent SR | Glue's wire format is incompatible with Confluent's; use Confluent SR if you want to keep the wire format from this lab. |
| Cross-region transport | **VPC peering or Transit Gateway** + private DNS | MM2 connectors need to reach the *peer* region's MSK bootstrap brokers. |
| Materializer | **ECS Fargate** or **EKS** | Stateless, small, scaled by partition count (one task per partition is fine). |

## Files (sketch)

```
terraform/
├── README.md             this file
├── main.tf               provider and module orchestration sketch
├── variables.tf          regions, sizes, naming
└── modules/
    ├── region/           per-region module: MSK + ElastiCache + materializer (ECS)
    └── mm2/              MM2 connectors (one per directional flow, six total)
```

The included `main.tf` is illustrative. It will not `terraform apply`
against an empty AWS account without:

1. A real provider configuration with credentials.
2. A real VPC and subnet topology.
3. SGs allowing MM2 ↔ MSK on broker ports across regions.
4. CloudWatch log destinations for MSK / Connect / ECS.

## Recommended progression

Don't start here. Progress through the targets:

1. Get the **docker-compose** lab running and convergent on demo.sh.
2. Bring up the **K8s overlays** in a single cluster, validate the
   same convergence with `kubectl exec` adaptations of the scripts.
3. Then take this Terraform sketch as the starting point for a real
   multi-region deployment, replacing each placeholder with your
   account's reality.
