# Sketch — orchestrates one `region` module per lab region, plus a `mm2`
# module that wires the six directional flows. NOT a working apply.
# Read infra/terraform/README.md before touching this.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  alias  = "us"
  region = var.regions["us"]
}
provider "aws" {
  alias  = "eu"
  region = var.regions["eu"]
}
provider "aws" {
  alias  = "ap"
  region = var.regions["ap"]
}

module "region_us" {
  source                      = "./modules/region"
  providers                   = { aws = aws.us }
  region_label                = "us"
  vpc_cidr                    = var.vpc_cidr_per_region["us"]
  msk_kafka_version           = var.msk_kafka_version
  msk_instance_type           = var.msk_instance_type
  msk_broker_count            = var.msk_broker_count_per_region
  redis_node_type             = var.redis_node_type
  redis_replicas              = var.redis_replicas_per_region
  tags                        = var.tags
}

module "region_eu" {
  source                      = "./modules/region"
  providers                   = { aws = aws.eu }
  region_label                = "eu"
  vpc_cidr                    = var.vpc_cidr_per_region["eu"]
  msk_kafka_version           = var.msk_kafka_version
  msk_instance_type           = var.msk_instance_type
  msk_broker_count            = var.msk_broker_count_per_region
  redis_node_type             = var.redis_node_type
  redis_replicas              = var.redis_replicas_per_region
  tags                        = var.tags
}

module "region_ap" {
  source                      = "./modules/region"
  providers                   = { aws = aws.ap }
  region_label                = "ap"
  vpc_cidr                    = var.vpc_cidr_per_region["ap"]
  msk_kafka_version           = var.msk_kafka_version
  msk_instance_type           = var.msk_instance_type
  msk_broker_count            = var.msk_broker_count_per_region
  redis_node_type             = var.redis_node_type
  redis_replicas              = var.redis_replicas_per_region
  tags                        = var.tags
}

# MM2 module: takes all three regions' MSK bootstrap endpoints and
# wires the six directional flows as MSK Connect connectors. In a real
# deployment, also: VPC peering or Transit Gateway between the three
# VPCs, and SGs allowing MSK Connect → peer-region MSK on broker ports.
module "mm2" {
  source = "./modules/mm2"
  flows = {
    "us-eu" = { source = module.region_us, target = module.region_eu, topic = "us\\.events" }
    "us-ap" = { source = module.region_us, target = module.region_ap, topic = "us\\.events" }
    "eu-us" = { source = module.region_eu, target = module.region_us, topic = "eu\\.events" }
    "eu-ap" = { source = module.region_eu, target = module.region_ap, topic = "eu\\.events" }
    "ap-us" = { source = module.region_ap, target = module.region_us, topic = "ap\\.events" }
    "ap-eu" = { source = module.region_ap, target = module.region_eu, topic = "ap\\.events" }
  }
  tags = var.tags
}
