# Sketch — see README.md for what's missing for a real apply.

variable "regions" {
  description = "AWS region per lab region."
  type        = map(string)
  default = {
    us = "us-east-1"
    eu = "eu-west-1"
    ap = "ap-southeast-1"
  }
}

variable "msk_kafka_version" {
  type    = string
  default = "3.7.0"
}

variable "msk_instance_type" {
  type    = string
  default = "kafka.m5.large"
}

variable "msk_broker_count_per_region" {
  type    = number
  default = 3
}

variable "redis_node_type" {
  type    = string
  default = "cache.t4g.small"
}

variable "redis_replicas_per_region" {
  type    = number
  default = 1
}

variable "vpc_cidr_per_region" {
  type = map(string)
  default = {
    us = "10.10.0.0/16"
    eu = "10.20.0.0/16"
    ap = "10.30.0.0/16"
  }
}

variable "tags" {
  type = map(string)
  default = {
    project = "redis-kafka-wal"
    env     = "lab-sketch"
  }
}
