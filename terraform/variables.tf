variable "region" {
  type    = string
  default = "us-east-1"
}

variable "azs" {
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1f"]
  description = "AZs to spread brokers/clients across (round-robin). Multi-AZ avoids single-AZ capacity limits and mirrors a real deployment."
}

variable "public_key_path" {
  type        = string
  description = "Path to the SSH public key Terraform imports as the EC2 key pair."
}

variable "key_name" {
  type    = string
  default = "rp-percore"
}

variable "ssh_user" {
  type    = string
  default = "ubuntu"
}

variable "owner" {
  type        = string
  default     = "rp-percore-benchmark"
  description = "Value for the owner tag on all resources."
}

variable "broker_instance_type" {
  type        = string
  description = "EC2 instance type for the single Redpanda broker (must have local NVMe)."
}

variable "broker_arch" {
  type        = string
  default     = "x86_64"
  description = "Architecture of the broker instance: x86_64 or arm64."
  validation {
    condition     = contains(["x86_64", "arm64"], var.broker_arch)
    error_message = "broker_arch must be x86_64 or arm64."
  }
}

variable "client_instance_type" {
  type        = string
  default     = "c5n.9xlarge"
  description = "EC2 instance type for the load clients. Held constant across runs."
}

variable "client_count" {
  type        = number
  default     = 3
  description = "Number of OMB load clients (distributed workers). client[0] also runs the coordinator. More clients = more fetch load to saturate the broker's single hot core."
}

variable "broker_count" {
  type        = number
  default     = 1
  description = "Number of Redpanda brokers. Must be >= the replication factor under test (RF=6 needs 6 brokers). broker[0] is the seed; the others join it."
}

# --- Monitoring -------------------------------------------------------------

variable "monitor_count" {
  type        = number
  default     = 1
  description = "Prometheus/Grafana instances. 1 is the whole stack; 0 opts out and every other resource is unchanged, for a run that wants no observer on the network at all."
}

variable "monitor_instance_type" {
  type        = string
  default     = "m6i.2xlarge"
  description = "EC2 instance type for the monitoring host. 8 vCPU / 32 GB holds the internal /metrics set from 3 brokers at a 15s interval with headroom; the tight /public_metrics scrape is cheap by comparison."
}

variable "monitor_volume_size" {
  type        = number
  default     = 100
  description = "Root volume GB on the monitoring host. This is the Prometheus TSDB, so it bounds how long a run's history survives."
}

variable "operator_extra_cidrs" {
  type        = list(string)
  default     = []
  description = <<-EOT
    Additional CIDRs allowed to reach the cluster, on top of the /32 this
    machine's egress IP resolves to.

    You need this when your egress IP is not stable. A carrier-grade NAT can
    put a browser on a different address than the one an IP-reflector API
    reports, and the symptom is not a refusal - every port TIMES OUT while the
    VPC, route table and security group all look correct. Grafana in a browser
    is a different flow than the terraform apply that opened the port, so this
    bites the monitoring host first. `wb monitor` diagnoses it and prints the
    fix. Widening to the enclosing /20 is usually enough.
  EOT
}

# --- Placement --------------------------------------------------------------

variable "broker_placement_group" {
  type        = bool
  default     = true
  description = <<-EOT
    Put the brokers in a CLUSTER placement group, so AWS packs them onto
    closely-coupled hardware instead of wherever it likes inside the AZ.

    Why: `produce` (client to quorum ack) is the noisiest leg in this
    benchmark and it varies more between cluster instantiations than the
    thing being measured. Measured 2026-09-16/17 on two identical
    single-AZ topologies: produce p50 666 us and 1,126 us, a 460 us spread,
    while relay_consume moved only 58 us. A cluster placement group removes
    rack distance as one candidate explanation for that.

    It does NOT remove the others - noisy neighbours, a different hardware
    generation behind the same instance name, or instance-store variance are
    all untouched. Treat it as eliminating one variable, not as a fix.

    COST: capacity gets harder to find, because AWS now has to place the
    brokers adjacently. This harness has already failed to place 8 x
    c5n.9xlarge in a region on a normal apply, so expect more
    InsufficientInstanceCapacity. The capacity probe creates a throwaway
    placement group and asks against it, so a refusal still arrives in
    seconds rather than as a stalled apply.

    Brokers only, deliberately. AWS recommends one instance type per cluster
    placement group, and the clients and monitoring host are different types.
    Cluster groups are also single-AZ, so this is refused when azs has more
    than one entry rather than silently ignored.
  EOT
}
