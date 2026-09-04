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
