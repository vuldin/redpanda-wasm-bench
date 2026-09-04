terraform {
  required_version = ">= 1.3"
  required_providers {
    aws    = { source = "hashicorp/aws", version = ">= 4.0" }
    random = { source = "hashicorp/random", version = "~> 3.4" }
  }
}

provider "aws" {
  region = var.region
}

resource "random_id" "hash" {
  byte_length = 4
}

# --- Networking -------------------------------------------------------------

resource "aws_vpc" "bench" {
  cidr_block = "10.0.0.0/16"
  tags       = { Name = "rp-percore-${random_id.hash.hex}", owner = var.owner }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.bench.id
  tags   = { owner = var.owner }
}

resource "aws_route" "internet" {
  route_table_id         = aws_vpc.bench.main_route_table_id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.igw.id
}

# One subnet per AZ so brokers/clients spread across AZs. This both matches a
# realistic multi-AZ deployment and avoids single-AZ InsufficientInstanceCapacity
# for large instance counts (e.g. 6x r8id.16xlarge). Per-core fetch results are
# unaffected: with follower-fetching off the leader is CPU-bound, and the latency
# we record is broker-side handler time, not client RTT.
resource "aws_subnet" "bench" {
  for_each                = { for idx, az in var.azs : az => idx }
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = "10.0.${each.value}.0/24"
  map_public_ip_on_launch = true
  availability_zone       = each.key
  tags                    = { owner = var.owner }
}

locals {
  subnet_ids = [for az in var.azs : aws_subnet.bench[az].id]
}

data "http" "myip" {
  url = "https://ipv4.icanhazip.com"
}

resource "aws_security_group" "bench" {
  name   = "rp-percore-${random_id.hash.hex}"
  vpc_id = aws_vpc.bench.id

  # Full connectivity inside the VPC (broker <-> client)
  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # SSH + admin/metrics from the machine running this harness only
  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["${chomp(data.http.myip.response_body)}/32"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "rp-percore-${random_id.hash.hex}", owner = var.owner }
}

resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}-${random_id.hash.hex}"
  public_key = file(var.public_key_path)
}

# --- AMIs (resolve both architectures so broker/client arch can differ) -----

data "aws_ami" "ubuntu" {
  for_each    = toset(["amd64", "arm64"])
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-${each.key}-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  broker_ami = var.broker_arch == "arm64" ? data.aws_ami.ubuntu["arm64"].id : data.aws_ami.ubuntu["amd64"].id
  client_ami = data.aws_ami.ubuntu["amd64"].id # client held constant (x86) across runs
}

# --- Instances --------------------------------------------------------------

resource "aws_instance" "broker" {
  count                  = var.broker_count
  ami                    = local.broker_ami
  instance_type          = var.broker_instance_type
  key_name               = aws_key_pair.auth.id
  subnet_id              = local.subnet_ids[count.index % length(var.azs)]
  vpc_security_group_ids = [aws_security_group.bench.id]
  monitoring             = true
  root_block_device { volume_size = 50 }
  tags = { Name = "rp-broker-${count.index}", owner = var.owner }
}

resource "aws_instance" "client" {
  count                  = var.client_count
  ami                    = local.client_ami
  instance_type          = var.client_instance_type
  key_name               = aws_key_pair.auth.id
  subnet_id              = local.subnet_ids[count.index % length(var.azs)]
  vpc_security_group_ids = [aws_security_group.bench.id]
  monitoring             = true
  root_block_device { volume_size = 50 }
  tags = { Name = "rp-client-${count.index}", owner = var.owner }
}

resource "local_file" "inventory" {
  filename = "${path.module}/hosts.ini"
  content = templatefile("${path.module}/hosts.ini.tpl", {
    broker_public_ips  = aws_instance.broker[*].public_ip
    broker_private_ips = aws_instance.broker[*].private_ip
    client_public_ips  = aws_instance.client[*].public_ip
    client_private_ips = aws_instance.client[*].private_ip
    broker_instance    = var.broker_instance_type
    ssh_user           = var.ssh_user
  })
}
