output "broker_public_ips" {
  value = aws_instance.broker[*].public_ip
}

output "broker_private_ips" {
  value = aws_instance.broker[*].private_ip
}

# broker[0] is the Raft seed the others join.
output "seed_private_ip" {
  value = aws_instance.broker[0].private_ip
}

output "client_public_ips" {
  value = aws_instance.client[*].public_ip
}

output "client_private_ips" {
  value = aws_instance.client[*].private_ip
}

# First client doubles as the benchmark coordinator.
output "coordinator_public_ip" {
  value = aws_instance.client[0].public_ip
}

output "broker_instance_type" {
  value = var.broker_instance_type
}
