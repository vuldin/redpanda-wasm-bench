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

# Empty list when monitor_count = 0, which is what every consumer checks
# against rather than assuming the stack is present.
output "monitor_public_ips" {
  value = aws_instance.monitor[*].public_ip
}

output "monitor_private_ips" {
  value = aws_instance.monitor[*].private_ip
}

# The URL to hand someone. Printed by `wb up` and `wb monitor` so it does not
# have to be assembled by hand from an IP and a remembered port.
output "grafana_url" {
  value = length(aws_instance.monitor) > 0 ? "http://${aws_instance.monitor[0].public_ip}:3000" : ""
}

output "prometheus_url" {
  value = length(aws_instance.monitor) > 0 ? "http://${aws_instance.monitor[0].public_ip}:9090" : ""
}
