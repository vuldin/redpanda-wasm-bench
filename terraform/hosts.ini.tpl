[broker]
%{ for i, ip in broker_public_ips ~}
${ip} ansible_user=${ssh_user} ansible_become=True private_ip=${broker_private_ips[i]} id=${i}
%{ endfor ~}

[client]
%{ for i, ip in client_public_ips ~}
${ip} ansible_user=${ssh_user} ansible_become=True private_ip=${client_private_ips[i]} id=${i}
%{ endfor ~}

[monitor]
%{ for i, ip in monitor_public_ips ~}
${ip} ansible_user=${ssh_user} ansible_become=True private_ip=${monitor_private_ips[i]} id=${i}
%{ endfor ~}

# Everything node_exporter goes on. The monitoring host is in here too, so a
# Prometheus that is itself starved of CPU is visible rather than guessed at.
[metrics_hosts:children]
broker
client
monitor

[all:vars]
broker_instance=${broker_instance}
seed_private_ip=${broker_private_ips[0]}
