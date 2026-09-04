[broker]
%{ for i, ip in broker_public_ips ~}
${ip} ansible_user=${ssh_user} ansible_become=True private_ip=${broker_private_ips[i]} id=${i}
%{ endfor ~}

[client]
%{ for i, ip in client_public_ips ~}
${ip} ansible_user=${ssh_user} ansible_become=True private_ip=${client_private_ips[i]} id=${i}
%{ endfor ~}

[all:vars]
broker_instance=${broker_instance}
seed_private_ip=${broker_private_ips[0]}
