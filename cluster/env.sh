#!/usr/bin/env bash
# Variables comunes de los scripts que se ejecutan en tu equipo (WSL o Mac). Se carga con source, no se ejecuta solo.

# Nombres de las instancias (tag Name en EC2)
ENV_MASTER="hadoop-master"
ENV_WORKER_1="hadoop-worker-1"
ENV_WORKER_2="hadoop-worker-2"
ENV_WORKER_3="hadoop-worker-3"
ENV_CLIENT="hadoop-client"

# Todas las instancias del clúster, separadas por comas (formato del filtro de aws)
ENV_NODES="$ENV_MASTER,$ENV_WORKER_1,$ENV_WORKER_2,$ENV_WORKER_3,$ENV_CLIENT"

# Grupos de seguridad que crea el playbook: el común a todos los nodos y el del master (con SSH abierto)
ENV_SG_CLUSTER="hadoop-sg"
ENV_SG_MASTER="hadoop-master-sg"

# Clave para entrar en las instancias
ENV_KEY=~/.ssh/vockey.pem

# Opciones de ssh/scp: usar la clave y no preguntar por la huella del servidor
ENV_SSH_OPTS="-i $ENV_KEY -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
