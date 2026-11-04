#!/usr/bin/env bash
# Local: muestra, listos para copiar y pegar, los comandos ssh para entrar al master y abrir las webs de HDFS y YARN.
# Cada túnel va en su propia terminal y hay que dejarla abierta mientras se usa la web.
# El DNS del master cambia en cada arranque: volver a ejecutarlo después de ./cluster/start.sh.
# Uso: ./cluster/ssh-commands.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

# DNS público del master encendido
MASTER_DNS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_MASTER" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicDnsName" \
  --output text)

# aws devuelve "None" si no encuentra la instancia
if [ "$MASTER_DNS" = "None" ]; then
  echo "No hay $ENV_MASTER encendido. Enciéndelo con ./cluster/start.sh"
  exit 1
fi

echo "Master:"
echo "ssh -i $ENV_KEY ec2-user@$MASTER_DNS"
echo
echo
echo "HDFS, web en http://localhost:50070:"
echo "ssh -i $ENV_KEY -N -L 50070:$MASTER_DNS:50070 ec2-user@$MASTER_DNS"
echo
echo
echo "YARN, web en http://localhost:8088/cluster/nodes:"
echo "ssh -i $ENV_KEY -N -L 8088:$MASTER_DNS:8088 ec2-user@$MASTER_DNS"
