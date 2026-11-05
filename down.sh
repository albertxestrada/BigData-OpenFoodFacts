#!/usr/bin/env bash
# Borra el clúster. Con --stop solo lo apaga.
set -euo pipefail
NODES="hadoop-master,hadoop-worker-1,hadoop-worker-2,hadoop-worker-3,hadoop-client"

# IDs de las instancias por estado
ids() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$NODES" "Name=instance-state-name,Values=$1" \
    --query "Reservations[].Instances[].InstanceId" --output text
}

# Apagar
if [ "${1:-}" = --stop ]; then
  running=$(ids running)
  [ -z "$running" ] || aws ec2 stop-instances --instance-ids $running > /dev/null
  echo "Apagando: $running"
  exit 0
fi

# Borrar, con confirmación
all=$(ids pending,running,stopping,stopped)
if [ -z "$all" ]; then
  echo "No hay instancias del clúster."
  exit 0
fi
echo "Se van a BORRAR: $all"
read -r -p "Escribe BORRAR para confirmar: " answer
if [ "$answer" != BORRAR ]; then
  echo "Cancelado."
  exit 1
fi
aws ec2 terminate-instances --instance-ids $all > /dev/null
aws ec2 wait instance-terminated --instance-ids $all
echo "Borradas."
