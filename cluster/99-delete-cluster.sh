#!/usr/bin/env bash
# Paso final (local): borra las 5 instancias y todo lo que hay en HDFS. Solo al acabar el trabajo.
# Uso: ./cluster/99-delete-cluster.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

# Buscar todas las instancias del clúster, encendidas o apagadas
IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_NODES" \
            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query "Reservations[].Instances[].InstanceId" \
  --output text)

if [ -z "$IDS" ]; then
  echo "No hay instancias del clúster."
  exit 0
fi

# Pedir confirmación
echo "Se van a BORRAR: $IDS"
read -r -p "Escribe BORRAR para confirmar: " ANSWER
if [ "$ANSWER" != "BORRAR" ]; then
  echo "Cancelado."
  exit 1
fi

# Borrar y esperar a que terminen
aws ec2 terminate-instances --instance-ids $IDS > /dev/null
aws ec2 wait instance-terminated --instance-ids $IDS
echo "Borradas. Los grupos de seguridad se quedan (no cuestan nada)."
