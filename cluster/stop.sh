#!/usr/bin/env bash
# Local: apaga las 5 instancias al terminar la sesión. Apagadas no gastan crédito y HDFS conserva los datos.
# Uso: ./cluster/stop.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

# Buscar las instancias encendidas
IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_NODES" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[].Instances[].InstanceId" \
  --output text)

# Apagarlas
if [ -n "$IDS" ]; then
  aws ec2 stop-instances --instance-ids $IDS > /dev/null
fi
echo "Apagando: $IDS"
