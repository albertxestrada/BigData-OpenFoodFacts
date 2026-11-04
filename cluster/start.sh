#!/usr/bin/env bash
# Local: enciende las 5 instancias al empezar la sesión. HDFS y YARN arrancan solos.
# Uso: ./cluster/start.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

# Buscar las instancias apagadas
IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_NODES" \
            "Name=instance-state-name,Values=stopped" \
  --query "Reservations[].Instances[].InstanceId" \
  --output text)

# Encenderlas y esperar a que estén en marcha
if [ -n "$IDS" ]; then
  aws ec2 start-instances --instance-ids $IDS > /dev/null
  echo "Encendiendo..."
  aws ec2 wait instance-running --instance-ids $IDS
fi

# Mostrar estado y DNS públicos
bash "$(dirname "$0")/status.sh"

# Mostrar los comandos ssh con el DNS nuevo
echo
bash "$(dirname "$0")/ssh-commands.sh"
