#!/usr/bin/env bash
# Paso 4 (local): crea la instancia hadoop-client, la que lanza los trabajos de Spark.
# Uso: ./cluster/04-create-client.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

# Buscar si ya existe un client, encendido o apagado
CLIENT_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_CLIENT" \
            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query "Reservations[].Instances[].InstanceId" \
  --output text)

# Si existe, no crear otro
if [ -n "$CLIENT_ID" ]; then
  echo "Ya existe $ENV_CLIENT ($CLIENT_ID)."
  exit 1
fi

# Crear la instancia con la misma AMI (Amazon Linux 2023), clave y grupos que el resto
CLIENT_ID=$(aws ec2 run-instances \
  --image-id ami-066784287e358dad1 \
  --instance-type t2.large \
  --key-name vockey \
  --security-groups "$ENV_SG_CLUSTER" "$ENV_SG_MASTER" \
  --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=50,DeleteOnTermination=true}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$ENV_CLIENT},{Key=Group,Value=$ENV_CLIENT}]" \
  --query "Instances[0].InstanceId" \
  --output text)

# Esperar a que arranque y acepte SSH (2 a 3 minutos)
echo "Creado $CLIENT_ID. Esperando a que esté listo..."
aws ec2 wait instance-status-ok --instance-ids "$CLIENT_ID"

echo
echo "Siguiente: ./cluster/05-copy-to-master.sh"
