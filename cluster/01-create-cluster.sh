#!/usr/bin/env bash
# Paso 1 (local): crea hadoop-master y 3 workers con Hadoop, HDFS, YARN y Spark usando Ansible.
# Uso: ./cluster/01-create-cluster.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

# Buscar si ya existe un master, encendido o apagado
MASTER_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_MASTER" \
            "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query "Reservations[].Instances[].InstanceId" \
  --output text)

# Si existe, parar: el playbook formatea HDFS y borraría los datos
if [ -n "$MASTER_ID" ]; then
  echo "Ya existe $ENV_MASTER ($MASTER_ID). Para encenderlo: ./cluster/start.sh"
  exit 1
fi

# Activar el entorno de Ansible y lanzar el playbook desde su carpeta
source ~/ansible/bin/activate
cd ~/hadoop-ansible-ec2
ansible-playbook \
  -i inventory.aws_ec2.yml \
  --key-file ~/.ssh/vockey.pem \
  --user ec2-user \
  create-instances.yml

echo
echo "Siguiente: ./cluster/02-open-ssh.sh"
