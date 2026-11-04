#!/usr/bin/env bash
# Paso 5 (local): copia al master los scripts, la configuración y los trabajos de Spark, y la clave.
# Repetir cada vez que cambie algo de eso.
# Uso: ./cluster/05-copy-to-master.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"
REPO="$(cd "$(dirname "$0")/.." && pwd)"

# DNS público del master encendido (cambia en cada arranque)
MASTER_DNS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_MASTER" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PublicDnsName" \
  --output text)

# IP privada del client encendido (la usa el paso 6 para el /etc/hosts del master)
CLIENT_IP=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$ENV_CLIENT" \
            "Name=instance-state-name,Values=running" \
  --query "Reservations[0].Instances[0].PrivateIpAddress" \
  --output text)

# aws devuelve "None" si no encuentra la instancia
if [ "$MASTER_DNS" = "None" ]; then
  echo "No hay $ENV_MASTER encendido. Créalo con ./cluster/01-create-cluster.sh o enciéndelo con ./cluster/start.sh"
  exit 1
fi
if [ "$CLIENT_IP" = "None" ]; then
  echo "No hay $ENV_CLIENT encendido. Créalo con ./cluster/04-create-client.sh o enciéndelo con ./cluster/start.sh"
  exit 1
fi

# Borrar la copia anterior del proyecto en el master
ssh $ENV_SSH_OPTS ec2-user@"$MASTER_DNS" "rm -rf open-food-facts-analytics && mkdir open-food-facts-analytics"

# En Mac, que tar no añada archivos ocultos ._* (no afecta en WSL)
export COPYFILE_DISABLE=1

# Solo lo que se usa en el master y en el client (rutas desde la raíz del proyecto)
FILES=(
  run.sh                            # client: lanza los trabajos
  jobs                              # client: trabajos de Spark
  cluster/06-setup-master.sh        # master: pasos 6 a 9
  cluster/07-apply-config.sh
  cluster/08-setup-client.sh
  cluster/09-upload-data.sh
  cluster/check-cluster.sh          # master: comprobar el clúster
  cluster/ssh-config                # master: ~/.ssh/config (paso 6)
  cluster/conf                      # master, workers y client: configuración de YARN, HDFS y Spark
  cluster/client-install-spark.sh   # client: instala Spark (paso 8)
)

# Copiar esa lista conservando las carpetas
tar -C "$REPO" -cf - "${FILES[@]}" \
  | ssh $ENV_SSH_OPTS ec2-user@"$MASTER_DNS" "tar -C open-food-facts-analytics -xf -"

# Escribir la IP del client en el master, donde la lee el paso 6
ssh $ENV_SSH_OPTS ec2-user@"$MASTER_DNS" "echo $CLIENT_IP > open-food-facts-analytics/cluster/client-ip.txt"

# Copiar la clave para que el master pueda entrar en los demás nodos
scp $ENV_SSH_OPTS -q "$ENV_KEY" ec2-user@"$MASTER_DNS":.ssh/vockey.pem

echo "Copiado."
echo

# Mostrar los comandos ssh para entrar al master y abrir las webs
bash "$(dirname "$0")/ssh-commands.sh"

echo
echo "Siguiente, en el master: cd open-food-facts-analytics && ./cluster/06-setup-master.sh"
