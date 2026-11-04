#!/usr/bin/env bash
# Paso 7 (master): sustituye el yarn-site.xml de Ansible por el nuestro (6 GB por worker) y reinicia YARN.
# Uso: ./cluster/07-apply-config.sh
set -euo pipefail
cd "$(dirname "$0")"
CONF=hadoop-3.3.6/etc/hadoop

# Master: copiar su yarn-site.xml y reiniciar el ResourceManager
cp conf/master/yarn-site.xml ~/$CONF/
sudo systemctl restart resourcemanager
echo "OK hadoop-master"

# Workers: copiar su yarn-site.xml y reiniciar el NodeManager de cada uno
for worker in hadoop-worker-1 hadoop-worker-2 hadoop-worker-3; do
  scp -q conf/worker/yarn-site.xml "$worker:$CONF/"
  ssh "$worker" "sudo systemctl restart nodemanager"
  echo "OK $worker"
done

# Esperar a que los NodeManagers vuelvan a registrarse y comprobar
echo "Esperando 30 s..."
sleep 30
bash check-cluster.sh

echo
echo "Siguiente: ./cluster/08-setup-client.sh"
