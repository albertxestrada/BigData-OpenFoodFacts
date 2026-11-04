#!/usr/bin/env bash
# Paso 6 (master): permite entrar a los nodos con "ssh hadoop-worker-1" y añade el client al /etc/hosts de todos.
# Uso: ./cluster/06-setup-master.sh
set -euo pipefail
cd "$(dirname "$0")"

# Nodos a los que el master copia /etc/hosts y en los que comprueba el acceso SSH
NODES="hadoop-worker-1 hadoop-worker-2 hadoop-worker-3 hadoop-client"

# Dejar la clave legible solo por ti (si no, ssh no la usa)
chmod 600 ~/.ssh/vockey.pem

# Instalar ~/.ssh/config: usuario ec2-user y clave vockey.pem para todos los hadoop-*
cp ssh-config ~/.ssh/config
chmod 600 ~/.ssh/config

# Añadir el client al /etc/hosts del master con la IP de client-ip.txt
if ! grep -q hadoop-client /etc/hosts; then
  # El /etc/hosts de Ansible no acaba en salto de línea: añadirlo para no pegar el client a la última línea
  [ -z "$(tail -c 1 /etc/hosts)" ] || echo | sudo tee -a /etc/hosts > /dev/null
  echo "$(cat client-ip.txt)  hadoop-client" | sudo tee -a /etc/hosts
fi

# Copiar ese /etc/hosts a los workers y al client para que todos se encuentren por su nombre.
# Los workers lo necesitan: el ApplicationMaster de Spark corre en un worker y conecta con el driver en hadoop-client.
for node in $NODES; do
  scp /etc/hosts "$node":/tmp/hosts
  ssh "$node" "sudo cp /tmp/hosts /etc/hosts"
done

# Comprobar que el master entra en cada nodo
for node in $NODES; do
  echo "$node: $(ssh "$node" hostname)"
done

echo "Esperado: 4 líneas con ip-172-..."
echo
echo "Siguiente: ./cluster/07-apply-config.sh"
