#!/usr/bin/env bash
# Paso 0 (local, una sola vez): crea el entorno de Ansible y clona el playbook.
# Uso: ./cluster/00-prepare-local.sh
set -euo pipefail

# Comprobar que la clave ya está donde la espera el playbook
if [ ! -f ~/.ssh/vockey.pem ]; then
  echo "Falta ~/.ssh/vockey.pem: cp ~/Downloads/labsuser.pem ~/.ssh/vockey.pem && chmod og-rwx ~/.ssh/vockey.pem"
  exit 1
fi

# Configurar aws en ~/.aws/config: región del Learner Lab y salida directa, sin abrir un paginador
aws configure set region us-east-1
aws configure set cli_pager ""

# Entorno de Ansible con el playbook
python3 -m venv ~/ansible
source ~/ansible/bin/activate
[ -d ~/hadoop-ansible-ec2 ] || git clone https://github.com/memaldi/hadoop-ansible-ec2 ~/hadoop-ansible-ec2
pip install -r ~/hadoop-ansible-ec2/requirements.txt

echo
echo "Siguiente: ./cluster/01-create-cluster.sh"
