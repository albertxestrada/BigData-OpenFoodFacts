#!/usr/bin/env bash
# Reconstruye el clúster siguiendo DEPLOYMENT.md.
# Uso: ./scripts/cluster.sh deploy
# Requiere AWS CLI v2, Git, SSH, rsync y Python 3.9-3.11 (dependencias de clase).
# Credenciales y clave son locales. La configuración remota está en ansible/deploy.yml.
set -euo pipefail

usage() {
    cat <<'EOF'
Uso: ./scripts/cluster.sh deploy

  deploy       Crea master, 3 workers y cliente; configura Hadoop, YARN y Spark;
               carga Open Food Facts, genera la muestra y comprueba el clúster.
  --help       Muestra esta ayuda.

Solo sirve para crear un clúster nuevo: se detiene si hadoop-master ya existe,
incluso apagado. No borra recursos ni vuelve a formatear un clúster existente.
Antes de ejecutar: configurar config/cluster.env, renovar AWS Academy y guardar
la clave indicada por SSH_KEY. El despliegue completo puede tardar bastante.
EOF
}

die() { printf 'Error: %s\n' "$*" >&2; exit 1; }

case "${1:---help}" in
    -h|--help) usage; exit 0 ;;
    deploy) shift ;;
    *) usage >&2; exit 1 ;;
esac
[[ $# -eq 0 ]] || die "Argumento desconocido. Consulta --help."

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
config_file="${CLUSTER_CONFIG:-$project_root/config/cluster.env}"
[[ -f "$config_file" ]] || die "Copia config/cluster.env.example a config/cluster.env."
# shellcheck source=../config/cluster.env.example
source "$config_file"
: "${AWS_REGION:?Define AWS_REGION}"
: "${AWS_PROFILE:?Define AWS_PROFILE}"
: "${SSH_KEY:?Define SSH_KEY}"
: "${SSH_USER:?Define SSH_USER}"
: "${REMOTE_DIR:?Define REMOTE_DIR}"
for dependency in aws git ssh rsync tar; do
    command -v "$dependency" >/dev/null || die "Falta $dependency. Revisa los requisitos de DEPLOYMENT.md."
done
[[ "$AWS_REGION" == us-east-1 ]] || die "La AMI del playbook de clase requiere us-east-1."
[[ "$SSH_USER" == ec2-user ]] || die "El playbook de clase instala todo para ec2-user."
[[ -f "$SSH_KEY" ]] || die "No se encuentra SSH_KEY: $SSH_KEY"
[[ "$REMOTE_DIR" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || die "REMOTE_DIR debe ser un nombre de carpeta."

export AWS_PROFILE AWS_REGION
export AWS_DEFAULT_REGION="$AWS_REGION" AWS_CLI_AUTO_PROMPT=off

# 1. Antes de instalar o crear nada, proteger un clúster que ya exista.
printf 'Comprobando que hadoop-master no existe...\n'
for master_tag in Name Group; do
    existing_master="$(aws --region "$AWS_REGION" --profile "$AWS_PROFILE" --no-cli-pager ec2 describe-instances \
        --filters "Name=tag:$master_tag,Values=hadoop-master" \
            'Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down' \
        --query 'Reservations[].Instances[].InstanceId' --output text)"
    [[ -z "$existing_master" ]] || die "hadoop-master ya existe ($existing_master). Despliegue cancelado."
done

# El repositorio de clase fija Ansible 2.15. Elegir un Python compatible.
python_bin=''
for candidate in python3.11 python3.10 python3.9 python3; do
    if command -v "$candidate" >/dev/null && "$candidate" -c \
        'import sys; sys.exit(not ((3, 9) <= sys.version_info[:2] <= (3, 11)))' 2>/dev/null; then
        python_bin="$(command -v "$candidate")"
        break
    fi
done
[[ -n "$python_bin" ]] || die "Instala Python 3.9, 3.10 o 3.11 con venv para las dependencias de clase."
SSH_KEY="$("$python_bin" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).expanduser().resolve())' "$SSH_KEY")"
case "$SSH_KEY$project_root" in
    *\"*|*$'\n'*) die "Las rutas no pueden contener comillas dobles ni saltos de línea." ;;
esac

# 2. Descargar una revisión conocida. La caché queda fuera de Git.
runtime_dir="$project_root/.deployment"
upstream_dir="$runtime_dir/upstream"
work_dir="$runtime_dir/class"
revision=354f82c2a8650f71470b768103ece5e8894daeff
mkdir -p "$work_dir" "$runtime_dir/tmp"
: > "$runtime_dir/known_hosts"
chmod 600 "$runtime_dir/known_hosts"
if [[ ! -d "$upstream_dir/.git" ]]; then
    git clone --quiet https://github.com/memaldi/hadoop-ansible-ec2 "$upstream_dir"
fi
if ! git -C "$upstream_dir" cat-file -e "$revision^{commit}" 2>/dev/null; then
    git -C "$upstream_dir" fetch --quiet --depth 1 origin "$revision"
fi
git -C "$upstream_dir" archive "$revision" | tar -x -C "$work_dir"

printf 'Preparando Ansible...\n'
"$python_bin" -m venv "$runtime_dir/venv"
python_bin="$runtime_dir/venv/bin/python"
"$python_bin" -m pip install --quiet --disable-pip-version-check -r "$work_dir/requirements.txt"
export ANSIBLE_LOCAL_TEMP="$runtime_dir/tmp"
export ANSIBLE_CONFIG="$work_dir/ansible.cfg"
chmod 600 "$SSH_KEY"

# Pasar rutas y opciones como JSON conserva espacios y evita construir argumentos de shell.
"$python_bin" - "$project_root" "$work_dir" "$runtime_dir" "$SSH_KEY" "$SSH_USER" \
    "$REMOTE_DIR" "$AWS_REGION" "$AWS_PROFILE" > "$runtime_dir/vars.json" <<'PY'
import json
from pathlib import Path
import sys

project, upstream, runtime, key, user, remote, region, profile = sys.argv[1:]
json.dump(dict(project_dir=project, upstream_dir=upstream, runtime_dir=runtime,
               ssh_key=str(Path(key).expanduser().resolve()), ssh_user=user,
               remote_dir=remote, aws_region=region, aws_profile=profile,
               hadoop_home=f"/home/{user}/hadoop-3.3.6",
               spark_version="3.5.5", java_home="/usr/lib/jvm/java"), sys.stdout)
PY
chmod 600 "$runtime_dir/vars.json"
playbook="$project_root/ansible/deploy.yml"
ansible_playbook="$runtime_dir/venv/bin/ansible-playbook"

# 3. Preparar las copias de clase: red, memoria, logs y espera de SSH.
"$ansible_playbook" -i localhost, "$playbook" --tags prepare -e "@$runtime_dir/vars.json"

# 4. Crear master y workers con el playbook del profesor, en su propia carpeta.
printf 'Creando master y workers...\n'
(
    cd "$work_dir"
    "$ansible_playbook" -i inventory.aws_ec2.yml --private-key "$SSH_KEY" \
        --user "$SSH_USER" create-instances.yml
)

# 5. Crear cliente, configurar todos los nodos, cargar datos y ejecutar las pruebas.
"$ansible_playbook" -i localhost, "$playbook" --skip-tags prepare -e "@$runtime_dir/vars.json"
printf '\nDespliegue terminado. Para subir cambios de código: ./scripts/sync.sh all\n'
