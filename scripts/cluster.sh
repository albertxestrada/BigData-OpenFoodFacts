#!/usr/bin/env bash
# Reconstruye el clúster siguiendo DEPLOYMENT.md.
# Uso: ./scripts/cluster.sh deploy | progress
# Requiere AWS CLI v2, Git, SSH y Python 3.9-3.11 (dependencias de clase).
# Credenciales y clave son locales. La configuración remota está en ansible/deploy.yml.
set -euo pipefail

usage() {
    cat <<'EOF'
Uso: ./scripts/cluster.sh deploy
Para ver el progreso: ./scripts/cluster.sh progress

  deploy       Crea master, 3 workers y cliente con Hadoop, YARN y Spark.
               El driver de Spark corre en el master. Carga Open Food Facts,
               genera la muestra y comprueba el clúster.
  progress     Muestra las descargas de Hadoop y Spark cada 10 segundos.
  --help       Muestra esta ayuda.

deploy solo sirve para crear un clúster nuevo: se detiene si hadoop-master ya existe,
incluso apagado. No borra recursos ni vuelve a formatear un clúster existente.
Antes de ejecutar: configurar config/cluster.env, renovar AWS Academy y guardar
la clave indicada por SSH_KEY. El despliegue completo puede tardar bastante.
La salida se conserva en .deployment/deploy.log para revisar posibles fallos.
EOF
}

die() { printf 'Error: %s\n' "$*" >&2; exit 1; }

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

show_progress() {
    local runtime="$project_root/.deployment" config_file inventory_file progress_command master_ip ssh_config
    config_file="${CLUSTER_CONFIG:-$project_root/config/cluster.env}"
    [[ -f "$config_file" ]] || die "Falta config/cluster.env."
    [[ -x "$runtime/venv/bin/ansible" && -f "$runtime/class/inventory.aws_ec2.yml" ]] || die "Primero inicia el despliegue."
    # shellcheck source=../config/cluster.env.example
    source "$config_file"
    export AWS_PROFILE AWS_REGION
    export AWS_DEFAULT_REGION="$AWS_REGION" AWS_CLI_AUTO_PROMPT=off ANSIBLE_CONFIG="$runtime/class/ansible.cfg"
    export ANSIBLE_LOCAL_TEMP="$runtime/tmp"
    export ANSIBLE_INVENTORY_UNPARSED_FAILED=true
    export ANSIBLE_SSH_USETTY=false
    export ANSIBLE_SSH_ARGS='-C -o ControlMaster=auto -o ControlPersist=60s'
    SSH_KEY="$("$runtime/venv/bin/python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).expanduser().resolve())' "$SSH_KEY")"

    # Los workers y el cliente se consultan por la red privada, pasando por el master.
    master_ip="$(aws --region "$AWS_REGION" --profile "$AWS_PROFILE" --no-cli-pager ec2 describe-instances \
        --filters 'Name=tag:Group,Values=hadoop-master' 'Name=instance-state-name,Values=running' \
        --query 'Reservations[].Instances[].PublicIpAddress' --output text)"
    [[ "$master_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "No se encontró un único master con IP pública."
    ssh_config="$runtime/progress.ssh_config"
    cat > "$ssh_config" <<EOF
Host hadoop-master $master_ip
    HostName $master_ip
    ProxyCommand none
Host *
    User $SSH_USER
    IdentityFile "$SSH_KEY"
    IdentitiesOnly yes
    UserKnownHostsFile "$runtime/known_hosts"
    StrictHostKeyChecking accept-new
    LogLevel ERROR
    ProxyCommand ssh -F "$ssh_config" -W %h:%p hadoop-master
    ConnectTimeout 10
EOF
    chmod 600 "$ssh_config"

    # Refrescar este inventario permite detectar el cliente cuando Ansible lo cree.
    inventory_file="$runtime/progress.aws_ec2.yml"
    cat > "$inventory_file" <<EOF
plugin: amazon.aws.aws_ec2
regions: [$AWS_REGION]
filters:
  tag:Group: [hadoop-master, hadoop-worker, hadoop-client]
  instance-state-name: running
compose:
  ansible_host: public_ip_address if tags.Group == 'hadoop-master' else private_ip_address
EOF
    progress_command="$(cat <<'SH'
role={{ tags.Group | quote }}
package=Hadoop; total=696; target="$HOME/hadoop-3.3.6.tar.gz"
if [ "$role" = hadoop-client ]; then
    package=Spark; total=382; target=/tmp/spark-3.5.5.tgz
elif [ "$role" = hadoop-master ] && [ -f "$target" ]; then
    package=Spark; total=382; target="$HOME/spark-3.5.5-bin-hadoop3.tgz"
fi
if [ -f "$target" ]; then
    bytes=$(stat -c%s "$target")
    complete=1
else
    bytes=$(find "$HOME/.ansible/tmp" -type f -name 'tmp*' -printf '%s\n' 2>/dev/null | sort -nr | head -n 1)
    complete=0
fi
awk -v name={{ tags.Name | default(inventory_hostname) | quote }} -v package="$package" -v total="$total" -v bytes="${bytes:-0}" -v complete="$complete" 'BEGIN {
    mib=bytes/1048576; percent=int(100*mib/total)
    if(complete) percent=100; else if(percent>99) percent=99
    printf "%-18s %-6s [", name, package
    for(i=0;i<20;i++) printf "%s", (i<int(percent/5)?"#":"-")
    printf "] %3d%%  %6.1f / ~%d MiB%s", percent, mib, total, (complete?" descargado":"")
}'
SH
)"
    trap 'exit 0' INT TERM
    while true; do
        if [[ -t 1 ]]; then printf '\033[2J\033[H'; fi
        printf 'Descargas de Hadoop y Spark (avance aproximado). Ctrl+C cierra el visor.\n\n'
        if ! "$runtime/venv/bin/ansible" all -i "$inventory_file" --user "$SSH_USER" \
            --private-key "$SSH_KEY" --ssh-common-args "-F \"$ssh_config\"" -m raw -a "$progress_command" -o |
            sed -E 's/^.* \| (CHANGED|SUCCESS) \| rc=0 \| \(stdout\) //' | sort; then
            printf 'No se pudo actualizar todo el progreso; se reintentará.\n'
        fi
        sleep 10
    done
}

case "${1:---help}" in
    -h|--help) usage; exit 0 ;;
    deploy) shift ;;
    progress) [[ $# -eq 1 ]] || die "progress no acepta argumentos."; show_progress; exit 0 ;;
    *) usage >&2; exit 1 ;;
esac
[[ $# -eq 0 ]] || die "Argumento desconocido. Consulta --help."

# Conservar también los errores iniciales, sin borrar los intentos anteriores.
runtime_dir="$project_root/.deployment"
log_file="$runtime_dir/deploy.log"
mkdir -p "$runtime_dir"
touch "$log_file"
chmod 600 "$log_file"
# Mostrar y guardar toda la ejecución; pipefail conserva el código de error.
{
    printf '\nDespliegue iniciado: %s\nRegistro: %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$log_file"

    config_file="${CLUSTER_CONFIG:-$project_root/config/cluster.env}"
    [[ -f "$config_file" ]] || die "Copia config/cluster.env.example a config/cluster.env."
    # shellcheck source=../config/cluster.env.example
    source "$config_file"
    : "${AWS_REGION:?Define AWS_REGION}"
    : "${AWS_PROFILE:?Define AWS_PROFILE}"
    : "${SSH_KEY:?Define SSH_KEY}"
    : "${SSH_USER:?Define SSH_USER}"
    : "${REMOTE_DIR:?Define REMOTE_DIR}"
    for dependency in aws git ssh tar; do
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
} 2>&1 | tee -a "$log_file"
