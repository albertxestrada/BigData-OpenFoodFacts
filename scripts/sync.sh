#!/usr/bin/env bash
# Copia cleaning al master y analysis al cliente para desarrollar las preguntas.
# Uso: ./scripts/sync.sh master | client | all
# ./scripts/sync.sh master: sincroniza la carpeta cleaning con el nodo master
# ./scripts/sync.sh client: sincroniza la carpeta analysis con el nodo client
# ./scripts/sync.sh all: sincroniza ambas carpetas con sus respectivos nodos
set -euo pipefail

die() { printf 'Error: %s\n' "$*" >&2; exit 1; }

[[ $# -eq 1 ]] || die "Indica un destino: master, client o all."
target="$1"
case "$target" in
    master|client|all) ;;
    *) die "Destino inválido: $target. Usa master, client o all." ;;
esac

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
config_file="${CLUSTER_CONFIG:-$project_root/config/cluster.env}"
[[ -f "$config_file" ]] || die "Crea config/cluster.env copiando config/cluster.env.example."
# shellcheck source=../config/cluster.env.example
source "$config_file"
: "${AWS_REGION:?Define AWS_REGION en la configuración}"
: "${AWS_PROFILE:?Define AWS_PROFILE en la configuración}"
: "${SSH_KEY:?Define SSH_KEY en la configuración}"
: "${SSH_USER:?Define SSH_USER en la configuración}"
: "${REMOTE_DIR:?Define REMOTE_DIR en la configuración}"
for dependency in aws ssh rsync; do
    command -v "$dependency" >/dev/null || die "Falta $dependency en tu ordenador. Consulta el README."
done
[[ -f "$SSH_KEY" ]] || die "No se encuentra la clave SSH: $SSH_KEY"
[[ -d "$project_root/src" ]] || die "No se encuentra la carpeta src/ del proyecto."
# Validar rutas y usuario.
[[ "$REMOTE_DIR" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || die "REMOTE_DIR debe ser un nombre de carpeta, sin espacios ni barras."
[[ "$SSH_USER" =~ ^[a-zA-Z0-9_-]+$ ]] || die "SSH_USER no es un nombre de usuario válido."
case "$SSH_KEY" in
    *\"*|*$'\n'*) die "La ruta de SSH_KEY no puede contener comillas dobles ni saltos de línea." ;;
esac

# Consultar la IP de una instancia activa.
get_address() {
    local name="$1" field="$2" row instance_id state address
    local filters=("Name=tag:Name,Values=$name" 'Name=instance-state-name,Values=pending,running,stopping,stopped')
    row="$(AWS_CLI_AUTO_PROMPT=off aws --region "$AWS_REGION" --profile "$AWS_PROFILE" \
        --no-cli-pager ec2 describe-instances --filters "${filters[@]}" \
        --query "Reservations[].Instances[].[InstanceId,State.Name,$field]" --output text)"
    [[ -n "$row" ]] || die "No se encuentra $name. Revisa EC2 y tu configuración."
    [[ "$row" != *$'\n'* ]] || die "Hay varios $name. Corrige sus etiquetas antes de continuar."
    read -r instance_id state address <<< "$row"
    [[ "$state" == running ]] || die "$name está $state. Arranca el clúster antes de sincronizar."
    [[ -n "$address" && "$address" != None ]] || die "$name no tiene $field disponible."
    printf '%s\n' "$address"
}

master_ip="$(get_address hadoop-master PublicIpAddress)"
client_ip=''
nodes=(master)
if [[ "$target" != master ]]; then
    client_ip="$(get_address hadoop-client PrivateIpAddress)"
    if [[ "$target" == client ]]; then nodes=(client); else nodes=(master client); fi
fi

# SSH temporal; cliente vía master.
temporary_dir="$(mktemp -d)"
trap 'rm -rf -- "$temporary_dir"' EXIT
ssh_config="$temporary_dir/ssh_config"
cat > "$ssh_config" <<EOF
Host master client
    User $SSH_USER
    IdentityFile "$SSH_KEY"
    IdentitiesOnly yes
    ConnectTimeout 10
Host master
    HostName $master_ip
EOF
if [[ -n "$client_ip" ]]; then
    cat >> "$ssh_config" <<EOF
Host client
    HostName $client_ip
    ProxyJump master
EOF
fi
chmod 600 "$ssh_config"

for node in "${nodes[@]}"; do
    folder=cleaning
    if [[ "$node" == client ]]; then folder=analysis; fi
    [[ -d "$project_root/src/$folder" ]] || die "No se encuentra src/$folder/ en el proyecto."
    printf '\nSincronizando src/%s/ con %s (carpeta ~/%s/src/%s/)...\n' "$folder" "$node" "$REMOTE_DIR" "$folder"
    if ! ssh -F "$ssh_config" "$node" 'command -v rsync >/dev/null'; then
        die "No se pudo conectar con $node o falta rsync allí. Consulta la preparación del README."
    fi
    ssh -F "$ssh_config" "$node" "mkdir -p -- '$REMOTE_DIR/src'"
    # Copiar la carpeta sin borrar archivos remotos.
    rsync -av --itemize-changes \
        --exclude='__pycache__/' --exclude='*.pyc' --exclude='.venv/' \
        --exclude='*.pem' --exclude='.env*' --exclude='*.csv*' --exclude='*.parquet' \
        -e "ssh -F \"$ssh_config\"" "$project_root/src/$folder" "$node:$REMOTE_DIR/src/"
done
printf '\nSincronización terminada.\n'
