#!/usr/bin/env bash
# Paso 8 (master): copia al client los trabajos de Spark y su configuración, y ejecuta allí la instalación de Spark.
# Uso: ./cluster/08-setup-client.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Solo lo que se usa en el client (rutas desde la raíz del proyecto)
FILES=(
  run.sh                            # lanza los trabajos
  jobs                              # trabajos de Spark
  cluster/client-install-spark.sh   # instala Spark
  cluster/conf/client               # configuración de Spark, HDFS y YARN
)

# Borrar la copia anterior en el client
ssh hadoop-client "rm -rf open-food-facts-analytics && mkdir open-food-facts-analytics"

# Copiar esa lista conservando las carpetas
tar -cf - "${FILES[@]}" \
  | ssh hadoop-client "tar -C open-food-facts-analytics -xf -"

# Instalar y configurar Spark en el client
ssh hadoop-client "bash open-food-facts-analytics/cluster/client-install-spark.sh"

echo
echo "Siguiente, si el CSV aún no está en HDFS: ./cluster/09-upload-data.sh"
