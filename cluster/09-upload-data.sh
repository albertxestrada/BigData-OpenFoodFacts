#!/usr/bin/env bash
# Paso 9 (master): descarga el CSV de Open Food Facts, lo sube a HDFS y crea una muestra de 100.000 filas.
# Uso: nohup ./cluster/09-upload-data.sh > upload.log 2>&1 &    y para ver el avance: tail -f --pid=$! upload.log
#      (--pid=$! cierra el tail cuando termina el script, así la terminal vuelve sola)
set -euo pipefail
export JAVA_HOME=/usr/lib/jvm/java
HDFS=~/hadoop-3.3.6/bin/hdfs
URL=https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz

# Crear las carpetas en HDFS
$HDFS dfs -mkdir -p /data/raw /data/clean /results

# Guardar la fecha de descarga (el CSV cambia cada noche)
date -u > ~/data-version.txt

# Descargar, descomprimir y subir a HDFS sin pasar por el disco (descomprimido para que Spark lo reparta)
curl -sL "$URL" \
  | gunzip \
  | $HDFS dfs -put -f - /data/raw/openfoodfacts.csv

# Crear una muestra con el 2 % de las filas al azar (las primeras filas son casi todas de EE. UU.)
$HDFS dfs -cat /data/raw/openfoodfacts.csv \
  | awk 'NR == 1 || rand() < 0.02' \
  | $HDFS dfs -put -f - /data/raw/muestra.csv

# Comprobar tamaños. Columnas: tamaño del fichero y espacio en disco con réplicas.
# El playbook de Ansible no fija dfs.replication, así que vale 3 (por defecto de Hadoop): 13 G ocupan 39 G.
$HDFS dfs -du -h /data/raw
echo "Esperado: openfoodfacts.csv unos 13 G (39 G con las 3 réplicas) y muestra.csv unos 270 M"
