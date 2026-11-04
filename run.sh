#!/usr/bin/env bash
# Client: lanza un script de Spark en YARN. Muestra solo el resultado; el log de Spark va a logs/.
# Uso: ./run.sh jobs/q01-nutriscore-by-category.py
#      ./run.sh jobs/00-etl.py hdfs:///data/raw/muestra.csv

mkdir -p logs
name=$(basename "$1" .py)
echo "======== $name ========"

# Ejecutar; si falla, enseñar el final del log
if spark-submit "$@" 2> "logs/$name.log"; then
  echo "OK $name"
else
  echo "ERROR $name. Últimas líneas de logs/$name.log:"
  tail -n 20 "logs/$name.log"
fi
