#!/usr/bin/env bash
# En el client: lanza un job. Uso: ./run.sh jobs/q01-nutriscore-by-category.py
mkdir -p logs
name=$(basename "$1" .py)
echo "======== $name ========"

# El log de Spark va a logs/; si falla, se muestra el final
if spark-submit "$@" 2> "logs/$name.log"; then
  echo "OK $name"
else
  echo "ERROR $name. Últimas líneas de logs/$name.log:"
  tail -n 20 "logs/$name.log"
fi
