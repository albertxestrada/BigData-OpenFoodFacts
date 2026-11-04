#!/usr/bin/env bash
# Local: ejecuta el ETL, las preguntas y las gráficas en tu equipo con pocos datos, sin clúster. WSL y Mac.
# Requisito: Java 17 y el entorno ~/off-venv con pyspark, pandas y seaborn
# Uso: ./tests/local-test.sh               (con tests/sample-1k.csv)
#      ./tests/local-test.sh muestra.csv   (con otra muestra)
set -euo pipefail
cd "$(dirname "$0")/.."
L="$PWD/local"

# Activar el entorno de Python ~/off-venv
source ~/off-venv/bin/activate

# En Mac, decir a Spark dónde está el Java de Homebrew
if [ "$(uname)" = "Darwin" ]; then
  export JAVA_HOME="$(brew --prefix openjdk@17)"
fi

# Empezar con la carpeta local/ limpia
rm -rf "$L"
mkdir -p "$L/data/raw" "$L/jobs" "$L/logs"

# Copiar los datos de prueba con el mismo nombre que en HDFS
cp "${1:-tests/sample-1k.csv}" "$L/data/raw/openfoodfacts.csv"
echo "Datos: $(($(wc -l < "$L/data/raw/openfoodfacts.csv") - 1)) filas"

# Copiar los scripts cambiando las rutas de HDFS por local/ y bajando el mínimo de productos a 5
for f in jobs/*.py; do
  sed -e "s#hdfs:///#file://$L/#g" \
      -e 's/^MIN_PRODUCTS = [0-9]*/MIN_PRODUCTS = 5/' \
      "$f" > "$L/jobs/$(basename "$f")"
done

# Ejecutar el ETL y las preguntas con Spark en local; si falla uno, enseñar su log y parar
for f in "$L"/jobs/*.py; do
  name=$(basename "$f" .py)
  echo
  echo "======== $name ========"
  if python3 "$f" > "$L/logs/$name.out" 2> "$L/logs/$name.log"; then
    head -n 14 "$L/logs/$name.out"
    echo "OK $name"
  else
    echo "ERROR $name. Últimas líneas de local/logs/$name.log:"
    tail -n 20 "$L/logs/$name.log"
    exit 1
  fi
done

# Generar las gráficas
echo
python3 viz/plots.py "$L/results" "$L/figures"
echo
echo "Resultados en local/results/ y gráficas en local/figures/"
