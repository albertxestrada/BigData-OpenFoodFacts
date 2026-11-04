#!/usr/bin/env bash
# Local, una sola vez: instala Java 17 y un entorno de Python con PySpark, pandas y seaborn. Vale para WSL y Mac.
# Uso: ./tests/setup-local-test.sh
set -euo pipefail

# Java 17: en WSL con apt, en Mac con Homebrew (https://brew.sh)
if [ "$(uname)" = "Linux" ]; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq \
    openjdk-17-jre-headless \
    python3-venv
else
  brew install openjdk@17
fi

# Crear el entorno de Python ~/off-venv con las librerías de la prueba y de las gráficas
python3 -m venv ~/off-venv
source ~/off-venv/bin/activate
pip install -q pyspark==3.5.3 pandas seaborn

echo
echo "Listo. Siguiente: ./tests/local-test.sh"
