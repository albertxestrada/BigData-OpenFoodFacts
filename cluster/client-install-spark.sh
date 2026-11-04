#!/usr/bin/env bash
# Client: instala Java 8 y Spark y lo configura para usar YARN y HDFS.
set -euo pipefail
cd "$(dirname "$0")"
V=3.5.3

# Instalar Java 8 y herramientas
sudo dnf install -y -q \
  java-1.8.0-amazon-corretto-devel \
  python3 \
  wget \
  tar

# Descargar Spark (solo la primera vez)
if [ ! -f /tmp/spark-$V-bin-hadoop3.tgz ]; then
  wget -q -P /tmp https://archive.apache.org/dist/spark/spark-$V/spark-$V-bin-hadoop3.tgz
fi

# Descomprimir Spark en /opt/spark
tar -xzf /tmp/spark-$V-bin-hadoop3.tgz -C /tmp
sudo rm -rf /opt/spark
sudo mv /tmp/spark-$V-bin-hadoop3 /opt/spark
sudo chown -R ec2-user:ec2-user /opt/spark

# Copiar core-site y yarn-site: le dicen a Spark dónde están HDFS y YARN
mkdir -p ~/hadoop_conf
cp conf/client/core-site.xml ~/hadoop_conf/
cp conf/client/yarn-site.xml ~/hadoop_conf/

# Copiar los recursos por defecto de cada trabajo (3 executors de 2 núcleos y 3 GB)
cp conf/client/spark-defaults.conf /opt/spark/conf/

# Añadir las variables de entorno a ~/.bashrc si no están
if ! grep -q SPARK_HOME ~/.bashrc; then
  cat conf/client/bashrc-vars >> ~/.bashrc
fi

# Probar con SparkPi en YARN
source conf/client/bashrc-vars
spark-submit $SPARK_HOME/examples/src/main/python/pi.py 10 2> /dev/null
echo "Esperado: Pi is roughly 3.14"
