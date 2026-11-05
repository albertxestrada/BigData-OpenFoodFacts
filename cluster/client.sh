#!/usr/bin/env bash
# En el client: instala Java y Spark apuntando a YARN y HDFS.
set -euo pipefail
V=3.5.3

if [ -x /opt/spark/bin/spark-submit ]; then
  echo "Spark ya instalado en el client"
  exit 0
fi

sudo dnf install -y -q java-1.8.0-amazon-corretto-devel python3 wget tar

# Descargar y descomprimir Spark
[ -f /tmp/spark-$V.tgz ] || wget -q -O /tmp/spark-$V.tgz https://archive.apache.org/dist/spark/spark-$V/spark-$V-bin-hadoop3.tgz
sudo rm -rf /opt/spark
sudo mkdir /opt/spark
sudo chown ec2-user: /opt/spark
tar -xzf /tmp/spark-$V.tgz -C /opt/spark --strip-components=1

# Recursos: 3 executors de 3 GB, uno por worker
cat > /opt/spark/conf/spark-defaults.conf <<'CONF'
spark.master                   yarn
spark.submit.deployMode        client
spark.driver.memory            2g
spark.executor.instances       3
spark.executor.cores           2
spark.executor.memory          3g
spark.executor.memoryOverhead  512m
CONF

# Variables de entorno
VARS='export JAVA_HOME=/usr/lib/jvm/java-1.8.0-amazon-corretto
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=$HOME/hadoop_conf
export YARN_CONF_DIR=$HOME/hadoop_conf
export PATH=$PATH:/opt/spark/bin'
grep -q SPARK_HOME ~/.bashrc || echo "$VARS" >> ~/.bashrc
eval "$VARS"

# Probar con SparkPi
spark-submit /opt/spark/examples/src/main/python/pi.py 10 2> /dev/null
echo "Esperado: Pi is roughly 3.14"
