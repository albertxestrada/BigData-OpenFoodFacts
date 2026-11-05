#!/usr/bin/env bash
# En el master. Uso: ./cluster/master.sh setup | data | check
set -euo pipefail
cd "$(dirname "$0")/.."
export JAVA_HOME=/usr/lib/jvm/java
HADOOP=~/hadoop-3.3.6
WORKERS="hadoop-worker-1 hadoop-worker-2 hadoop-worker-3"

# Configura SSH, /etc/hosts, YARN y el client
setup() {
  # SSH a los nodos por nombre
  chmod 600 ~/.ssh/vockey.pem
  printf 'Host hadoop-*\n  User ec2-user\n  IdentityFile ~/.ssh/vockey.pem\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n' > ~/.ssh/config
  chmod 600 ~/.ssh/config

  # Añadir el client al /etc/hosts del master
  if ! grep -q hadoop-client /etc/hosts; then
    echo | sudo tee -a /etc/hosts > /dev/null
    echo "$(cat client-ip.txt)  hadoop-client" | sudo tee -a /etc/hosts
  fi

  # El client conoce master y workers, pero no su propio alias (Spark se anuncia con el nombre de EC2)
  grep -v hadoop-client /etc/hosts | ssh hadoop-client "sudo tee /etc/hosts > /dev/null"

  # YARN con 6 GB por worker (solo si cambia)
  for w in $WORKERS; do
    if ! ssh "$w" cat hadoop-3.3.6/etc/hadoop/yarn-site.xml | cmp -s - cluster/worker-yarn-site.xml; then
      scp -q cluster/worker-yarn-site.xml "$w:hadoop-3.3.6/etc/hadoop/yarn-site.xml"
      ssh "$w" "sudo systemctl restart nodemanager"
      echo "YARN configurado en $w"
    fi
  done

  # Client: jobs, config de HDFS y YARN, y Spark
  ssh hadoop-client "rm -rf open-food-facts-analytics hadoop_conf && mkdir open-food-facts-analytics hadoop_conf"
  tar -cf - run.sh jobs cluster/client.sh | ssh hadoop-client "tar -C open-food-facts-analytics -xf -"
  scp -q $HADOOP/etc/hadoop/core-site.xml $HADOOP/etc/hadoop/yarn-site.xml hadoop-client:hadoop_conf/
  ssh hadoop-client "bash open-food-facts-analytics/cluster/client.sh"

  echo
  check
}

# Sube el CSV a HDFS y crea una muestra del 2 %
data() {
  HDFS=$HADOOP/bin/hdfs

  # La muestra se crea la última: si existe, ya está todo
  if $HDFS dfs -test -e /data/raw/muestra.csv; then
    echo "Los datos ya están en HDFS (descargados el $(cat ~/data-version.txt))"
    return
  fi

  $HDFS dfs -mkdir -p /data/raw /data/clean /results
  date -u > ~/data-version.txt   # fecha de descarga

  # Descargar y subir sin pasar por el disco
  curl -sL https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz \
    | gunzip | $HDFS dfs -put -f - /data/raw/openfoodfacts.csv

  # Muestra al azar del 2 %
  $HDFS dfs -cat /data/raw/openfoodfacts.csv \
    | awk 'NR == 1 || rand() < 0.02' | $HDFS dfs -put -f - /data/raw/muestra.csv

  $HDFS dfs -du -h /data/raw
  echo "Esperado: openfoodfacts.csv unos 13 G (39 G con réplicas) y muestra.csv unos 270 M"
}

# Comprueba procesos y nodos vivos
check() {
  for node in hadoop-master $WORKERS; do
    echo "== $node"
    ssh "$node" /usr/lib/jvm/java/bin/jps
  done
  echo "Esperado: master con NameNode y ResourceManager; workers con DataNode y NodeManager"
  echo
  $HADOOP/bin/hdfs dfsadmin -report 2> /dev/null | grep "Live datanodes"
  $HADOOP/bin/yarn node -list -showDetails 2> /dev/null | grep -E "Total Nodes|Configured Resources"
  echo "Esperado: Live datanodes (3), Total Nodes:3 y 3 veces <memory:6144, vCores:2>"
}

case "${1:-}" in
  setup | data | check) "$1" ;;
  *) echo "Uso: ./cluster/master.sh setup | data | check"; exit 1 ;;
esac
