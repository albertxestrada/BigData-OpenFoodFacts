#!/usr/bin/env bash
# Master: comprueba que cada proceso está en su nodo y que HDFS y YARN ven los 3 workers.
# Uso: ./cluster/check-cluster.sh
export JAVA_HOME=/usr/lib/jvm/java

# Listar los procesos Java de cada nodo
for node in hadoop-master hadoop-worker-1 hadoop-worker-2 hadoop-worker-3; do
  echo "== $node"
  ssh "$node" /usr/lib/jvm/java/bin/jps
done
echo "Esperado: master con NameNode y ResourceManager, workers con DataNode y NodeManager"

# Ver cuántos DataNodes ve HDFS
echo
~/hadoop-3.3.6/bin/hdfs dfsadmin -report 2> /dev/null \
  | grep "Live datanodes"

# Ver cuántos NodeManagers ve YARN y su memoria
~/hadoop-3.3.6/bin/yarn node -list -showDetails 2> /dev/null \
  | grep -E "Total Nodes|Configured Resources"

echo "Esperado: Live datanodes (3), Total Nodes:3 y 3 veces <memory:6144, vCores:2>"
