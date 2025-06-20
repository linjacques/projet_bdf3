#!/bin/bash

/entrypoint.sh echo "Generating Hadoop config..."

if [ ! -d /hadoop/dfs/name/current ]; then
  echo "Formatting HDFS..."
  /entrypoint.sh hdfs namenode -format -force -nonInteractive
fi

echo "Starting NameNode..."
/entrypoint.sh hdfs namenode &

echo "⏳ Attente du démarrage de HDFS..."
sleep 10

echo "📁 Création de /tmp/logs dans HDFS"
hdfs dfs -mkdir -p /tmp/logs || echo "❌ Échec mkdir"
hdfs dfs -chown -R yarn:hadoop /tmp/logs || echo "❌ Échec chown"
hdfs dfs -chmod -R 1777 /tmp/logs || echo "❌ Échec chmod"

wait
