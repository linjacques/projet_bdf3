#!/bin/bash

echo "⏳ Waiting for HDFS at namenode:8020..."
until hdfs dfs -ls / > /dev/null 2>&1; do
  sleep 2
done

echo "✅ HDFS is available, starting HiveServer2..."

# Démarrer metastore + hiveserver2
/opt/hive/bin/hive --service metastore &
exec /opt/hive/bin/hive --service hiveserver2
