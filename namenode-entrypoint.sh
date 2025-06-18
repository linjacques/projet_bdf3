#!/bin/bash

/entrypoint.sh echo "Generating Hadoop config..."

if [ ! -d /hadoop/dfs/name/current ]; then
  echo "Formatting HDFS..."
  /entrypoint.sh hdfs namenode -format -force -nonInteractive
fi

echo "Starting NameNode..."
exec /entrypoint.sh hdfs namenode
