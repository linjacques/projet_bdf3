#!/bin/bash

# Lancer le entrypoint d'origine en fond
/docker-entrypoint.sh &

# Attendre que HDFS soit prêt
echo "⏳ Attente du démarrage de HDFS..."
sleep 10

# Créer le répertoire de logs YARN
hdfs dfs -mkdir -p /tmp/logs
hdfs dfs -chown -R yarn:hadoop /tmp/logs
hdfs dfs -chmod -R 1777 /tmp/logs

# Attendre que le processus principal se termine
wait
