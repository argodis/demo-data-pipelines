#!/bin/bash

#RAY PORT
RAY_PORT=9339
REDIS_PASS="d4t4bricks"
STORAGE_PATH="/dbfs/mnt/data/alpaca/workflows"

# install ray
/databricks/python/bin/pip install ray pyarrow alpaca-trade-api

# Install additional ray libraries
/databricks/python/bin/pip install ray[default]

# If starting on the Spark driver node, initialize the Ray head node
# If starting on the Spark worker node, connect to the head Ray node
if [ ! -z $DB_IS_DRIVER ] && [ $DB_IS_DRIVER = TRUE ] ; then
  echo "Starting the head node"
  ray start  --min-worker-port=20000 --max-worker-port=25000 --temp-dir="/tmp/ray" --head --port=$RAY_PORT --redis-password="$REDIS_PASS"  --include-dashboard=false --metrics-export-port=8080 --storage="${STORAGE_PATH}"
  
  cd /tmp
  /usr/bin/curl -LO https://github.com/prometheus/prometheus/releases/download/v2.32.1/prometheus-2.32.1.linux-amd64.tar.gz
  /usr/bin/tar xf prometheus-2.32.1.linux-amd64.tar.gz
  nohup /tmp/prometheus-2.32.1.linux-amd64/prometheus --config.file="/dbfs/Users/david@argodis.de/init/prometheus.yaml" --storage.tsdb.path="/tmp/prometheus/${DB_CLUSTER_ID}/" &
else
  sleep 120
  echo "Starting the non-head node - connecting to $DB_DRIVER_IP:$RAY_PORT"
  ray start  --min-worker-port=20000 --max-worker-port=25000 --temp-dir="/tmp/ray" --address="$DB_DRIVER_IP:$RAY_PORT" --redis-password="$REDIS_PASS" --storage="${STORAGE_PATH}"
fi
