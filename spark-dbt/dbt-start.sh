#!/bin/bash
set -e

# 1. Setup folders and permissions for dbt
mkdir -p /opt/dbt/ecommerce_analytics/logs
chmod -R 777 /opt/dbt/ecommerce_analytics

# 2. Start Spark Master
# We call the Master class directly to bypass the base image entrypoint logic
echo "Starting Spark Master for DBT..."
exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0