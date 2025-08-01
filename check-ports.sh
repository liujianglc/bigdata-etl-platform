#!/bin/bash

# Check if ports are already in use
echo "Checking port availability..."

PORTS=(8080 8081 9870 3306 9083 7077 9000 6379)
PORT_NAMES=("Airflow Webserver" "Spark UI" "HDFS UI" "MySQL" "Hive Metastore" "Spark Master" "HDFS Namenode" "Redis")

for i in "${!PORTS[@]}"; do
    PORT=${PORTS[$i]}
    NAME=${PORT_NAMES[$i]}
    
    if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null ; then
        echo "❌ Port $PORT ($NAME) is already in use"
        echo "   Process using port $PORT:"
        lsof -Pi :$PORT -sTCP:LISTEN
        echo ""
    else
        echo "✅ Port $PORT ($NAME) is available"
    fi
done

echo ""
echo "If any ports are in use, modify the corresponding variables in .env file:"
echo "  AIRFLOW_WEBSERVER_PORT=8080"
echo "  SPARK_UI_PORT=8081"
echo "  HDFS_UI_PORT=9870"
echo "  MYSQL_PORT=3306"
echo "  HIVE_METASTORE_PORT=9083"
echo "  SPARK_MASTER_PORT=7077"
echo "  HDFS_NAMENODE_PORT=9000"
echo "  REDIS_PORT=6379"