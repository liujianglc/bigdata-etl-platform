#!/bin/bash

# Check if ports are already in use
echo "Checking port availability..."

PORTS=(8080 8081 9870 3306 9083 7077 9000 6379)
PORT_NAMES=("Airflow Webserver" "Spark UI" "HDFS UI" "MySQL" "Hive Metastore" "Spark Master" "HDFS Namenode" "Redis")

check_port() {
    local port=$1
    local name=$2
    
    # Try multiple methods to check port availability
    if command -v ss >/dev/null 2>&1; then
        # Use ss (modern replacement for netstat)
        if ss -tuln | grep -q ":$port "; then
            echo "❌ Port $port ($name) is already in use"
            echo "   Process details:"
            ss -tulnp | grep ":$port "
            echo ""
            return 1
        fi
    elif command -v netstat >/dev/null 2>&1; then
        # Use netstat if available
        if netstat -tuln | grep -q ":$port "; then
            echo "❌ Port $port ($name) is already in use"
            echo "   Process details:"
            netstat -tulnp | grep ":$port " 2>/dev/null || netstat -tuln | grep ":$port "
            echo ""
            return 1
        fi
    elif command -v lsof >/dev/null 2>&1; then
        # Use lsof if available
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            echo "❌ Port $port ($name) is already in use"
            echo "   Process details:"
            lsof -Pi :$port -sTCP:LISTEN
            echo ""
            return 1
        fi
    else
        # Fallback: try to bind to the port
        if timeout 1 bash -c "</dev/tcp/localhost/$port" 2>/dev/null; then
            echo "❌ Port $port ($name) is already in use"
            echo "   (Unable to get process details - install ss, netstat, or lsof for more info)"
            echo ""
            return 1
        fi
    fi
    
    echo "✅ Port $port ($name) is available"
    return 0
}

for i in "${!PORTS[@]}"; do
    PORT=${PORTS[$i]}
    NAME=${PORT_NAMES[$i]}
    check_port $PORT "$NAME"
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