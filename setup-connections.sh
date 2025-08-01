#!/bin/bash

echo "Setting up Airflow connections..."

# 使用Python API设置连接
echo "使用Python API设置连接..."
docker-compose exec airflow-webserver python /opt/airflow/setup-connections-python.py

# 备用方案：使用CLI命令（如果Python API失败）
if [ $? -ne 0 ]; then
    echo "Python API失败，尝试使用CLI命令..."
    
    # MySQL connection
    echo "添加MySQL连接..."
    docker-compose exec airflow-webserver airflow connections add mysql_default \
        --conn-type mysql \
        --conn-host mysql \
        --conn-port 3306 \
        --conn-login etl_user \
        --conn-password etl_pass \
        --conn-schema source_db || echo "MySQL连接设置失败，请手动在UI中添加"

    # Spark connection
    echo "添加Spark连接..."
    docker-compose exec airflow-webserver airflow connections add spark_default \
        --conn-type spark \
        --conn-host spark-master \
        --conn-port 7077 || echo "Spark连接设置失败，请手动在UI中添加"

    # Hive connection
    echo "添加Hive连接..."
    docker-compose exec airflow-webserver airflow connections add hive_default \
        --conn-type hive_cli \
        --conn-host hive-metastore \
        --conn-port 9083 || echo "Hive连接设置失败，请手动在UI中添加"
fi

echo ""
echo "连接设置完成！"
echo ""
echo "如果自动设置失败，请手动在Airflow UI中添加连接："
echo "1. 访问 http://localhost:8080"
echo "2. 进入 Admin -> Connections"
echo "3. 添加以下连接："
echo ""
echo "MySQL连接："
echo "  - Conn Id: mysql_default"
echo "  - Conn Type: MySQL"
echo "  - Host: mysql"
echo "  - Port: 3306"
echo "  - Login: etl_user"
echo "  - Password: etl_pass"
echo "  - Schema: source_db"
echo ""
echo "Spark连接："
echo "  - Conn Id: spark_default"
echo "  - Conn Type: Spark"
echo "  - Host: spark-master"
echo "  - Port: 7077"