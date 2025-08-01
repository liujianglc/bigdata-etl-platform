#!/bin/bash

echo "启动所有大数据服务..."

echo "=== 检查当前服务状态 ==="
docker-compose ps

echo ""
echo "=== 第1步：确保基础服务运行 ==="
docker-compose up -d postgres redis mysql

echo "等待基础服务启动（20秒）..."
sleep 20

echo ""
echo "=== 第2步：启动HDFS服务 ==="
docker-compose up -d namenode
sleep 15
docker-compose up -d datanode
sleep 10

echo ""
echo "=== 第3步：启动Hive Metastore ==="
docker-compose up -d hive-metastore
sleep 20

echo ""
echo "=== 第4步：启动Spark服务 ==="
docker-compose up -d spark-master
sleep 15
docker-compose up -d spark-worker
sleep 10

echo ""
echo "=== 第5步：启动Airflow服务 ==="
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker

echo ""
echo "=== 等待所有服务完全启动（30秒）==="
sleep 30

echo ""
echo "=== 检查最终服务状态 ==="
docker-compose ps

echo ""
echo "=== 检查关键服务健康状态 ==="
services=("postgres" "mysql" "namenode" "hive-metastore" "spark-master" "airflow-webserver")

for service in "${services[@]}"; do
    if docker-compose ps $service | grep -q "Up"; then
        echo "✅ $service: 运行中"
    else
        echo "❌ $service: 未运行"
        echo "   查看日志: docker-compose logs $service"
    fi
done

echo ""
echo "=== 服务启动完成！==="
echo ""
echo "可用的Web界面："
echo "- Airflow: http://localhost:8080 (admin/admin)"
echo "- Spark Master: http://localhost:8081"
echo "- HDFS NameNode: http://localhost:9870"
echo ""
echo "现在可以运行: ./create-real-hive-data.sh"