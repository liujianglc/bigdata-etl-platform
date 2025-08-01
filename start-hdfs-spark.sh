#!/bin/bash

echo "启动HDFS和Spark服务（需要先运行start-basic.sh）..."

# 检查基础服务是否运行
if ! docker-compose ps | grep -q "airflow-webserver.*Up"; then
    echo "错误：请先运行 ./start-basic.sh 启动基础服务"
    exit 1
fi

echo "=== 第1步：启动HDFS服务 ==="
docker-compose up -d namenode

echo "等待NameNode启动（30秒）..."
sleep 30

docker-compose up -d datanode

echo "等待DataNode启动（20秒）..."
sleep 20

echo "=== 第2步：启动Hive Metastore ==="
docker-compose up -d hive-metastore

echo "等待Hive Metastore启动（30秒）..."
sleep 30

echo "=== 第3步：启动Spark集群 ==="
docker-compose up -d spark-master

echo "等待Spark Master启动（20秒）..."
sleep 20

docker-compose up -d spark-worker

echo "等待Spark Worker启动（20秒）..."
sleep 20

echo "=== 第4步：初始化HDFS目录 ==="
docker-compose exec namenode hdfs dfs -mkdir -p /user/hive/warehouse || true
docker-compose exec namenode hdfs dfs -mkdir -p /tmp/spark-events || true
docker-compose exec namenode hdfs dfs -chmod -R 777 /user/hive/warehouse || true
docker-compose exec namenode hdfs dfs -chmod -R 777 /tmp || true

echo "=== 完整大数据环境启动完成！==="
echo ""
echo "所有服务访问地址："
echo "- Airflow Web UI: http://localhost:8080 (admin/admin)"
echo "- Spark Master UI: http://localhost:8081"
echo "- HDFS NameNode UI: http://localhost:9870"
echo "- MySQL: localhost:3306 (root/rootpass)"
echo ""
echo "现在可以运行完整的大数据DAG："
echo "- complete_bigdata_etl"
echo "- mysql_to_hive_etl"
echo ""
echo "运行测试："
echo "./test-bigdata.sh"