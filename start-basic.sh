#!/bin/bash

echo "启动基础大数据环境（逐步启动）..."

# 停止所有服务
echo "停止现有服务..."
docker-compose down

# 清理
echo "清理Docker资源..."
docker system prune -f

# 创建必要目录
mkdir -p logs config plugins dags spark_jobs

echo "=== 第1步：启动基础数据服务 ==="
docker-compose up -d postgres mysql redis

echo "等待基础服务启动（30秒）..."
sleep 30

# 检查基础服务状态
echo "检查基础服务状态..."
docker-compose ps postgres mysql redis

echo "=== 第2步：初始化Airflow ==="
# 初始化Airflow数据库
docker-compose run --rm airflow-webserver airflow db init

# 创建管理员用户
docker-compose run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "=== 第3步：启动Airflow服务 ==="
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker

echo "等待Airflow服务启动（30秒）..."
sleep 30

echo "=== 第4步：初始化MySQL数据 ==="
docker-compose exec -T mysql mysql -u root -prootpass source_db < init-mysql-data.sql

echo "=== 第5步：设置Airflow连接 ==="
./setup-connections.sh

echo "=== 基础环境启动完成！==="
echo ""
echo "可用服务："
echo "- Airflow Web UI: http://localhost:8080 (admin/admin)"
echo "- MySQL: localhost:3306 (root/rootpass)"
echo ""
echo "可运行的DAG："
echo "- simple_mysql_etl (推荐先运行这个)"
echo ""
echo "如果需要完整大数据环境，请运行："
echo "./start-hdfs-spark.sh"