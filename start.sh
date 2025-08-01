#!/bin/bash

echo "启动完整大数据管道环境..."

# 检测docker-compose命令
if [ -n "$DOCKER_COMPOSE_CMD" ]; then
    # 使用从quick-deploy.sh传递的命令
    COMPOSE_CMD="$DOCKER_COMPOSE_CMD"
elif command -v docker-compose &> /dev/null; then
    COMPOSE_CMD="docker-compose"
elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
    echo "💡 使用 Docker Compose V2"
else
    echo "❌ 无法找到可用的docker-compose命令"
    echo "请安装Docker Compose或运行: ./fix-permissions.sh"
    exit 1
fi

# 停止可能运行的服务
$COMPOSE_CMD down

# 创建必要的目录
mkdir -p logs config plugins dags spark_jobs

# 分阶段启动服务
echo "第一阶段：启动基础服务..."
$COMPOSE_CMD up -d postgres redis mysql

echo "等待基础服务启动..."
sleep 20

# 初始化Airflow数据库
echo "初始化Airflow数据库..."
$COMPOSE_CMD run --rm airflow-webserver airflow db init

# 创建Airflow管理员用户
echo "创建Airflow管理员用户..."
$COMPOSE_CMD run --rm airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "第二阶段：启动HDFS服务..."
$COMPOSE_CMD up -d namenode datanode

echo "等待HDFS服务启动..."
sleep 30

echo "第三阶段：启动Hive和Spark服务..."
$COMPOSE_CMD up -d hive-metastore spark-master spark-worker

echo "等待Hive和Spark服务启动..."
sleep 30

echo "第四阶段：启动Airflow服务..."
$COMPOSE_CMD up -d airflow-webserver airflow-scheduler airflow-worker

echo "等待Airflow服务启动..."
sleep 30

# 初始化MySQL测试数据
echo "初始化MySQL测试数据..."
$COMPOSE_CMD exec -T mysql mysql -u root -prootpass source_db < init-mysql-data.sql

# 设置Airflow连接
echo "设置Airflow连接..."
./setup-connections.sh

# 初始化HDFS目录
echo "初始化HDFS目录结构..."
$COMPOSE_CMD exec namenode hdfs dfs -mkdir -p /user/hive/warehouse || true
$COMPOSE_CMD exec namenode hdfs dfs -mkdir -p /tmp/spark-events || true
$COMPOSE_CMD exec namenode hdfs dfs -chmod -R 777 /user/hive/warehouse || true
$COMPOSE_CMD exec namenode hdfs dfs -chmod -R 777 /tmp || true

echo "环境启动完成!"
echo ""
echo "=== 访问地址 ==="
echo "- Airflow Web UI: http://localhost:8080 (admin/admin)"
echo "- Spark Master UI: http://localhost:8081"
echo "- HDFS NameNode UI: http://localhost:9870"
echo "- MySQL: localhost:3306 (root/rootpass)"
echo ""
echo "=== 可用的DAG ==="
echo "1. simple_mysql_etl - 简单MySQL ETL流程"
echo "2. complete_bigdata_etl - 完整大数据ETL流程"
echo "3. mysql_to_hive_etl - MySQL到Hive的ETL流程"
echo ""
echo "=== 测试步骤 ==="
echo "1. 访问Airflow UI"
echo "2. 启用想要运行的DAG"
echo "3. 手动触发DAG执行"
echo "4. 查看执行日志和结果"