#!/bin/bash

# Airflow 完整初始化脚本
# 初始化数据库、创建管理员用户、设置连接

set -e

echo "🚀 开始 Airflow 完整初始化..."

# 等待数据库服务启动
echo "⏳ 等待 PostgreSQL 数据库启动..."
while ! pg_isready -h postgres -p 5432 -U ${POSTGRES_USER:-airflow}; do
    echo "等待数据库连接..."
    sleep 2
done
echo "✅ 数据库连接成功"

# 初始化 Airflow 数据库
echo "🗄️ 初始化 Airflow 数据库..."
airflow db init

# 创建管理员用户
echo "👤 创建管理员用户..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123

# 设置连接
echo "🔗 设置 Airflow 连接..."
if [ -f "/opt/airflow/setup-connections-enhanced.py" ]; then
    python /opt/airflow/setup-connections-enhanced.py
else
    echo "⚠️ setup-connections-enhanced.py 文件不存在，跳过连接设置"
fi

# 设置变量
echo "📝 设置 Airflow 变量..."
airflow variables set ETL_SCHEDULE_CRON "${ETL_SCHEDULE_CRON:-0 2 * * *}"
airflow variables set ETL_TIMEZONE "${ETL_TIMEZONE:-Asia/Shanghai}"
airflow variables set ETL_RETRY_ATTEMPTS "${ETL_RETRY_ATTEMPTS:-2}"
airflow variables set ETL_RETRY_DELAY "${ETL_RETRY_DELAY:-10}"
airflow variables set DATA_QUALITY_NULL_THRESHOLD "${DATA_QUALITY_NULL_THRESHOLD:-0.05}"
airflow variables set DATA_QUALITY_FRESHNESS_HOURS "${DATA_QUALITY_FRESHNESS_HOURS:-25}"

echo "✅ Airflow 初始化完成！"
echo "🌐 Web UI: http://localhost:${AIRFLOW_WEBSERVER_PORT:-8080}"
echo "👤 用户名: admin"
echo "🔑 密码: admin123"