#!/bin/bash

echo "🔧 修复Airflow权限问题"
echo "======================"

# 获取当前用户ID和组ID
CURRENT_UID=$(id -u)
CURRENT_GID=$(id -g)

echo "当前用户ID: $CURRENT_UID"
echo "当前组ID: $CURRENT_GID"

# 创建必要的目录
echo "📁 创建必要目录..."
mkdir -p logs config plugins dags spark_jobs

# 设置正确的权限
echo "🔧 设置目录权限..."

# Airflow在容器中以UID 50000运行，我们需要确保这些目录可写
sudo chown -R $CURRENT_UID:$CURRENT_GID logs config plugins dags spark_jobs
chmod -R 755 logs config plugins dags spark_jobs

# 特别处理logs目录，确保Airflow容器可以写入
sudo chmod -R 777 logs

echo "✅ 权限修复完成"

# 验证权限
echo ""
echo "🔍 验证目录权限:"
ls -la logs config plugins dags spark_jobs 2>/dev/null || echo "某些目录不存在，将在启动时创建"

echo ""
echo "💡 如果问题仍然存在，请尝试："
echo "1. 完全停止所有容器: docker-compose down -v"
echo "2. 删除logs目录: sudo rm -rf logs"
echo "3. 重新创建: mkdir -p logs && chmod 777 logs"
echo "4. 重新启动: ./start.sh"