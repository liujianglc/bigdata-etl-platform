#!/bin/bash

echo "🚀 大数据平台快速部署脚本"
echo "================================"

# 检查系统要求
echo "📋 检查系统要求..."

# 检查Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker未安装，请先安装Docker"
    exit 1
fi

# 检查Docker Compose
if ! command -v docker-compose &> /dev/null && ! command -v docker &> /dev/null; then
    echo "❌ Docker Compose未安装，请先安装Docker Compose"
    exit 1
fi

# 检查docker-compose权限并尝试修复
if [ -f "/usr/local/bin/docker-compose" ] && [ ! -x "/usr/local/bin/docker-compose" ]; then
    echo "🔧 修复docker-compose权限..."
    sudo chmod +x /usr/local/bin/docker-compose
fi

# 检查内存
total_mem=$(free -g | awk '/^Mem:/{print $2}')
if [ "$total_mem" -lt 8 ]; then
    echo "⚠️  警告：系统内存少于8GB，可能影响性能"
fi

# 检查磁盘空间
available_space=$(df -BG . | awk 'NR==2{print $4}' | sed 's/G//')
if [ "$available_space" -lt 20 ]; then
    echo "⚠️  警告：可用磁盘空间少于20GB，可能不足"
fi

echo "✅ 系统检查完成"

# 设置权限
echo ""
echo "🔧 设置脚本权限..."
chmod +x *.sh

# 创建必要目录
echo "📁 创建必要目录..."
mkdir -p logs config plugins dags spark_jobs

# 设置正确的权限
echo "🔧 设置目录权限..."
chmod -R 755 logs config plugins dags spark_jobs
chmod -R 777 logs  # Airflow logs需要特殊权限

# 环境配置
echo ""
echo "⚙️  环境配置..."
if [ ! -f ".env" ]; then
    echo "创建环境配置文件..."
    cp .env.example .env
    echo "✅ 请根据需要编辑 .env 文件"
fi

# 选择部署模式
echo ""
echo "🎯 选择部署模式："
echo "1. 完整部署 (所有组件)"
echo "2. 基础部署 (Airflow + MySQL)"
echo "3. 自定义部署"

read -p "请选择 (1-3): " deploy_mode

# 检测docker-compose命令
DOCKER_COMPOSE_CMD="docker compose"

case $deploy_mode in
    1)
        echo "🚀 启动完整大数据平台..."
        if [ -f "./start.sh" ]; then
            # 传递docker-compose命令给start.sh
            export DOCKER_COMPOSE_CMD
            ./start.sh
        else
            echo "❌ start.sh 文件不存在"
            exit 1
        fi
        ;;
    2)
        echo "🚀 启动基础服务..."
        if [ -f "./start-basic.sh" ]; then
            export DOCKER_COMPOSE_CMD
            ./start-basic.sh
        else
            echo "❌ start-basic.sh 文件不存在"
            exit 1
        fi
        ;;
    3)
        echo "📋 自定义部署选项："
        echo "1. 基础服务: ./start-basic.sh"
        echo "2. 大数据组件: ./start-hdfs-spark.sh"
        echo "3. 完整环境: ./start.sh"
        echo "请手动执行相应脚本"
        exit 0
        ;;
    *)
        echo "❌ 无效选择"
        exit 1
        ;;
esac

# 等待服务启动
echo ""
echo "⏳ 等待服务启动完成..."
sleep 30

# 验证部署
echo ""
echo "🔍 验证部署状态..."

# 检查容器状态
echo "检查Docker容器状态："
$DOCKER_COMPOSE_CMD ps

# 检查关键服务
services=("postgres" "mysql" "airflow-webserver")
all_running=true

for service in "${services[@]}"; do
    if $DOCKER_COMPOSE_CMD ps $service | grep -q "Up"; then
        echo "✅ $service: 运行正常"
    else
        echo "❌ $service: 未运行"
        all_running=false
    fi
done

# 检查Web界面
echo ""
echo "🌐 检查Web界面可访问性："

# 检查Airflow
if curl -s http://localhost:8080 > /dev/null; then
    echo "✅ Airflow Web UI: http://localhost:8080 (admin/admin)"
else
    echo "❌ Airflow Web UI不可访问"
    all_running=false
fi

# 检查Spark (如果启动了)
if $DOCKER_COMPOSE_CMD ps spark-master | grep -q "Up"; then
    if curl -s http://localhost:8081 > /dev/null; then
        echo "✅ Spark Master UI: http://localhost:8081"
    else
        echo "❌ Spark Master UI不可访问"
    fi
fi

# 检查HDFS (如果启动了)
if $DOCKER_COMPOSE_CMD ps namenode | grep -q "Up"; then
    if curl -s http://localhost:9870 > /dev/null; then
        echo "✅ HDFS NameNode UI: http://localhost:9870"
    else
        echo "❌ HDFS NameNode UI不可访问"
    fi
fi

echo ""
if [ "$all_running" = true ]; then
    echo "🎉 部署成功完成！"
    echo ""
    echo "📊 访问地址："
    echo "  - Airflow Web UI: http://localhost:8080 (admin/admin)"
    echo "  - Spark Master UI: http://localhost:8081"
    echo "  - HDFS NameNode UI: http://localhost:9870"
    echo "  - MySQL: localhost:3306 (root/rootpass)"
    echo ""
    echo "🎯 下一步："
    echo "1. 访问Airflow UI并启用DAG"
    echo "2. 运行simple_mysql_etl进行测试"
    echo "3. 查看README.md了解更多功能"
    echo ""
    echo "🔧 故障排除："
    echo "  - 查看日志: docker-compose logs [service-name]"
    echo "  - 重启服务: docker-compose restart [service-name]"
    echo "  - 完全重启: docker-compose down && ./start.sh"
else
    echo "⚠️  部署完成但有服务未正常启动"
    echo ""
    echo "🔧 故障排除建议："
    echo "1. 检查系统资源是否充足"
    echo "2. 查看服务日志: $DOCKER_COMPOSE_CMD logs"
    echo "3. 尝试重启: $DOCKER_COMPOSE_CMD restart"
    echo "4. 运行权限修复: ./fix-permissions.sh"
    echo "5. 运行故障排除脚本: ./troubleshoot.sh"
fi

echo ""
echo "📖 更多信息请查看："
echo "  - README.md (项目说明)"
echo "  - DEPLOYMENT.md (详细部署指南)"
echo "  - CHECKLIST.md (部署检查清单)"