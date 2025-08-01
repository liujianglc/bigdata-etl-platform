#!/bin/bash

echo "🧹 大数据平台清理脚本"
echo "====================="

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Detect docker-compose command
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    print_error "Docker Compose未找到"
    exit 1
fi

echo "选择清理级别："
echo "1. 软清理 - 停止容器但保留数据"
echo "2. 中等清理 - 停止容器并删除容器，保留数据卷"
echo "3. 完全清理 - 删除所有容器、网络、数据卷"
echo "4. 深度清理 - 完全清理 + 删除自定义镜像"
echo "5. 仅清理日志文件"

read -p "请选择 (1-5): " cleanup_level

case $cleanup_level in
    1)
        echo "🛑 停止所有服务..."
        $DOCKER_COMPOSE_CMD stop
        print_status "服务已停止，数据已保留"
        ;;
    2)
        echo "🛑 停止并删除容器..."
        $DOCKER_COMPOSE_CMD down
        print_status "容器已删除，数据卷已保留"
        ;;
    3)
        echo "🗑️  完全清理所有资源..."
        $DOCKER_COMPOSE_CMD down -v --remove-orphans
        
        # Clean up networks
        echo "清理网络..."
        docker network prune -f
        
        print_status "完全清理完成"
        ;;
    4)
        echo "🗑️  深度清理..."
        $DOCKER_COMPOSE_CMD down -v --remove-orphans
        
        # Remove custom images
        echo "删除自定义镜像..."
        docker rmi custom-airflow:latest 2>/dev/null || true
        
        # Clean up everything
        docker system prune -f
        docker volume prune -f
        docker network prune -f
        
        print_status "深度清理完成"
        ;;
    5)
        echo "🧹 清理日志文件..."
        if [ -d "logs" ]; then
            rm -rf logs/*
            print_status "日志文件已清理"
        else
            print_warning "未找到日志目录"
        fi
        ;;
    *)
        print_error "无效选择"
        exit 1
        ;;
esac

echo ""
echo "🎯 清理完成！"
echo ""
echo "💡 提示："
echo "  - 重新部署: ./deploy.sh"
echo "  - 查看状态: $DOCKER_COMPOSE_CMD ps"
echo "  - 查看镜像: docker images"