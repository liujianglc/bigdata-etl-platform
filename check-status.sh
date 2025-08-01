#!/bin/bash

echo "🔍 大数据平台状态检查"
echo "=================="

# 颜色代码
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# 检测docker-compose命令
if command -v docker-compose &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker-compose"
elif docker compose version &> /dev/null; then
    DOCKER_COMPOSE_CMD="docker compose"
else
    print_error "Docker Compose未找到"
    exit 1
fi

echo "1. 📋 容器状态检查"
echo "=================="
$DOCKER_COMPOSE_CMD ps

echo ""
echo "2. 🌐 Web服务检查"
echo "================"

# 检查Airflow
if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
    print_status "Airflow Web UI: http://localhost:8080 ✓"
else
    print_error "Airflow Web UI: http://localhost:8080 ✗"
fi

# 检查Spark Master
if curl -s -f http://localhost:8081 > /dev/null 2>&1; then
    print_status "Spark Master UI: http://localhost:8081 ✓"
else
    print_warning "Spark Master UI: http://localhost:8081 ✗ (可能未启动)"
fi

# 检查HDFS
if curl -s -f http://localhost:9870 > /dev/null 2>&1; then
    print_status "HDFS NameNode UI: http://localhost:9870 ✓"
else
    print_warning "HDFS NameNode UI: http://localhost:9870 ✗ (可能未启动)"
fi

echo ""
echo "3. 🔌 数据库连接检查"
echo "=================="

# 检查PostgreSQL
if $DOCKER_COMPOSE_CMD exec -T postgres pg_isready -U airflow > /dev/null 2>&1; then
    print_status "PostgreSQL: 连接正常 ✓"
else
    print_error "PostgreSQL: 连接失败 ✗"
fi

# 检查MySQL
if $DOCKER_COMPOSE_CMD exec -T mysql mysqladmin ping -h localhost --silent > /dev/null 2>&1; then
    print_status "MySQL: 连接正常 ✓"
else
    print_error "MySQL: 连接失败 ✗"
fi

# 检查Redis
if $DOCKER_COMPOSE_CMD exec -T redis redis-cli ping > /dev/null 2>&1; then
    print_status "Redis: 连接正常 ✓"
else
    print_error "Redis: 连接失败 ✗"
fi

echo ""
echo "4. 📊 系统资源检查"
echo "================"

# 检查内存使用
echo "内存使用情况:"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | head -10

echo ""
echo "5. 📝 最近日志 (最后10行)"
echo "======================"
echo "Airflow Webserver 日志:"
$DOCKER_COMPOSE_CMD logs --tail=5 airflow-webserver 2>/dev/null | tail -5

echo ""
print_info "状态检查完成！"
echo ""
echo "🔧 常用命令："
echo "  - 查看详细日志: $DOCKER_COMPOSE_CMD logs -f [service-name]"
echo "  - 重启服务: $DOCKER_COMPOSE_CMD restart [service-name]"
echo "  - 停止所有服务: $DOCKER_COMPOSE_CMD down"