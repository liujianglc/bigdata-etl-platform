#!/bin/bash

echo "⚡ 快速启动大数据平台"
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

# 快速设置
print_info "快速环境设置..."

# 创建必要目录
mkdir -p logs config plugins dags spark_jobs
chmod -R 755 logs config plugins dags spark_jobs
chmod -R 777 logs

# 创建.env文件
if [ ! -f ".env" ]; then
    cp .env.example .env
    print_status "创建了.env配置文件"
fi

# 选择启动模式
echo ""
echo "选择启动模式："
echo "1. 🚀 原始镜像 + 运行时安装 (推荐，最稳定)"
echo "2. 🔨 尝试自定义镜像构建"
echo "3. 📦 仅启动基础服务 (Airflow + MySQL)"

read -p "请选择 (1-3): " mode

case $mode in
    1)
        print_info "使用原始镜像启动..."
        $DOCKER_COMPOSE_CMD -f docker-compose.yml -f docker-compose.fallback.yml up -d
        ;;
    2)
        print_info "尝试构建自定义镜像..."
        ./build-airflow-image.sh
        if [ $? -eq 0 ]; then
            print_status "镜像构建成功，启动服务..."
            $DOCKER_COMPOSE_CMD up -d
        else
            print_warning "镜像构建失败，回退到原始镜像..."
            $DOCKER_COMPOSE_CMD -f docker-compose.yml -f docker-compose.fallback.yml up -d
        fi
        ;;
    3)
        print_info "启动基础服务..."
        $DOCKER_COMPOSE_CMD -f docker-compose.yml -f docker-compose.fallback.yml up -d postgres mysql redis airflow-webserver airflow-scheduler
        ;;
    *)
        print_error "无效选择"
        exit 1
        ;;
esac

if [ $? -eq 0 ]; then
    print_status "服务启动成功！"
    
    echo ""
    print_info "等待服务初始化..."
    sleep 15
    
    echo ""
    print_info "检查服务状态..."
    $DOCKER_COMPOSE_CMD ps
    
    echo ""
    print_status "🎉 快速启动完成！"
    echo ""
    echo "📊 访问地址："
    echo "  - Airflow: http://localhost:8080 (admin/admin)"
    echo "  - MySQL: localhost:3306"
    echo ""
    echo "🔧 管理命令："
    echo "  - 查看日志: $DOCKER_COMPOSE_CMD logs -f airflow-webserver"
    echo "  - 停止服务: $DOCKER_COMPOSE_CMD down"
    echo "  - 完全清理: ./cleanup.sh"
    
    # 检查Airflow是否可访问
    echo ""
    print_info "验证Airflow访问..."
    sleep 10
    if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
        print_status "Airflow Web UI 可以访问！"
    else
        print_warning "Airflow可能还在启动中，请稍等几分钟后访问"
    fi
    
else
    print_error "服务启动失败"
    echo ""
    echo "🔧 故障排除："
    echo "  1. 检查端口是否被占用: netstat -tulpn | grep :8080"
    echo "  2. 查看错误日志: $DOCKER_COMPOSE_CMD logs"
    echo "  3. 重新启动: $DOCKER_COMPOSE_CMD down && ./quick-start.sh"
    exit 1
fi