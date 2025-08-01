#!/bin/bash

echo "🔨 构建自定义Airflow镜像"
echo "======================"

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

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    print_error "Docker未运行，请启动Docker"
    exit 1
fi

# 构建策略1：尝试完整版本
build_full() {
    print_info "尝试构建完整版本（包含所有依赖）..."
    docker build -f Dockerfile.airflow -t custom-airflow:latest . --no-cache
    return $?
}

# 构建策略2：简化版本
build_simple() {
    print_info "尝试构建简化版本（核心依赖）..."
    if [ -f "Dockerfile.airflow.simple" ]; then
        docker build -f Dockerfile.airflow.simple -t custom-airflow:latest . --no-cache
        return $?
    else
        print_warning "未找到简化版Dockerfile"
        return 1
    fi
}

# 构建策略3：最小版本
build_minimal() {
    print_info "构建最小版本（仅基础功能）..."
    
    cat > Dockerfile.airflow.temp << 'EOF'
FROM apache/airflow:2.8.2
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir apache-airflow-providers-mysql mysql-connector-python
RUN echo 'print("Minimal Airflow ready!")' > /opt/airflow/setup-connections-python.py
EOF

    docker build -f Dockerfile.airflow.temp -t custom-airflow:latest . --no-cache
    local result=$?
    rm -f Dockerfile.airflow.temp
    return $result
}

# 主构建逻辑
main() {
    print_info "开始构建自定义Airflow镜像..."
    
    # 策略1：完整构建
    if build_full; then
        print_status "完整版本构建成功！"
        echo "📦 包含所有依赖包"
    # 策略2：简化构建  
    elif build_simple; then
        print_status "简化版本构建成功！"
        print_warning "使用简化版本，部分高级功能可能不可用"
    # 策略3：最小构建
    elif build_minimal; then
        print_status "最小版本构建成功！"
        print_warning "使用最小版本，仅包含基础MySQL连接功能"
    else
        print_error "所有构建策略都失败了"
        print_info "建议："
        echo "  1. 检查网络连接"
        echo "  2. 运行 ./test-build.sh 诊断问题"
        echo "  3. 使用原始镜像: docker-compose -f docker-compose.yml -f docker-compose.fallback.yml up"
        exit 1
    fi
    
    # 显示镜像信息
    echo ""
    print_info "镜像构建完成！"
    echo "📋 镜像详情："
    docker images | head -1
    docker images | grep custom-airflow
    
    echo ""
    print_status "现在可以启动服务了："
    echo "  ./deploy.sh --mode dev"
    echo "  或者"
    echo "  docker-compose up -d"
}

# 运行主函数
main "$@"