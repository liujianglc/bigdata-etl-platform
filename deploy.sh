#!/bin/bash

set -e  # Exit on any error

echo "🚀 大数据平台部署脚本 v2.0"
echo "================================"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_info "检查系统要求..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    
    # Check Docker Compose
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker compose"
    elif command -v docker-compose &> /dev/null; then
        DOCKER_COMPOSE_CMD="docker-compose"
    else
        print_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
    
    print_status "Docker和Docker Compose检查通过"
    
    # Check system resources
    if command -v free &> /dev/null; then
        total_mem=$(free -g | awk '/^Mem:/{print $2}')
        if [ "$total_mem" -lt 8 ]; then
            print_warning "系统内存少于8GB，可能影响性能"
        fi
    fi
    
    # Check disk space
    available_space=$(df -BG . | awk 'NR==2{print $4}' | sed 's/G//')
    if [ "$available_space" -lt 20 ]; then
        print_warning "可用磁盘空间少于20GB，可能不足"
    fi
}

# Setup environment
setup_environment() {
    print_info "设置环境..."
    
    # Create necessary directories
    mkdir -p logs config plugins dags spark_jobs sample_data
    
    # Set permissions
    chmod -R 755 logs config plugins dags spark_jobs
    chmod -R 777 logs  # Airflow logs need special permissions
    
    # Create .env file if it doesn't exist
    if [ ! -f ".env" ]; then
        cp .env.example .env
        print_status "创建了.env配置文件，请根据需要修改"
    fi
    
    print_status "环境设置完成"
}

# Build custom Airflow image
build_custom_image() {
    print_info "构建自定义Airflow镜像..."
    
    if [ -f "Dockerfile.airflow" ] && [ -f "requirements.txt" ]; then
        # Try building the custom image
        ./build-airflow-image.sh
        if [ $? -eq 0 ]; then
            print_status "自定义Airflow镜像构建成功"
            export USE_CUSTOM_IMAGE=true
        else
            print_warning "自定义镜像构建失败，将使用原始镜像和运行时安装"
            export USE_CUSTOM_IMAGE=false
        fi
    else
        print_warning "未找到Dockerfile.airflow或requirements.txt，使用原始镜像"
        export USE_CUSTOM_IMAGE=false
    fi
}

# Deploy services
deploy_services() {
    local mode=$1
    local compose_files="docker-compose.yml"
    
    # Add fallback if custom image build failed
    if [ "$USE_CUSTOM_IMAGE" = "false" ]; then
        if [ -f "docker-compose.fallback.yml" ]; then
            compose_files="$compose_files -f docker-compose.fallback.yml"
            print_info "使用原始镜像和运行时安装模式"
        fi
    fi
    
    case $mode in
        "dev"|"development")
            print_info "启动开发环境..."
            if [ -f "docker-compose.override.yml" ]; then
                compose_files="$compose_files -f docker-compose.override.yml"
            fi
            ;;
        "prod"|"production")
            print_info "启动生产环境..."
            if [ -f "docker-compose.prod.yml" ]; then
                compose_files="$compose_files -f docker-compose.prod.yml"
            fi
            ;;
        *)
            print_info "启动默认环境..."
            ;;
    esac
    
    print_info "使用配置文件: $compose_files"
    
    # Start services
    $DOCKER_COMPOSE_CMD $compose_files up -d
    
    if [ $? -eq 0 ]; then
        print_status "服务启动成功"
    else
        print_error "服务启动失败"
        exit 1
    fi
}

# Wait for services to be ready
wait_for_services() {
    print_info "等待服务启动完成..."
    
    # Wait for key services
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if $DOCKER_COMPOSE_CMD ps | grep -q "Up.*healthy"; then
            print_status "服务健康检查通过"
            break
        fi
        
        attempt=$((attempt + 1))
        echo "等待服务启动... ($attempt/$max_attempts)"
        sleep 10
    done
    
    if [ $attempt -eq $max_attempts ]; then
        print_warning "服务启动超时，但继续验证"
    fi
}

# Verify deployment
verify_deployment() {
    print_info "验证部署状态..."
    
    # Check container status
    echo "Docker容器状态："
    $DOCKER_COMPOSE_CMD ps
    
    # Check web interfaces
    local all_good=true
    
    # Check Airflow
    if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
        print_status "Airflow Web UI: http://localhost:8080"
    else
        print_error "Airflow Web UI不可访问"
        all_good=false
    fi
    
    # Check Spark (if running)
    if $DOCKER_COMPOSE_CMD ps spark-master | grep -q "Up"; then
        if curl -s -f http://localhost:8081 > /dev/null 2>&1; then
            print_status "Spark Master UI: http://localhost:8081"
        else
            print_warning "Spark Master UI不可访问"
        fi
    fi
    
    # Check HDFS (if running)
    if $DOCKER_COMPOSE_CMD ps namenode | grep -q "Up"; then
        if curl -s -f http://localhost:9870 > /dev/null 2>&1; then
            print_status "HDFS NameNode UI: http://localhost:9870"
        else
            print_warning "HDFS NameNode UI不可访问"
        fi
    fi
    
    if [ "$all_good" = true ]; then
        print_status "所有核心服务验证通过！"
    else
        print_warning "部分服务可能需要更多时间启动"
    fi
}

# Show deployment summary
show_summary() {
    echo ""
    echo "🎉 部署完成！"
    echo "================================"
    echo ""
    echo "📊 访问地址："
    echo "  - Airflow Web UI: http://localhost:8080 (admin/admin)"
    echo "  - Spark Master UI: http://localhost:8081"
    echo "  - HDFS NameNode UI: http://localhost:9870"
    echo "  - MySQL: localhost:3306"
    echo ""
    echo "🔧 管理命令："
    echo "  - 查看日志: $DOCKER_COMPOSE_CMD logs [service-name]"
    echo "  - 重启服务: $DOCKER_COMPOSE_CMD restart [service-name]"
    echo "  - 停止所有服务: $DOCKER_COMPOSE_CMD down"
    echo "  - 完全清理: $DOCKER_COMPOSE_CMD down -v"
    echo ""
    echo "📖 更多信息请查看 README.md"
}

# Main deployment logic
main() {
    # Parse command line arguments
    local mode="dev"
    local skip_build=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --mode|-m)
                mode="$2"
                shift 2
                ;;
            --skip-build)
                skip_build=true
                shift
                ;;
            --help|-h)
                echo "用法: $0 [选项]"
                echo "选项:"
                echo "  --mode, -m MODE    部署模式 (dev|prod) [默认: dev]"
                echo "  --skip-build       跳过镜像构建"
                echo "  --help, -h         显示帮助信息"
                exit 0
                ;;
            *)
                print_error "未知选项: $1"
                exit 1
                ;;
        esac
    done
    
    # Execute deployment steps
    check_prerequisites
    setup_environment
    
    if [ "$skip_build" = false ]; then
        build_custom_image
    fi
    
    deploy_services "$mode"
    wait_for_services
    verify_deployment
    show_summary
}

# Run main function with all arguments
main "$@"