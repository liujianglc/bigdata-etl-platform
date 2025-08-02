#!/bin/bash

# 优化的一次性启动脚本
# 支持完整的服务启动和数据初始化

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# 检查必要文件
check_requirements() {
    log_step "检查必要文件..."
    
    local required_files=(
        ".env"
        "docker-compose.yaml"
        "Dockerfile.airflow"
        "setup-connections-enhanced.py"
        "init-hdfs-directories.sh"
        "init-multiple-databases.sh"
        "init-mysql-data.sql"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "必要文件不存在: $file"
            exit 1
        fi
    done
    
    log_info "所有必要文件检查完成"
}

# 设置权限
set_permissions() {
    log_step "设置脚本权限..."
    
    chmod +x init-hdfs-directories.sh
    chmod +x init-multiple-databases.sh
    chmod +x setup-connections-enhanced.py
    
    # 创建必要的目录
    mkdir -p logs dags config plugins spark_jobs
    
    # 设置目录权限
    chmod -R 755 logs dags config plugins spark_jobs
    
    log_info "权限设置完成"
}

# 清理旧容器和数据
cleanup_old_resources() {
    log_step "清理旧的容器和网络..."
    
    # 停止并删除旧容器
    docker-compose down -v --remove-orphans 2>/dev/null || true
    
    # 删除悬空的镜像
    docker image prune -f 2>/dev/null || true
    
    log_info "清理完成"
}

# 构建镜像
build_images() {
    log_step "构建Airflow镜像..."
    
    docker-compose build airflow-webserver
    
    if [[ $? -eq 0 ]]; then
        log_info "镜像构建完成"
    else
        log_error "镜像构建失败"
        exit 1
    fi
}

# 启动基础服务
start_base_services() {
    log_step "启动基础服务 (PostgreSQL, Redis, MySQL)..."
    
    docker-compose up -d postgres redis mysql
    
    # 等待基础服务启动
    log_info "等待基础服务启动..."
    sleep 30
    
    # 检查服务状态
    local services=("postgres" "redis" "mysql")
    for service in "${services[@]}"; do
        if docker-compose ps "$service" | grep -q "Up"; then
            log_info "$service 服务启动成功"
        else
            log_error "$service 服务启动失败"
            docker-compose logs "$service"
            exit 1
        fi
    done
}

# 启动大数据服务
start_bigdata_services() {
    log_step "启动大数据服务 (HDFS, Hive, Spark)..."
    
    # 启动HDFS
    docker-compose up -d namenode datanode
    log_info "等待HDFS启动..."
    sleep 45
    
    # 启动Hive Metastore
    docker-compose up -d hive-metastore
    log_info "等待Hive Metastore启动..."
    sleep 30
    
    # 启动Spark
    docker-compose up -d spark-master spark-worker
    log_info "等待Spark启动..."
    sleep 20
    
    # 检查大数据服务状态
    local bigdata_services=("namenode" "datanode" "hive-metastore" "spark-master" "spark-worker")
    for service in "${bigdata_services[@]}"; do
        if docker-compose ps "$service" | grep -q "Up"; then
            log_info "$service 服务启动成功"
        else
            log_warn "$service 服务可能启动失败，检查日志..."
            docker-compose logs --tail=20 "$service"
        fi
    done
}

# 初始化数据
initialize_data() {
    log_step "初始化数据和目录结构..."
    
    # 运行HDFS目录初始化
    log_info "初始化HDFS目录..."
    docker-compose up hdfs-init
    
    if [[ $? -eq 0 ]]; then
        log_info "HDFS目录初始化完成"
    else
        log_warn "HDFS目录初始化可能失败，继续执行..."
    fi
}

# 启动Airflow服务
start_airflow_services() {
    log_step "启动Airflow服务..."
    
    # 运行Airflow初始化
    log_info "初始化Airflow..."
    docker-compose up airflow-init
    
    if [[ $? -eq 0 ]]; then
        log_info "Airflow初始化完成"
    else
        log_error "Airflow初始化失败"
        docker-compose logs airflow-init
        exit 1
    fi
    
    # 启动Airflow服务
    log_info "启动Airflow服务..."
    docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
    
    # 等待Airflow启动
    log_info "等待Airflow服务启动..."
    sleep 60
    
    # 检查Airflow服务状态
    local airflow_services=("airflow-webserver" "airflow-scheduler" "airflow-worker")
    for service in "${airflow_services[@]}"; do
        if docker-compose ps "$service" | grep -q "Up"; then
            log_info "$service 服务启动成功"
        else
            log_error "$service 服务启动失败"
            docker-compose logs --tail=30 "$service"
        fi
    done
}

# 验证服务状态
verify_services() {
    log_step "验证所有服务状态..."
    
    # 检查端口可访问性
    local ports=(
        "5432:PostgreSQL"
        "6379:Redis"
        "3306:MySQL"
        "9870:HDFS NameNode UI"
        "8081:Spark Master UI"
        "9083:Hive Metastore"
        "8080:Airflow WebUI"
    )
    
    for port_info in "${ports[@]}"; do
        local port=$(echo $port_info | cut -d: -f1)
        local service=$(echo $port_info | cut -d: -f2)
        
        if nc -z localhost "$port" 2>/dev/null; then
            log_info "$service (端口 $port) 可访问"
        else
            log_warn "$service (端口 $port) 不可访问"
        fi
    done
    
    # 显示服务状态
    echo
    log_step "所有服务状态:"
    docker-compose ps
}

# 显示访问信息
show_access_info() {
    log_step "服务访问信息:"
    
    echo
    echo "🌐 Web界面访问地址:"
    echo "  • Airflow WebUI:     http://localhost:8080 (admin/admin123)"
    echo "  • Spark Master UI:   http://localhost:8081"
    echo "  • HDFS NameNode UI:  http://localhost:9870"
    echo
    echo "🔌 数据库连接信息:"
    echo "  • PostgreSQL:        localhost:5432 (airflow/airflow123)"
    echo "  • MySQL:             localhost:3306 (etl_user/etl_pass)"
    echo "  • Hive Metastore:    localhost:9083"
    echo
    echo "📁 重要目录:"
    echo "  • DAGs目录:          ./dags/"
    echo "  • 配置目录:          ./config/"
    echo "  • Spark作业目录:     ./spark_jobs/"
    echo "  • 日志目录:          ./logs/"
    echo
    echo "🚀 快速命令:"
    echo "  • 查看日志:          docker-compose logs -f [service_name]"
    echo "  • 重启服务:          docker-compose restart [service_name]"
    echo "  • 停止所有服务:      docker-compose down"
    echo "  • 查看服务状态:      docker-compose ps"
    echo
}

# 主函数
main() {
    log_info "开始启动优化的大数据ETL平台..."
    echo
    
    # 检查Docker和Docker Compose
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装或不在PATH中"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose未安装或不在PATH中"
        exit 1
    fi
    
    # 执行启动步骤
    check_requirements
    set_permissions
    cleanup_old_resources
    build_images
    start_base_services
    start_bigdata_services
    initialize_data
    start_airflow_services
    verify_services
    show_access_info
    
    log_info "🎉 大数据ETL平台启动完成！"
    log_info "请等待1-2分钟让所有服务完全启动，然后访问 http://localhost:8080 查看Airflow界面"
}

# 捕获中断信号
trap 'log_error "启动过程被中断"; exit 1' INT TERM

# 运行主函数
main "$@"