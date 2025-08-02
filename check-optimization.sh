#!/bin/bash

# 优化配置检查脚本
# 验证所有必要文件和配置是否正确

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 计数器
CHECKS_PASSED=0
CHECKS_FAILED=0
CHECKS_WARNING=0

# 检查函数
check_pass() {
    echo -e "${GREEN}✅ PASS${NC} $1"
    ((CHECKS_PASSED++))
}

check_fail() {
    echo -e "${RED}❌ FAIL${NC} $1"
    ((CHECKS_FAILED++))
}

check_warn() {
    echo -e "${YELLOW}⚠️  WARN${NC} $1"
    ((CHECKS_WARNING++))
}

check_info() {
    echo -e "${BLUE}ℹ️  INFO${NC} $1"
}

# 检查必要文件
check_required_files() {
    echo -e "\n${BLUE}=== 检查必要文件 ===${NC}"
    
    local required_files=(
        "docker-compose.yaml:Docker Compose 配置文件"
        "Dockerfile.airflow:Airflow Docker 文件"
        "setup-connections-enhanced.py:增强连接设置脚本"
        "init-hdfs-directories.sh:HDFS 初始化脚本"
        "init-multiple-databases.sh:数据库初始化脚本"
        "init-mysql-data.sql:MySQL 数据初始化脚本"
        "start-optimized.sh:优化启动脚本"
        "config/production_etl_config_dynamic.yaml:动态配置文件"
        "dags/dynamic_bigdata_etl.py:动态 DAG 文件"
    )
    
    for file_info in "${required_files[@]}"; do
        local file=$(echo $file_info | cut -d: -f1)
        local desc=$(echo $file_info | cut -d: -f2)
        
        if [[ -f "$file" ]]; then
            check_pass "$desc ($file)"
        else
            check_fail "$desc ($file) 不存在"
        fi
    done
}

# 检查环境变量文件
check_env_file() {
    echo -e "\n${BLUE}=== 检查环境变量配置 ===${NC}"
    
    if [[ -f ".env" ]]; then
        check_pass ".env 文件存在"
        
        # 检查关键环境变量
        local required_vars=(
            "POSTGRES_USER"
            "POSTGRES_PASSWORD"
            "POSTGRES_DB"
            "AIRFLOW__CORE__FERNET_KEY"
            "MYSQL_ROOT_PASSWORD"
            "MYSQL_DATABASE"
            "SPARK_MASTER_HOST"
            "HDFS_NAMENODE_HOST"
        )
        
        for var in "${required_vars[@]}"; do
            if grep -q "^${var}=" .env; then
                local value=$(grep "^${var}=" .env | cut -d= -f2)
                if [[ -n "$value" && "$value" != "your-"* ]]; then
                    check_pass "环境变量 $var 已设置"
                else
                    check_warn "环境变量 $var 可能需要自定义值"
                fi
            else
                check_fail "环境变量 $var 未在 .env 文件中定义"
            fi
        done
        
        # 检查 Fernet Key 格式
        if grep -q "^AIRFLOW__CORE__FERNET_KEY=" .env; then
            local fernet_key=$(grep "^AIRFLOW__CORE__FERNET_KEY=" .env | cut -d= -f2)
            if [[ ${#fernet_key} -eq 44 && "$fernet_key" =~ ^[A-Za-z0-9+/]*=*$ ]]; then
                check_pass "Fernet Key 格式正确"
            else
                check_warn "Fernet Key 格式可能不正确，建议重新生成"
            fi
        fi
        
    else
        check_fail ".env 文件不存在，请创建并配置"
        check_info "可以复制 .env.example 文件或运行: cp .env.example .env"
    fi
}

# 检查脚本权限
check_script_permissions() {
    echo -e "\n${BLUE}=== 检查脚本权限 ===${NC}"
    
    local scripts=(
        "start-optimized.sh"
        "init-hdfs-directories.sh"
        "setup-connections-enhanced.py"
    )
    
    for script in "${scripts[@]}"; do
        if [[ -f "$script" ]]; then
            if [[ -x "$script" ]]; then
                check_pass "$script 有执行权限"
            else
                check_warn "$script 没有执行权限，运行: chmod +x $script"
            fi
        fi
    done
}

# 检查目录结构
check_directory_structure() {
    echo -e "\n${BLUE}=== 检查目录结构 ===${NC}"
    
    local required_dirs=(
        "dags:DAG 文件目录"
        "config:配置文件目录"
        "spark_jobs:Spark 作业目录"
        "logs:日志目录"
        "plugins:插件目录"
    )
    
    for dir_info in "${required_dirs[@]}"; do
        local dir=$(echo $dir_info | cut -d: -f1)
        local desc=$(echo $dir_info | cut -d: -f2)
        
        if [[ -d "$dir" ]]; then
            check_pass "$desc ($dir) 存在"
        else
            check_warn "$desc ($dir) 不存在，将自动创建"
        fi
    done
}

# 检查 Docker 环境
check_docker_environment() {
    echo -e "\n${BLUE}=== 检查 Docker 环境 ===${NC}"
    
    if command -v docker &> /dev/null; then
        check_pass "Docker 已安装"
        
        # 检查 Docker 是否运行
        if docker info &> /dev/null; then
            check_pass "Docker 服务正在运行"
        else
            check_fail "Docker 服务未运行，请启动 Docker"
        fi
        
        # 检查 Docker 版本
        local docker_version=$(docker --version | grep -oE '[0-9]+\.[0-9]+')
        check_info "Docker 版本: $docker_version"
        
    else
        check_fail "Docker 未安装或不在 PATH 中"
    fi
    
    if command -v docker-compose &> /dev/null; then
        check_pass "Docker Compose 已安装"
        
        # 检查 Docker Compose 版本
        local compose_version=$(docker-compose --version | grep -oE '[0-9]+\.[0-9]+')
        check_info "Docker Compose 版本: $compose_version"
        
    else
        check_fail "Docker Compose 未安装或不在 PATH 中"
    fi
}

# 检查端口占用
check_port_availability() {
    echo -e "\n${BLUE}=== 检查端口可用性 ===${NC}"
    
    if [[ -f ".env" ]]; then
        # 从 .env 文件读取端口
        local ports=(
            "$(grep '^POSTGRES_PORT=' .env | cut -d= -f2):PostgreSQL"
            "$(grep '^REDIS_PORT=' .env | cut -d= -f2):Redis"
            "$(grep '^MYSQL_PORT=' .env | cut -d= -f2):MySQL"
            "$(grep '^AIRFLOW_WEBSERVER_PORT=' .env | cut -d= -f2):Airflow WebUI"
            "$(grep '^SPARK_UI_PORT=' .env | cut -d= -f2):Spark UI"
            "$(grep '^HDFS_UI_PORT=' .env | cut -d= -f2):HDFS UI"
            "$(grep '^HIVE_METASTORE_PORT=' .env | cut -d= -f2):Hive Metastore"
        )
        
        for port_info in "${ports[@]}"; do
            local port=$(echo $port_info | cut -d: -f1)
            local service=$(echo $port_info | cut -d: -f2)
            
            if [[ -n "$port" ]]; then
                if ! nc -z localhost "$port" 2>/dev/null; then
                    check_pass "端口 $port ($service) 可用"
                else
                    check_warn "端口 $port ($service) 已被占用"
                fi
            fi
        done
    else
        check_warn "无法检查端口可用性 (.env 文件不存在)"
    fi
}

# 检查系统资源
check_system_resources() {
    echo -e "\n${BLUE}=== 检查系统资源 ===${NC}"
    
    # 检查内存
    if command -v free &> /dev/null; then
        local total_mem=$(free -m | awk 'NR==2{printf "%.1f", $2/1024}')
        local available_mem=$(free -m | awk 'NR==2{printf "%.1f", $7/1024}')
        
        check_info "总内存: ${total_mem}GB, 可用内存: ${available_mem}GB"
        
        if (( $(echo "$available_mem >= 4" | bc -l) )); then
            check_pass "内存充足 (建议 4GB+)"
        else
            check_warn "内存可能不足，建议至少 4GB 可用内存"
        fi
    fi
    
    # 检查磁盘空间
    local disk_usage=$(df -h . | awk 'NR==2 {print $4}')
    check_info "当前目录可用磁盘空间: $disk_usage"
    
    # 检查 CPU 核心数
    local cpu_cores=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "未知")
    check_info "CPU 核心数: $cpu_cores"
}

# 检查配置文件语法
check_config_syntax() {
    echo -e "\n${BLUE}=== 检查配置文件语法 ===${NC}"
    
    # 检查 Docker Compose 语法
    if [[ -f "docker-compose.yaml" ]]; then
        if docker-compose config &> /dev/null; then
            check_pass "Docker Compose 配置语法正确"
        else
            check_fail "Docker Compose 配置语法错误"
            check_info "运行 'docker-compose config' 查看详细错误"
        fi
    fi
    
    # 检查 YAML 配置文件
    if [[ -f "config/production_etl_config_dynamic.yaml" ]]; then
        if python3 -c "import yaml; yaml.safe_load(open('config/production_etl_config_dynamic.yaml'))" 2>/dev/null; then
            check_pass "动态配置文件 YAML 语法正确"
        else
            check_fail "动态配置文件 YAML 语法错误"
        fi
    fi
    
    # 检查 Python DAG 文件语法
    if [[ -f "dags/dynamic_bigdata_etl.py" ]]; then
        if python3 -m py_compile dags/dynamic_bigdata_etl.py 2>/dev/null; then
            check_pass "动态 DAG 文件 Python 语法正确"
        else
            check_fail "动态 DAG 文件 Python 语法错误"
        fi
    fi
}

# 生成报告
generate_report() {
    echo -e "\n${BLUE}=== 检查报告 ===${NC}"
    
    local total_checks=$((CHECKS_PASSED + CHECKS_FAILED + CHECKS_WARNING))
    
    echo -e "总检查项: $total_checks"
    echo -e "${GREEN}通过: $CHECKS_PASSED${NC}"
    echo -e "${YELLOW}警告: $CHECKS_WARNING${NC}"
    echo -e "${RED}失败: $CHECKS_FAILED${NC}"
    
    if [[ $CHECKS_FAILED -eq 0 ]]; then
        if [[ $CHECKS_WARNING -eq 0 ]]; then
            echo -e "\n${GREEN}🎉 所有检查都通过了！可以安全启动服务。${NC}"
        else
            echo -e "\n${YELLOW}✅ 基本检查通过，但有一些警告需要注意。${NC}"
        fi
        echo -e "\n运行以下命令启动服务:"
        echo -e "  ${BLUE}./start-optimized.sh${NC}"
    else
        echo -e "\n${RED}❌ 有检查项失败，请修复后再启动服务。${NC}"
        echo -e "\n修复建议:"
        echo -e "  1. 创建 .env 文件并配置必要的环境变量"
        echo -e "  2. 确保所有必要文件存在"
        echo -e "  3. 设置脚本执行权限: chmod +x *.sh"
        echo -e "  4. 检查 Docker 环境是否正常"
    fi
}

# 主函数
main() {
    echo -e "${BLUE}大数据ETL平台优化配置检查${NC}"
    echo -e "${BLUE}================================${NC}"
    
    check_required_files
    check_env_file
    check_script_permissions
    check_directory_structure
    check_docker_environment
    check_port_availability
    check_system_resources
    check_config_syntax
    generate_report
}

# 运行检查
main "$@"