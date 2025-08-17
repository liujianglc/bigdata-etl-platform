#!/bin/bash

# 数据重置脚本 - 清除所有 HDFS 和 Hive 数据
# 用于重新开始数据备份和处理

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

# 显示警告信息
show_warning() {
    echo
    log_warn "⚠️  警告：此操作将删除以下所有数据："
    echo "  • HDFS 文件系统中的所有数据"
    echo "  • Hive 数据仓库中的所有表和数据"
    echo "  • PostgreSQL 中的 Airflow 和 Hive Metastore 数据"
    echo "  • 所有日志文件"
    echo "  • Docker 数据卷"
    echo
    log_warn "此操作不可逆！请确保已备份重要数据。"
    echo
}

# 重置所有数据
reset_all_data() {
    log_step "开始重置所有数据..."
    
    # 停止所有服务
    log_info "停止所有 Docker 服务..."
    docker compose down -v --remove-orphans 2>/dev/null || true
    
    # 删除所有相关的数据卷
    log_info "删除 Docker 数据卷..."
    local volumes=$(docker volume ls -q | grep -E "(hdfs|hive|postgres|mysql|namenode|datanode|airflow)" 2>/dev/null || true)
    if [[ -n "$volumes" ]]; then
        echo "$volumes" | xargs docker volume rm 2>/dev/null || true
        log_info "已删除相关数据卷"
    else
        log_info "未找到相关数据卷"
    fi
    
    # 删除本地数据目录
    log_info "删除本地数据目录..."
    local data_dirs=(
        "hadoop-data"
        "hive-warehouse" 
        "postgres-data"
        "mysql-data"
        "logs/hadoop"
        "logs/hive"
        "logs/spark"
        "logs/airflow"
    )
    
    for dir in "${data_dirs[@]}"; do
        if [[ -d "$dir" ]]; then
            log_info "删除目录: $dir"
            rm -rf "$dir"
        fi
    done
    
    # 清理日志文件
    log_info "清理日志文件..."
    if [[ -d "logs" ]]; then
        find logs -name "*.log" -type f -delete 2>/dev/null || true
        find logs -name "*.out" -type f -delete 2>/dev/null || true
        find logs -name "*.err" -type f -delete 2>/dev/null || true
    fi
    
    # 清理临时文件
    log_info "清理临时文件..."
    rm -rf /tmp/hive* /tmp/hadoop* 2>/dev/null || true
    
    # 删除可能的 PID 文件
    find . -name "*.pid" -type f -delete 2>/dev/null || true
    
    log_info "✅ 数据重置完成！"
    echo
    log_info "现在可以重新启动服务来开始全新的数据处理："
    echo "  ./start-optimized.sh"
    echo
}

# 主函数
main() {
    log_info "大数据平台数据重置工具"
    
    # 解析命令行参数
    local force=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --force)
                force=true
                shift
                ;;
            -h|--help)
                echo "用法: $0 [选项]"
                echo "选项:"
                echo "  --force         跳过确认直接重置"
                echo "  -h, --help      显示帮助信息"
                echo
                echo "此脚本将删除所有 HDFS、Hive 和相关数据。"
                exit 0
                ;;
            *)
                log_error "未知参数: $1"
                echo "使用 -h 或 --help 查看帮助"
                exit 1
                ;;
        esac
    done
    
    # 检查 Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装或不在 PATH 中"
        exit 1
    fi
    
    show_warning
    
    # 确认操作
    if [[ "$force" != true ]]; then
        echo -n "确认要删除所有数据吗? 输入 'YES' 继续: "
        read -r confirmation
        if [[ "$confirmation" != "YES" ]]; then
            log_info "操作已取消"
            exit 0
        fi
    fi
    
    reset_all_data
}

# 捕获中断信号
trap 'log_error "操作被中断"; exit 1' INT TERM

# 运行主函数
main "$@"