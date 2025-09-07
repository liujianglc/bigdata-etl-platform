#!/bin/bash
"""
重复数据清理执行脚本
"""

# 设置环境变量
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH

# 配置参数
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLEANUP_SCRIPT="$SCRIPT_DIR/cleanup_duplicate_data.py"

# 默认参数
CLEANUP_DAYS=${CLEANUP_DAYS:-30}
ANALYZE_ONLY=${ANALYZE_ONLY:-false}

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# 显示帮助信息
show_help() {
    cat << EOF
重复数据清理脚本

用法:
    $0 [选项]

选项:
    -d, --days DAYS         检查最近多少天的数据 (默认: 30)
    -a, --analyze-only      仅分析重复数据，不执行清理 (默认: false)
    -h, --help             显示此帮助信息

示例:
    # 仅分析最近7天的重复数据
    $0 --analyze-only --days 7
    
    # 清理最近30天的重复数据
    $0 --days 30
    
    # 使用环境变量
    CLEANUP_DAYS=7 ANALYZE_ONLY=true $0

EOF
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--days)
            CLEANUP_DAYS="$2"
            shift 2
            ;;
        -a|--analyze-only)
            ANALYZE_ONLY="true"
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo_error "未知参数: $1"
            show_help
            exit 1
            ;;
    esac
done

# 验证参数
if ! [[ "$CLEANUP_DAYS" =~ ^[0-9]+$ ]] || [ "$CLEANUP_DAYS" -lt 1 ]; then
    echo_error "CLEANUP_DAYS 必须是正整数"
    exit 1
fi

# 检查Python脚本是否存在
if [ ! -f "$CLEANUP_SCRIPT" ]; then
    echo_error "清理脚本不存在: $CLEANUP_SCRIPT"
    exit 1
fi

# 显示配置信息
echo_info "重复数据清理配置:"
echo_info "  检查天数: $CLEANUP_DAYS"
echo_info "  仅分析模式: $ANALYZE_ONLY"
echo_info "  脚本路径: $CLEANUP_SCRIPT"
echo ""

# 确认执行
if [ "$ANALYZE_ONLY" = "false" ]; then
    echo_warn "⚠️  即将执行数据清理操作，这将修改现有数据！"
    echo_warn "建议先运行分析模式: $0 --analyze-only --days $CLEANUP_DAYS"
    echo ""
    read -p "确认继续执行清理操作? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo_info "操作已取消"
        exit 0
    fi
fi

# 设置环境变量并执行Python脚本
echo_info "开始执行重复数据清理..."
export CLEANUP_DAYS
export ANALYZE_ONLY

# 创建日志目录
LOG_DIR="/opt/airflow/logs/cleanup"
mkdir -p "$LOG_DIR"

# 生成日志文件名
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/cleanup_${TIMESTAMP}.log"

echo_info "日志文件: $LOG_FILE"

# 执行清理脚本
if python3 "$CLEANUP_SCRIPT" 2>&1 | tee "$LOG_FILE"; then
    echo_success "重复数据清理完成！"
    echo_info "详细日志请查看: $LOG_FILE"
else
    echo_error "重复数据清理失败！"
    echo_info "错误日志请查看: $LOG_FILE"
    exit 1
fi

