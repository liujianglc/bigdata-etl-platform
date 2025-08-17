#!/bin/bash

# HDFS目录初始化脚本
# 创建ETL流程所需的所有目录结构

set -e

echo "开始初始化HDFS目录结构..."

# 等待HDFS完全启动
echo "等待HDFS服务启动..."
for i in {1..30}; do
    if hdfs dfsadmin -report > /dev/null 2>&1; then
        echo "HDFS服务已启动"
        break
    fi
    echo "等待HDFS启动... ($i/30)"
    sleep 10
done

# 检查HDFS是否可用
if ! hdfs dfsadmin -report > /dev/null 2>&1; then
    echo "错误: HDFS服务未启动"
    exit 1
fi

# 检查是否已经初始化过
if hdfs dfs -test -e /user/bigdata/wudeli/.hdfs_initialized 2>/dev/null; then
    echo "HDFS目录已经初始化过，跳过初始化步骤"
    echo "如需重新初始化，请先删除标记文件: hdfs dfs -rm /user/bigdata/wudeli/.hdfs_initialized"
    exit 0
fi

# 安全创建目录的函数
create_dir_safe() {
    local dir_path=$1
    if ! hdfs dfs -test -d "$dir_path" 2>/dev/null; then
        echo "创建目录: $dir_path"
        hdfs dfs -mkdir -p "$dir_path"
    else
        echo "目录已存在: $dir_path"
    fi
}

# 创建基础目录结构
echo "创建基础目录结构..."

# 创建用户目录
create_dir_safe "/user"
create_dir_safe "/user/bigdata"
create_dir_safe "/user/bigdata/wudeli"

# 创建ETL流程目录
echo "创建ETL流程目录..."
create_dir_safe "/user/bigdata/wudeli/raw"
create_dir_safe "/user/bigdata/wudeli/processed"
create_dir_safe "/user/bigdata/wudeli/reports"
create_dir_safe "/user/bigdata/wudeli/temp"
create_dir_safe "/user/bigdata/wudeli/archive"

# 创建分区目录结构
echo "创建分区目录结构..."
create_dir_safe "/user/bigdata/wudeli/raw/orders"
create_dir_safe "/user/bigdata/wudeli/processed/orders"
create_dir_safe "/user/bigdata/wudeli/processed/analytics"

# 创建Hive仓库目录
echo "创建Hive仓库目录..."
create_dir_safe "/user/hive"
create_dir_safe "/user/hive/warehouse"
create_dir_safe "/user/hive/warehouse/wudeli_analytics.db"

# 创建日志目录
echo "创建日志目录..."
create_dir_safe "/user/bigdata/logs"
create_dir_safe "/user/bigdata/logs/etl"
create_dir_safe "/user/bigdata/logs/spark"

# 设置目录权限
echo "设置目录权限..."
hdfs dfs -chmod -R 755 /user/bigdata
hdfs dfs -chmod -R 777 /user/hive/warehouse
hdfs dfs -chmod -R 755 /user/bigdata/logs

# 创建测试文件验证写入权限
echo "验证HDFS写入权限..."
test_file="/user/bigdata/wudeli/temp/init_test.txt"
if hdfs dfs -test -e "$test_file" 2>/dev/null; then
    echo "删除已存在的测试文件: $test_file"
    hdfs dfs -rm "$test_file"
fi
echo "HDFS初始化完成 - $(date)" | hdfs dfs -put - "$test_file"
echo "✓ HDFS写入权限验证成功"

# 显示目录结构
echo "HDFS目录结构创建完成:"
hdfs dfs -ls -R /user/bigdata/wudeli | head -20

echo "HDFS初始化完成！"

# 创建初始化标记文件
echo "$(date): HDFS directories initialized successfully" | hdfs dfs -put - /user/bigdata/wudeli/.hdfs_initialized

echo "所有HDFS目录初始化完成，可以开始ETL作业"