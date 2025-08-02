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

# 创建基础目录结构
echo "创建基础目录结构..."

# 创建用户目录
hdfs dfs -mkdir -p /user
hdfs dfs -mkdir -p /user/bigdata
hdfs dfs -mkdir -p /user/bigdata/wudeli

# 创建ETL流程目录
echo "创建ETL流程目录..."
hdfs dfs -mkdir -p /user/bigdata/wudeli/raw
hdfs dfs -mkdir -p /user/bigdata/wudeli/processed
hdfs dfs -mkdir -p /user/bigdata/wudeli/reports
hdfs dfs -mkdir -p /user/bigdata/wudeli/temp
hdfs dfs -mkdir -p /user/bigdata/wudeli/archive

# 创建分区目录结构
echo "创建分区目录结构..."
hdfs dfs -mkdir -p /user/bigdata/wudeli/raw/orders
hdfs dfs -mkdir -p /user/bigdata/wudeli/processed/orders
hdfs dfs -mkdir -p /user/bigdata/wudeli/processed/analytics

# 创建Hive仓库目录
echo "创建Hive仓库目录..."
hdfs dfs -mkdir -p /user/hive
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/hive/warehouse/wudeli_analytics.db

# 创建日志目录
echo "创建日志目录..."
hdfs dfs -mkdir -p /user/bigdata/logs
hdfs dfs -mkdir -p /user/bigdata/logs/etl
hdfs dfs -mkdir -p /user/bigdata/logs/spark

# 设置目录权限
echo "设置目录权限..."
hdfs dfs -chmod -R 755 /user/bigdata
hdfs dfs -chmod -R 777 /user/hive/warehouse
hdfs dfs -chmod -R 755 /user/bigdata/logs

# 创建测试文件验证写入权限
echo "验证HDFS写入权限..."
echo "HDFS初始化完成 - $(date)" | hdfs dfs -put - /user/bigdata/wudeli/temp/init_test.txt

# 显示目录结构
echo "HDFS目录结构创建完成:"
hdfs dfs -ls -R /user/bigdata/wudeli | head -20

echo "HDFS初始化完成！"

# 创建初始化标记文件
echo "$(date): HDFS directories initialized successfully" | hdfs dfs -put - /user/bigdata/wudeli/.hdfs_initialized

echo "所有HDFS目录初始化完成，可以开始ETL作业"