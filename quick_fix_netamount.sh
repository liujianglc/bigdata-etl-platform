#!/bin/bash

# 快速修复NetAmount schema问题的脚本
# 自动启动服务、复制脚本并执行修复

echo "🚀 NetAmount Schema 快速修复工具"
echo "=================================="
echo ""

# 检查Docker是否可用
if ! command -v docker &> /dev/null; then
    echo "❌ Docker未安装或不在PATH中"
    exit 1
fi

# 启动Docker服务 (如果需要)
if ! docker ps >/dev/null 2>&1; then
    echo "🔧 Docker服务未运行，尝试启动..."
    # 在macOS上尝试启动Docker Desktop
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open -a Docker
        echo "⏳ 等待Docker启动..."
        sleep 10
        
        # 等待Docker启动
        for i in {1..30}; do
            if docker ps >/dev/null 2>&1; then
                echo "✅ Docker已启动"
                break
            fi
            echo "⏳ 等待Docker启动... ($i/30)"
            sleep 2
        done
        
        if ! docker ps >/dev/null 2>&1; then
            echo "❌ Docker启动失败，请手动启动Docker Desktop"
            exit 1
        fi
    else
        echo "❌ 请手动启动Docker服务"
        exit 1
    fi
fi

echo "✅ Docker服务运行正常"

# 启动项目服务
echo "🚀 启动项目服务..."
if ! docker compose ps | grep -q "spark-master.*Up"; then
    echo "📦 启动Spark和相关服务..."
    docker compose up -d
    
    echo "⏳ 等待服务启动..."
    sleep 15
    
    # 等待服务完全启动
    for i in {1..20}; do
        if docker compose ps | grep -q "spark-master.*Up"; then
            echo "✅ 服务启动成功"
            break
        fi
        echo "⏳ 等待服务启动... ($i/20)"
        sleep 3
    done
    
    if ! docker compose ps | grep -q "spark-master.*Up"; then
        echo "❌ 服务启动失败"
        echo "请检查docker-compose.yaml配置或手动启动: docker compose up -d"
        exit 1
    fi
else
    echo "✅ 服务已在运行"
fi

# 检查修复脚本是否在正确位置
echo "📦 检查修复脚本..."
if [ ! -f "./spark_jobs/detect_and_fix_schema_issues.py" ]; then
    echo "⚠️  修复脚本不在spark_jobs目录中，正在移动..."
    mv detect_and_fix_schema_issues.py fix_specific_partition.py fix_netamount_schema.py ./spark_jobs/ 2>/dev/null || echo "❌ 移动失败，请手动移动脚本到spark_jobs目录"
fi

# 验证脚本是否在容器中可用
if docker compose exec spark-master test -f /opt/spark-jobs/detect_and_fix_schema_issues.py; then
    echo "✅ 修复脚本已在容器中可用"
else
    echo "❌ 修复脚本不可用"
    echo "请确保脚本在spark_jobs目录中:"
    echo "  mv detect_and_fix_schema_issues.py fix_specific_partition.py fix_netamount_schema.py ./spark_jobs/"
    exit 1
fi

echo ""
echo "🎯 开始执行NetAmount schema修复..."
echo "选择修复方式:"
echo "1. 自动检测并修复所有问题分区 (推荐)"
echo "2. 修复特定问题分区 (dt=2025-09-07, dt=2025-09-08)"
echo "3. 仅删除问题分区，重新生成数据"
echo ""
read -p "请输入选择 (1-3): " choice

case $choice in
    1)
        echo "🔍 自动检测并修复所有问题分区..."
        docker compose exec spark-master python /opt/spark-jobs/detect_and_fix_schema_issues.py
        ;;
    2)
        echo "🎯 修复特定问题分区..."
        docker compose exec spark-master python /opt/spark-jobs/fix_specific_partition.py
        ;;
    3)
        echo "🗑️  删除问题分区..."
        docker compose exec spark-master spark-sql \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            -e "ALTER TABLE dwd_db.dwd_orders DROP IF EXISTS PARTITION (dt='2025-09-07'); ALTER TABLE dwd_db.dwd_orders DROP IF EXISTS PARTITION (dt='2025-09-08');"
        
        echo "✅ 问题分区已删除"
        echo "💡 请重新运行dwd_orders_pipeline DAG来重新生成数据"
        ;;
    *)
        echo "❌ 无效选择"
        exit 1
        ;;
esac

echo ""
echo "🎉 修复操作完成!"
echo ""
echo "📋 后续步骤:"
echo "1. 重新运行失败的Spark作业"
echo "2. 检查ETL管道是否正常运行"
echo "3. 验证数据质量"
echo ""
echo "💡 验证修复结果:"
echo "  docker compose exec spark-master spark-sql -e \"DESCRIBE dwd_db.dwd_orders;\""
echo "  docker compose exec airflow-webserver airflow dags trigger dws_orders_analytics"
