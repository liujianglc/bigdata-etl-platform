#!/bin/bash

# NetAmount Schema修复脚本
# 修复Parquet列类型不匹配问题

echo "🔧 开始修复NetAmount列的schema问题..."
echo "问题: Parquet column cannot be converted - NetAmount Expected: decimal(10,2), Found: INT64"
echo ""

# 检查Docker服务状态
echo "📋 检查Docker服务状态..."
if ! docker ps >/dev/null 2>&1; then
    echo "❌ Docker服务未运行，请先启动Docker服务"
    echo ""
    echo "🔧 手动修复步骤:"
    echo "1. 启动Docker服务"
    echo "2. 启动项目服务: docker compose up -d"
    echo "3. 将修复脚本复制到容器: docker compose cp detect_and_fix_schema_issues.py spark-master:/opt/airflow/"
    echo "4. 重新运行此脚本"
    echo ""
    echo "或者直接在本地运行修复脚本 (需要配置Spark环境):"
    echo "   python detect_and_fix_schema_issues.py"
    exit 1
fi

# 检查容器是否运行
if ! docker compose ps | grep -q "spark-master.*Up"; then
    echo "❌ Spark服务未运行，请先启动服务:"
    echo "   docker compose up -d"
    echo ""
    echo "启动服务后，请将修复脚本复制到容器:"
    echo "   docker compose cp detect_and_fix_schema_issues.py spark-master:/opt/airflow/"
    echo "   docker compose cp fix_specific_partition.py spark-master:/opt/airflow/"
    echo "   docker compose cp fix_netamount_schema.py spark-master:/opt/airflow/"
    exit 1
fi

echo "✅ Docker和Spark服务正在运行"

# 检查修复脚本是否在容器中
echo "📋 检查修复脚本..."
if ! docker compose exec spark-master test -f /opt/spark-jobs/detect_and_fix_schema_issues.py; then
    echo "❌ 修复脚本不在spark_jobs目录中"
    echo "请将修复脚本移动到spark_jobs目录:"
    echo "  mv detect_and_fix_schema_issues.py fix_specific_partition.py fix_netamount_schema.py ./spark_jobs/"
    exit 1
else
    echo "✅ 修复脚本已在容器中可用"
fi
echo ""

# 选择修复方式
echo "请选择修复方式:"
echo "1. 自动检测并修复所有问题分区 (推荐)"
echo "2. 修复特定问题分区 (dt=2025-09-07, dt=2025-09-08)"
echo "3. 修复所有相关表的NetAmount列"
echo "4. 仅删除已知问题分区，重新生成数据"
echo ""
read -p "请输入选择 (1-4): " choice

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
        echo "🔧 修复所有相关表..."
        docker compose exec spark-master python /opt/spark-jobs/fix_netamount_schema.py
        ;;
    4)
        echo "🗑️  删除已知问题分区..."
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
echo "🎉 修复完成!"
echo ""
echo "📋 后续步骤:"
echo "1. 重新运行失败的Spark作业"
echo "2. 检查ETL管道是否正常运行"
echo "3. 验证数据质量"
echo ""
echo "💡 如果问题仍然存在，请检查:"
echo "   - Hive元数据是否已刷新"
echo "   - 是否还有其他分区存在类似问题"
echo "   - ETL管道中的数据类型转换是否正确"
