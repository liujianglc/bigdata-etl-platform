#!/bin/bash

# NetAmount Schema修复脚本
# 修复Parquet列类型不匹配问题

echo "🔧 开始修复NetAmount列的schema问题..."
echo "问题: Parquet column cannot be converted - NetAmount Expected: decimal(10,2), Found: INT64"
echo ""

# 检查Docker容器是否运行
echo "📋 检查服务状态..."
if ! docker-compose ps | grep -q "spark-master.*Up"; then
    echo "❌ Spark服务未运行，请先启动服务:"
    echo "   docker-compose up -d"
    exit 1
fi

echo "✅ Spark服务正在运行"
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
        docker compose exec spark-master python /opt/airflow/detect_and_fix_schema_issues.py
        ;;
    2)
        echo "🎯 修复特定问题分区..."
        docker compose exec spark-master python /opt/airflow/fix_specific_partition.py
        ;;
    3)
        echo "🔧 修复所有相关表..."
        docker compose exec spark-master python /opt/airflow/fix_netamount_schema.py
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
