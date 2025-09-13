#!/usr/bin/env python3
"""
修复特定分区的NetAmount列schema问题

专门处理错误消息中提到的问题分区：
hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders/dt=2025-09-07/part-00005-adc4d030-a5d5-4bb6-a0b4-c05b623c1f8c.c000.snappy.parquet
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fix_specific_partition():
    """修复特定分区的NetAmount列schema问题"""
    
    # 创建Spark会话
    spark = SparkSession.builder \
        .appName("FixSpecificPartitionNetAmount") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # 问题分区列表 - 添加所有有问题的分区
        problem_partitions = [
            "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders/dt=2025-09-07",
            "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders/dt=2025-09-08"
        ]
        
        logging.info(f"🔧 开始修复 {len(problem_partitions)} 个问题分区")
        
        successful_fixes = 0
        failed_partitions = []
        
        for problem_partition in problem_partitions:
            partition_date = problem_partition.split('dt=')[1]
            logging.info(f"\n📅 处理分区: {partition_date}")
            logging.info(f"📂 分区路径: {problem_partition}")
            
            # 读取问题分区
            try:
                df = spark.read.parquet(problem_partition)
                logging.info(f"📊 成功读取分区，行数: {df.count()}")
                
                # 检查NetAmount列的当前类型
                netamount_field = [field for field in df.schema.fields if field.name == 'NetAmount'][0]
                current_type = str(netamount_field.dataType)
                
                logging.info(f"📊 当前NetAmount类型: {current_type}")
                
                # 如果已经是正确类型，跳过
                if 'decimal(10,2)' in current_type.lower():
                    logging.info(f"✅ 分区 {partition_date} 的NetAmount类型已经正确，跳过")
                    successful_fixes += 1
                    continue
                
                # 显示一些样本数据
                sample_data = df.select("OrderID", "NetAmount", "TotalAmount", "Discount").limit(3).collect()
                logging.info("📝 样本数据:")
                for row in sample_data:
                    logging.info(f"  OrderID: {row['OrderID']}, NetAmount: {row['NetAmount']}, TotalAmount: {row['TotalAmount']}, Discount: {row['Discount']}")
                
                # 转换NetAmount列类型
                logging.info(f"🔄 转换NetAmount列类型为decimal(10,2)")
                df_fixed = df.withColumn("NetAmount", col("NetAmount").cast(DecimalType(10,2)))
                
                # 验证转换后的数据
                sample_fixed = df_fixed.select("OrderID", "NetAmount").limit(3).collect()
                logging.info(f"📝 转换后的样本数据:")
                for row in sample_fixed:
                    logging.info(f"  OrderID: {row['OrderID']}, NetAmount: {row['NetAmount']} (type: {type(row['NetAmount'])})")
                
                # 创建备份路径
                backup_partition = f"{problem_partition}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # 备份原始分区
                logging.info(f"📦 备份原始分区到: {backup_partition}")
                df.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(backup_partition)
                
                # 写入修复后的数据
                logging.info(f"💾 写入修复后的数据")
                df_fixed.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(problem_partition)
                
                # 验证修复结果
                df_verify = spark.read.parquet(problem_partition)
                verify_count = df_verify.count()
                original_count = df.count()
                
                # 检查修复后的schema
                netamount_field_fixed = [field for field in df_verify.schema.fields if field.name == 'NetAmount'][0]
                fixed_type = str(netamount_field_fixed.dataType)
                
                logging.info(f"✅ 修复后NetAmount类型: {fixed_type}")
                
                if verify_count == original_count and 'decimal(10,2)' in fixed_type.lower():
                    logging.info(f"✅ 分区 {partition_date} 修复成功!")
                    logging.info(f"  - 原始行数: {original_count}")
                    logging.info(f"  - 修复后行数: {verify_count}")
                    logging.info(f"  - 类型已修复: {fixed_type}")
                    successful_fixes += 1
                    
                else:
                    logging.error(f"❌ 分区 {partition_date} 修复验证失败:")
                    logging.error(f"  - 原始行数: {original_count}, 修复后行数: {verify_count}")
                    logging.error(f"  - 修复后类型: {fixed_type}")
                    failed_partitions.append(partition_date)
                    
            except Exception as read_error:
                logging.error(f"❌ 读取分区 {partition_date} 时出错: {str(read_error)}")
                
                # 尝试删除有问题的分区并重新生成
                logging.info(f"🗑️  尝试删除有问题的分区 {partition_date}...")
                try:
                    # 删除分区数据
                    spark.sql(f"ALTER TABLE dwd_db.dwd_orders DROP IF EXISTS PARTITION (dt='{partition_date}')")
                    logging.info(f"✅ 已删除有问题的分区 {partition_date}")
                    failed_partitions.append(f"{partition_date} (已删除)")
                    
                except Exception as drop_error:
                    logging.error(f"❌ 删除分区 {partition_date} 时出错: {str(drop_error)}")
                    failed_partitions.append(f"{partition_date} (删除失败)")
        
        # 刷新Hive表元数据
        if successful_fixes > 0:
            logging.info("\n🔄 刷新Hive表元数据")
            spark.sql("REFRESH TABLE dwd_db.dwd_orders")
            spark.sql("ANALYZE TABLE dwd_db.dwd_orders COMPUTE STATISTICS")
        
        # 总结修复结果
        logging.info(f"\n📋 修复总结:")
        logging.info(f"  - 成功修复: {successful_fixes} 个分区")
        logging.info(f"  - 失败分区: {len(failed_partitions)} 个")
        
        if failed_partitions:
            logging.info(f"  - 失败分区列表: {', '.join(failed_partitions)}")
            logging.info("💡 建议重新运行dwd_orders_pipeline DAG来重新生成失败分区的数据")
        
        if successful_fixes > 0:
            logging.info("🎉 修复完成! 现在可以重新运行失败的Spark作业了。")
        
    except Exception as e:
        logging.error(f"❌ 修复过程中出现错误: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    fix_specific_partition()
