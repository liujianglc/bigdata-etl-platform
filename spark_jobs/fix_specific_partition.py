#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
        
        logging.info("开始修复 %d 个问题分区", len(problem_partitions))
        
        successful_fixes = 0
        failed_partitions = []
        
        for problem_partition in problem_partitions:
            partition_date = problem_partition.split('dt=')[1]
            logging.info("处理分区: %s", partition_date)
            logging.info("分区路径: %s", problem_partition)
            
            # 读取问题分区
            try:
                df = spark.read.parquet(problem_partition)
                logging.info("成功读取分区，行数: %d", df.count())
                
                # 检查NetAmount列的当前类型
                netamount_field = [field for field in df.schema.fields if field.name == 'NetAmount'][0]
                current_type = str(netamount_field.dataType)
                
                logging.info("当前NetAmount类型: %s", current_type)
                
                # 如果已经是正确类型，跳过
                if 'decimal(10,2)' in current_type.lower():
                    logging.info("分区 %s 的NetAmount类型已经正确，跳过", partition_date)
                    successful_fixes += 1
                    continue
                
                # 显示一些样本数据
                sample_data = df.select("OrderID", "NetAmount", "TotalAmount", "Discount").limit(3).collect()
                logging.info("样本数据:")
                for row in sample_data:
                    logging.info("  OrderID: %s, NetAmount: %s, TotalAmount: %s, Discount: %s", 
                               str(row['OrderID']), str(row['NetAmount']), 
                               str(row['TotalAmount']), str(row['Discount']))
                
                # 转换NetAmount列类型
                logging.info("转换NetAmount列类型为decimal(10,2)")
                df_fixed = df.withColumn("NetAmount", col("NetAmount").cast(DecimalType(10,2)))
                
                # 验证转换后的数据
                sample_fixed = df_fixed.select("OrderID", "NetAmount").limit(3).collect()
                logging.info("转换后的样本数据:")
                for row in sample_fixed:
                    logging.info("  OrderID: %s, NetAmount: %s (type: %s)", 
                               str(row['OrderID']), str(row['NetAmount']), str(type(row['NetAmount'])))
                
                # 创建备份路径
                backup_partition = problem_partition + "_backup_" + datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # 备份原始分区
                logging.info("备份原始分区到: %s", backup_partition)
                df.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(backup_partition)
                
                # 写入修复后的数据
                logging.info("写入修复后的数据")
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
                
                logging.info("修复后NetAmount类型: %s", fixed_type)
                
                if verify_count == original_count and 'decimal(10,2)' in fixed_type.lower():
                    logging.info("分区 %s 修复成功!", partition_date)
                    logging.info("  - 原始行数: %d", original_count)
                    logging.info("  - 修复后行数: %d", verify_count)
                    logging.info("  - 类型已修复: %s", fixed_type)
                    successful_fixes += 1
                    
                else:
                    logging.error("分区 %s 修复验证失败:", partition_date)
                    logging.error("  - 原始行数: %d, 修复后行数: %d", original_count, verify_count)
                    logging.error("  - 修复后类型: %s", fixed_type)
                    failed_partitions.append(partition_date)
                    
            except Exception as read_error:
                logging.error("读取分区 %s 时出错: %s", partition_date, str(read_error))
                
                # 尝试删除有问题的分区并重新生成
                logging.info("尝试删除有问题的分区 %s...", partition_date)
                try:
                    # 删除分区数据
                    drop_sql = "ALTER TABLE dwd_db.dwd_orders DROP IF EXISTS PARTITION (dt='{}')".format(partition_date)
                    spark.sql(drop_sql)
                    logging.info("已删除有问题的分区 %s", partition_date)
                    failed_partitions.append(partition_date + " (已删除)")
                    
                except Exception as drop_error:
                    logging.error("删除分区 %s 时出错: %s", partition_date, str(drop_error))
                    failed_partitions.append(partition_date + " (删除失败)")
        
        # 刷新Hive表元数据
        if successful_fixes > 0:
            logging.info("刷新Hive表元数据")
            spark.sql("REFRESH TABLE dwd_db.dwd_orders")
            spark.sql("ANALYZE TABLE dwd_db.dwd_orders COMPUTE STATISTICS")
        
        # 总结修复结果
        logging.info("修复总结:")
        logging.info("  - 成功修复: %d 个分区", successful_fixes)
        logging.info("  - 失败分区: %d 个", len(failed_partitions))
        
        if failed_partitions:
            logging.info("  - 失败分区列表: %s", ', '.join(failed_partitions))
            logging.info("建议重新运行dwd_orders_pipeline DAG来重新生成失败分区的数据")
        
        if successful_fixes > 0:
            logging.info("修复完成! 现在可以重新运行失败的Spark作业了。")
        
    except Exception as e:
        logging.error("修复过程中出现错误: %s", str(e))
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    fix_specific_partition()