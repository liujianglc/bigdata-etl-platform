#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动检测和修复所有NetAmount列schema问题的分区

这个脚本会：
1. 扫描dwd_orders表的所有分区
2. 检测NetAmount列类型不匹配的分区
3. 批量修复所有有问题的分区
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from datetime import datetime, timedelta
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def detect_and_fix_schema_issues():
    """自动检测和修复所有NetAmount列schema问题"""
    
    # 创建Spark会话
    spark = SparkSession.builder \
        .appName("DetectAndFixNetAmountSchema") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        logging.info("检测NetAmount列schema问题...")
        
        # 获取所有分区
        partitions_df = spark.sql("SHOW PARTITIONS dwd_db.dwd_orders")
        partitions = [row[0] for row in partitions_df.collect()]
        
        logging.info("找到 %d 个分区", len(partitions))
        
        problem_partitions = []
        good_partitions = []
        
        # 检测每个分区的schema
        for partition in partitions:
            # 提取分区日期
            partition_date = partition.split('=')[1]
            partition_path = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders/" + partition
            
            try:
                # 读取分区schema（不读取数据，只读取schema）
                df = spark.read.parquet(partition_path)
                
                # 检查NetAmount列类型
                netamount_field = [field for field in df.schema.fields if field.name == 'NetAmount'][0]
                current_type = str(netamount_field.dataType)
                
                if 'decimal(10,2)' in current_type.lower():
                    good_partitions.append(partition_date)
                    logging.info("分区 %s: NetAmount类型正确 (%s)", partition_date, current_type)
                else:
                    problem_partitions.append((partition_date, partition_path, current_type))
                    logging.warning("分区 %s: NetAmount类型错误 (%s)", partition_date, current_type)
                    
            except Exception as e:
                logging.error("检测分区 %s 时出错: %s", partition_date, str(e))
                problem_partitions.append((partition_date, partition_path, "ERROR"))
        
        logging.info("检测结果:")
        logging.info("  - 正常分区: %d 个", len(good_partitions))
        logging.info("  - 问题分区: %d 个", len(problem_partitions))
        
        if not problem_partitions:
            logging.info("所有分区的NetAmount列类型都正确!")
            return
        
        logging.info("开始修复 %d 个问题分区...", len(problem_partitions))
        
        successful_fixes = 0
        failed_partitions = []
        
        for partition_date, partition_path, current_type in problem_partitions:
            logging.info("处理分区: %s", partition_date)
            logging.info("分区路径: %s", partition_path)
            logging.info("当前类型: %s", current_type)
            
            if current_type == "ERROR":
                # 如果读取时就出错，直接删除分区
                logging.info("分区 %s 存在读取错误，删除并重新生成", partition_date)
                try:
                    drop_sql = "ALTER TABLE dwd_db.dwd_orders DROP IF EXISTS PARTITION (dt='{}')".format(partition_date)
                    spark.sql(drop_sql)
                    logging.info("已删除有问题的分区 %s", partition_date)
                    failed_partitions.append(partition_date + " (已删除-读取错误)")
                except Exception as drop_error:
                    logging.error("删除分区 %s 时出错: %s", partition_date, str(drop_error))
                    failed_partitions.append(partition_date + " (删除失败)")
                continue
            
            try:
                # 读取分区数据
                df = spark.read.parquet(partition_path)
                row_count = df.count()
                logging.info("分区行数: %d", row_count)
                
                if row_count == 0:
                    logging.info("分区 %s 为空，跳过修复", partition_date)
                    successful_fixes += 1
                    continue
                
                # 显示样本数据
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
                    logging.info("  OrderID: %s, NetAmount: %s", str(row['OrderID']), str(row['NetAmount']))
                
                # 创建备份路径
                backup_path = partition_path + "_backup_" + datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # 备份原始分区
                logging.info("备份原始分区到: %s", backup_path)
                df.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(backup_path)
                
                # 写入修复后的数据
                logging.info("写入修复后的数据")
                df_fixed.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(partition_path)
                
                # 验证修复结果
                df_verify = spark.read.parquet(partition_path)
                verify_count = df_verify.count()
                
                # 检查修复后的schema
                netamount_field_fixed = [field for field in df_verify.schema.fields if field.name == 'NetAmount'][0]
                fixed_type = str(netamount_field_fixed.dataType)
                
                logging.info("修复后NetAmount类型: %s", fixed_type)
                
                if verify_count == row_count and 'decimal(10,2)' in fixed_type.lower():
                    logging.info("分区 %s 修复成功!", partition_date)
                    logging.info("  - 行数: %d", verify_count)
                    logging.info("  - 类型: %s", fixed_type)
                    successful_fixes += 1
                else:
                    logging.error("分区 %s 修复验证失败:", partition_date)
                    logging.error("  - 原始行数: %d, 修复后行数: %d", row_count, verify_count)
                    logging.error("  - 修复后类型: %s", fixed_type)
                    failed_partitions.append(partition_date)
                    
            except Exception as fix_error:
                logging.error("修复分区 %s 时出错: %s", partition_date, str(fix_error))
                
                # 尝试删除有问题的分区
                logging.info("尝试删除有问题的分区 %s...", partition_date)
                try:
                    drop_sql = "ALTER TABLE dwd_db.dwd_orders DROP IF EXISTS PARTITION (dt='{}')".format(partition_date)
                    spark.sql(drop_sql)
                    logging.info("已删除有问题的分区 %s", partition_date)
                    failed_partitions.append(partition_date + " (已删除-修复失败)")
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
        logging.info("  - 检测到问题分区: %d 个", len(problem_partitions))
        logging.info("  - 成功修复: %d 个分区", successful_fixes)
        logging.info("  - 失败/删除分区: %d 个", len(failed_partitions))
        
        if failed_partitions:
            logging.info("  - 失败分区列表: %s", ', '.join(failed_partitions))
            logging.info("建议重新运行dwd_orders_pipeline DAG来重新生成失败分区的数据")
        
        if successful_fixes > 0:
            logging.info("修复完成! 现在可以重新运行失败的Spark作业了。")
        
        # 提供后续建议
        logging.info("后续建议:")
        logging.info("1. 重新运行失败的DWS分析作业")
        logging.info("2. 检查ETL管道确保新数据使用正确的类型转换")
        logging.info("3. 监控后续分区是否还会出现类似问题")
        
    except Exception as e:
        logging.error("检测和修复过程中出现错误: %s", str(e))
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    detect_and_fix_schema_issues()