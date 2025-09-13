#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复NetAmount列的schema不匹配问题

这个脚本会：
1. 读取有问题的Parquet文件
2. 将NetAmount列从INT64转换为decimal(10,2)
3. 重新写入文件，保持正确的schema
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fix_netamount_schema():
    """修复NetAmount列的schema问题"""
    
    # 创建Spark会话
    spark = SparkSession.builder \
        .appName("FixNetAmountSchema") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        # 需要修复的表和分区
        tables_to_fix = [
            {
                'table': 'dwd_db.dwd_orders',
                'path': 'hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders'
            },
            {
                'table': 'dwd_db.dwd_orderdetails', 
                'path': 'hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orderdetails'
            }
        ]
        
        for table_info in tables_to_fix:
            table_name = table_info['table']
            table_path = table_info['path']
            
            logging.info(f"🔧 开始修复表: {table_name}")
            
            try:
                # 读取表数据
                df = spark.read.parquet(table_path)
                
                # 检查是否存在NetAmount列
                if 'NetAmount' not in df.columns:
                    logging.info(f"⚠️  表 {table_name} 中没有NetAmount列，跳过")
                    continue
                
                # 检查NetAmount列的当前类型
                netamount_field = [field for field in df.schema.fields if field.name == 'NetAmount'][0]
                current_type = str(netamount_field.dataType)
                
                logging.info(f"📊 当前NetAmount类型: {current_type}")
                
                # 如果已经是正确的类型，跳过
                if 'decimal(10,2)' in current_type.lower():
                    logging.info(f"✅ 表 {table_name} 的NetAmount列类型已经正确")
                    continue
                
                # 转换NetAmount列类型
                logging.info(f"🔄 转换NetAmount列类型为decimal(10,2)")
                df_fixed = df.withColumn("NetAmount", col("NetAmount").cast(DecimalType(10,2)))
                
                # 验证转换后的数据
                sample_data = df_fixed.select("NetAmount").limit(5).collect()
                logging.info(f"📝 转换后的样本数据: {[row['NetAmount'] for row in sample_data]}")
                
                # 创建临时表路径
                temp_path = f"{table_path}_temp_fixed"
                
                # 写入修复后的数据到临时路径
                logging.info(f"💾 写入修复后的数据到临时路径: {temp_path}")
                df_fixed.write \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .parquet(temp_path)
                
                # 验证修复后的数据
                df_verify = spark.read.parquet(temp_path)
                verify_count = df_verify.count()
                original_count = df.count()
                
                if verify_count == original_count:
                    logging.info(f"✅ 数据验证成功: 原始行数={original_count}, 修复后行数={verify_count}")
                    
                    # 备份原始数据
                    backup_path = f"{table_path}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    logging.info(f"📦 备份原始数据到: {backup_path}")
                    
                    # 使用Hadoop命令移动文件（更安全）
                    spark.sql(f"CREATE TABLE IF NOT EXISTS temp_backup_table USING PARQUET LOCATION '{backup_path}'")
                    df.write.mode("overwrite").parquet(backup_path)
                    
                    # 替换原始数据
                    logging.info(f"🔄 替换原始数据")
                    df_fixed.write \
                        .mode("overwrite") \
                        .option("compression", "snappy") \
                        .parquet(table_path)
                    
                    # 清理临时文件
                    logging.info(f"🧹 清理临时文件: {temp_path}")
                    spark.sql(f"DROP TABLE IF EXISTS temp_table_{table_name.replace('.', '_')}")
                    
                    logging.info(f"✅ 表 {table_name} 修复完成")
                    
                else:
                    logging.error(f"❌ 数据验证失败: 原始行数={original_count}, 修复后行数={verify_count}")
                    raise Exception("数据行数不匹配")
                    
            except Exception as e:
                logging.error(f"❌ 修复表 {table_name} 时出错: {str(e)}")
                continue
        
        # 刷新Hive元数据
        logging.info("🔄 刷新Hive元数据")
        for table_info in tables_to_fix:
            table_name = table_info['table']
            try:
                spark.sql(f"REFRESH TABLE {table_name}")
                spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
                logging.info(f"✅ 已刷新表 {table_name} 的元数据")
            except Exception as e:
                logging.warning(f"⚠️  刷新表 {table_name} 元数据时出错: {str(e)}")
        
        logging.info("🎉 NetAmount schema修复完成!")
        
    except Exception as e:
        logging.error(f"❌ 修复过程中出现错误: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    fix_netamount_schema()