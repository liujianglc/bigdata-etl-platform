#!/usr/bin/env python3
"""
历史重复数据清理脚本
用于清理 DWD 层表中的重复数据
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, desc, count, max as spark_max
from pyspark.sql.window import Window

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_spark_session():
    """创建 Spark 会话"""
    return SparkSession.builder \
        .appName("DWD_Duplicate_Data_Cleanup") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

def analyze_duplicates(spark, table_name, primary_key, partition_days=30):
    """
    分析表中的重复数据情况
    
    Args:
        spark: Spark会话
        table_name: 表名 (如 'dwd_db.dwd_orders')
        primary_key: 主键字段名
        partition_days: 检查最近多少天的分区
    """
    logging.info(f"🔍 分析表 {table_name} 的重复数据...")
    
    try:
        # 获取最近的分区
        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        if not partitions:
            logging.warning(f"表 {table_name} 没有分区")
            return None
            
        # 获取最近 partition_days 天的分区
        recent_partitions = []
        today = datetime.now()
        for i in range(partition_days):
            date_str = (today - timedelta(days=i)).strftime('%Y-%m-%d')
            partition_str = f"dt={date_str}"
            if any(partition_str in str(p) for p in partitions):
                recent_partitions.append(date_str)
        
        if not recent_partitions:
            logging.warning(f"表 {table_name} 最近 {partition_days} 天没有数据")
            return None
            
        logging.info(f"检查分区: {recent_partitions}")
        
        # 分析每个分区的重复情况
        duplicate_stats = {}
        
        for partition_date in recent_partitions:
            logging.info(f"检查分区 dt={partition_date}...")
            
            # 统计总记录数和唯一记录数
            total_query = f"""
            SELECT COUNT(*) as total_count
            FROM {table_name}
            WHERE dt = '{partition_date}'
            """
            
            unique_query = f"""
            SELECT COUNT(*) as unique_count
            FROM (
                SELECT {primary_key}
                FROM {table_name}
                WHERE dt = '{partition_date}'
                GROUP BY {primary_key}
            ) t
            """
            
            total_count = spark.sql(total_query).collect()[0]['total_count']
            unique_count = spark.sql(unique_query).collect()[0]['unique_count']
            
            duplicate_count = total_count - unique_count
            duplicate_rate = duplicate_count / total_count if total_count > 0 else 0
            
            duplicate_stats[partition_date] = {
                'total_count': total_count,
                'unique_count': unique_count,
                'duplicate_count': duplicate_count,
                'duplicate_rate': duplicate_rate
            }
            
            if duplicate_count > 0:
                logging.warning(f"⚠️ 分区 dt={partition_date}: {duplicate_count} 条重复数据 (重复率: {duplicate_rate:.2%})")
            else:
                logging.info(f"✅ 分区 dt={partition_date}: 无重复数据")
        
        return duplicate_stats
        
    except Exception as e:
        logging.error(f"分析重复数据失败: {e}")
        return None

def backup_partition(spark, table_name, partition_date, backup_location):
    """
    备份分区数据
    
    Args:
        spark: Spark会话
        table_name: 表名
        partition_date: 分区日期
        backup_location: 备份位置
    """
    logging.info(f"📦 备份分区 {table_name} dt={partition_date}...")
    
    try:
        backup_path = f"{backup_location}/{table_name.replace('.', '_')}/dt={partition_date}"
        
        # 读取分区数据并备份
        df = spark.sql(f"SELECT * FROM {table_name} WHERE dt = '{partition_date}'")
        
        df.write \
          .mode("overwrite") \
          .format("parquet") \
          .save(backup_path)
        
        record_count = df.count()
        logging.info(f"✅ 备份完成: {record_count} 条记录 -> {backup_path}")
        return True
        
    except Exception as e:
        logging.error(f"备份失败: {e}")
        return False

def cleanup_duplicates(spark, table_name, primary_key, partition_date, backup_first=True):
    """
    清理指定分区的重复数据
    
    Args:
        spark: Spark会话
        table_name: 表名
        primary_key: 主键字段
        partition_date: 分区日期
        backup_first: 是否先备份
    """
    logging.info(f"🧹 清理分区 {table_name} dt={partition_date} 的重复数据...")
    
    try:
        # 备份数据
        if backup_first:
            backup_location = "hdfs://namenode:9000/backup/duplicate_cleanup"
            if not backup_partition(spark, table_name, partition_date, backup_location):
                logging.error("备份失败，停止清理操作")
                return False
        
        # 读取分区数据
        df = spark.sql(f"SELECT * FROM {table_name} WHERE dt = '{partition_date}'")
        initial_count = df.count()
        
        if initial_count == 0:
            logging.info("分区无数据，跳过清理")
            return True
        
        # 去重逻辑：保留最新的记录（基于 etl_created_date 或 UpdatedDate）
        # 如果有 etl_created_date 字段，优先使用；否则使用 UpdatedDate
        columns = [field.name for field in df.schema.fields]
        
        if 'etl_created_date' in columns:
            order_column = 'etl_created_date'
        elif 'UpdatedDate' in columns:
            order_column = 'UpdatedDate'
        else:
            # 如果没有时间字段，使用行号去重（保留第一条）
            order_column = None
        
        if order_column:
            # 使用窗口函数去重，保留最新记录
            window_spec = Window.partitionBy(primary_key).orderBy(desc(order_column))
            df_dedup = df.withColumn("rn", row_number().over(window_spec)) \
                        .filter(col("rn") == 1) \
                        .drop("rn")
        else:
            # 简单去重
            df_dedup = df.dropDuplicates([primary_key])
        
        final_count = df_dedup.count()
        removed_count = initial_count - final_count
        
        if removed_count > 0:
            logging.info(f"去重结果: {initial_count} -> {final_count}, 移除 {removed_count} 条重复记录")
            
            # 删除原分区
            spark.sql(f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION (dt='{partition_date}')")
            logging.info(f"删除原分区 dt={partition_date}")
            
            # 写入去重后的数据
            df_dedup.write \
                .mode("append") \
                .partitionBy("dt") \
                .format("parquet") \
                .saveAsTable(table_name)
            
            # 刷新元数据
            spark.sql(f"MSCK REPAIR TABLE {table_name}")
            spark.sql(f"REFRESH TABLE {table_name}")
            
            logging.info(f"✅ 清理完成，写入 {final_count} 条去重记录")
            return True
        else:
            logging.info("✅ 无重复数据，无需清理")
            return True
            
    except Exception as e:
        logging.error(f"清理重复数据失败: {e}")
        return False

def main():
    """主函数"""
    # 配置要清理的表
    tables_to_clean = [
        {
            'table_name': 'dwd_db.dwd_orders',
            'primary_key': 'OrderID',
            'description': 'DWD订单表'
        },
        {
            'table_name': 'dwd_db.dwd_orderdetails',
            'primary_key': 'OrderDetailID',
            'description': 'DWD订单明细表'
        }
    ]
    
    # 检查最近多少天的数据
    partition_days = int(os.getenv('CLEANUP_DAYS', '30'))
    
    # 是否只分析不清理
    analyze_only = os.getenv('ANALYZE_ONLY', 'false').lower() == 'true'
    
    logging.info("🚀 开始历史重复数据清理...")
    logging.info(f"检查最近 {partition_days} 天的数据")
    logging.info(f"分析模式: {'仅分析' if analyze_only else '分析并清理'}")
    
    spark = None
    try:
        spark = create_spark_session()
        
        for table_config in tables_to_clean:
            table_name = table_config['table_name']
            primary_key = table_config['primary_key']
            description = table_config['description']
            
            logging.info(f"\n{'='*50}")
            logging.info(f"处理表: {description} ({table_name})")
            logging.info(f"主键: {primary_key}")
            logging.info(f"{'='*50}")
            
            # 分析重复数据
            duplicate_stats = analyze_duplicates(spark, table_name, primary_key, partition_days)
            
            if not duplicate_stats:
                logging.warning(f"跳过表 {table_name}")
                continue
            
            # 统计总体情况
            total_duplicates = sum(stats['duplicate_count'] for stats in duplicate_stats.values())
            partitions_with_duplicates = [date for date, stats in duplicate_stats.items() 
                                        if stats['duplicate_count'] > 0]
            
            logging.info(f"\n📊 {description} 重复数据统计:")
            logging.info(f"  - 检查分区数: {len(duplicate_stats)}")
            logging.info(f"  - 有重复的分区数: {len(partitions_with_duplicates)}")
            logging.info(f"  - 总重复记录数: {total_duplicates}")
            
            if partitions_with_duplicates:
                logging.info(f"  - 有重复的分区: {partitions_with_duplicates}")
            
            # 如果不是仅分析模式，执行清理
            if not analyze_only and partitions_with_duplicates:
                logging.info(f"\n🧹 开始清理 {description} 的重复数据...")
                
                success_count = 0
                for partition_date in partitions_with_duplicates:
                    if cleanup_duplicates(spark, table_name, primary_key, partition_date):
                        success_count += 1
                    else:
                        logging.error(f"清理分区 dt={partition_date} 失败")
                
                logging.info(f"✅ {description} 清理完成: {success_count}/{len(partitions_with_duplicates)} 个分区清理成功")
            
        logging.info(f"\n🎉 历史重复数据清理完成！")
        
    except Exception as e:
        logging.error(f"清理过程出错: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()

