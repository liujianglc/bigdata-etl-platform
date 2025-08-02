#!/usr/bin/env python3
"""
增量MySQL到Hive的ETL作业
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """创建Spark会话"""
    return SparkSession.builder \
        .appName("MySQL_to_Hive_ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    """主函数"""
    logger.info("开始MySQL到Hive的ETL作业")
    
    try:
        # 创建Spark会话
        spark = create_spark_session()
        
        # 模拟数据处理（实际中应该从MySQL读取数据）
        logger.info("模拟数据处理...")
        
        # 创建示例数据
        data = [
            (1, "订单1", "2025-08-02", 100.0),
            (2, "订单2", "2025-08-02", 200.0),
            (3, "订单3", "2025-08-02", 150.0)
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("date", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # 显示数据
        logger.info("处理的数据:")
        df.show()
        
        # 写入到HDFS（模拟写入Hive表）
        output_path = "/user/bigdata/wudeli/processed/orders"
        logger.info(f"写入数据到: {output_path}")
        
        df.write \
          .mode("overwrite") \
          .option("path", output_path) \
          .saveAsTable("wudeli_analytics.orders")
        
        logger.info("ETL作业完成")
        
    except Exception as e:
        logger.error(f"ETL作业失败: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()