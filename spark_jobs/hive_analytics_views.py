#!/usr/bin/env python3
"""
创建Hive分析视图
"""

import os
import sys
from pyspark.sql import SparkSession
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """创建Spark会话"""
    return SparkSession.builder \
        .appName("Hive_Analytics_Views") \
        .config("spark.sql.adaptive.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    """主函数"""
    logger.info("开始创建Hive分析视图")
    
    try:
        # 创建Spark会话
        spark = create_spark_session()
        
        # 创建数据库（如果不存在）
        logger.info("创建数据库 wudeli_analytics")
        spark.sql("CREATE DATABASE IF NOT EXISTS wudeli_analytics")
        
        # 创建分析视图
        logger.info("创建订单汇总视图")
        spark.sql("""
            CREATE OR REPLACE VIEW wudeli_analytics.orders_summary AS
            SELECT 
                date,
                COUNT(*) as order_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount
            FROM wudeli_analytics.orders
            GROUP BY date
        """)
        
        # 显示视图数据
        logger.info("订单汇总视图数据:")
        spark.sql("SELECT * FROM wudeli_analytics.orders_summary").show()
        
        logger.info("分析视图创建完成")
        
    except Exception as e:
        logger.error(f"创建分析视图失败: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()