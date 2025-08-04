#!/usr/bin/env python3
"""
动态MySQL到Hive的ETL作业
支持多表增量同步，针对外部MySQL数据库优化
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import yaml
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_mysql_config():
    """从配置文件加载MySQL连接配置"""
    config_file = '/opt/airflow/config/production_etl_config_dynamic.yaml'
    
    # 默认配置（作为后备）
    default_config = {
        'host': os.getenv('EXTERNAL_MYSQL_HOST', '10.0.19.6'),
        'port': int(os.getenv('EXTERNAL_MYSQL_PORT', 3306)),
        'user': os.getenv('EXTERNAL_MYSQL_USER', 'root'),
        'password': os.getenv('EXTERNAL_MYSQL_PASSWORD', 'Sp1derman123@'),
        'database': os.getenv('EXTERNAL_MYSQL_DATABASE', 'wudeli')
    }
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # 替换环境变量
                for key, value in os.environ.items():
                    content = content.replace(f"${{{key}}}", value)
                    # 处理默认值语法 ${VAR:-default}
                    import re
                    pattern = f"\\${{{key}:-([^}}]+)}}"
                    content = re.sub(pattern, value if value else r'\1', content)
                
                config = yaml.safe_load(content)
                if config and 'mysql_conn' in config:
                    mysql_config = config['mysql_conn']
                    logger.info(f"从配置文件加载MySQL配置: {mysql_config['host']}:{mysql_config['port']}")
                    return mysql_config
        
        logger.info("使用默认MySQL配置")
        return default_config
        
    except Exception as e:
        logger.warning(f"加载配置文件失败，使用默认配置: {e}")
        return default_config

# 动态加载MySQL配置
MYSQL_CONFIG = load_mysql_config()

# 表配置 - 基于提供的schema
TABLE_SCHEMAS = {
    'customers': {
        'primary_key': 'CustomerID',
        'fields': [
            'CustomerID', 'CustomerName', 'ContactPerson', 'ContactPhone', 
            'Email', 'Address', 'CustomerType', 'CreditLimit', 
            'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    },
    'employees': {
        'primary_key': 'EmployeeID',
        'fields': [
            'EmployeeID', 'EmployeeName', 'Username', 'Department', 
            'Position', 'ContactPhone', 'Email', 'Status', 
            'LastLoginTime', 'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    },
    'products': {
        'primary_key': 'ProductID',
        'fields': [
            'ProductID', 'ProductName', 'Specification', 'Category', 
            'Description', 'UnitPrice', 'Barcode', 'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    },
    'orders': {
        'primary_key': 'OrderID',
        'fields': [
            'OrderID', 'CustomerID', 'OrderDate', 'RequiredDate', 'ShippedDate',
            'Status', 'PaymentMethod', 'PaymentStatus', 'TotalAmount', 'Discount',
            'ShippingAddress', 'ShippingMethod', 'Remarks', 'CreatedBy',
            'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    },
    'orderdetails': {  # 注意：MySQL表名可能是orderdetails而不是order_details
        'primary_key': 'OrderDetailID',
        'fields': [
            'OrderDetailID', 'OrderID', 'ProductID', 'Quantity', 'UnitPrice',
            'Discount', 'Amount', 'WarehouseID', 'Status', 'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    },
    'inventory': {
        'primary_key': 'InventoryID',
        'fields': [
            'InventoryID', 'ProductID', 'WarehouseID', 'Quantity', 'Unit',
            'ProductionDate', 'ExpiryDate', 'BatchNumber', 'Status',
            'LastCountDate', 'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    },
    'inventorytransactions': {  # 注意：MySQL表名可能是inventorytransactions
        'primary_key': 'TransactionID',
        'fields': [
            'TransactionID', 'ProductID', 'WarehouseID', 'TransactionType',
            'Quantity', 'UnitPrice', 'RelatedOrderID', 'TransactionDate',
            'BatchNumber', 'Operator', 'Remarks', 'CreatedDate'
        ],
        'incremental_field': 'CreatedDate'
    },
    'warehouses': {
        'primary_key': 'WarehouseID',
        'fields': [
            'WarehouseID', 'FactoryID', 'WarehouseName', 'Capacity',
            'Manager', 'ContactPhone', 'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    },
    'factories': {
        'primary_key': 'FactoryID',
        'fields': [
            'FactoryID', 'FactoryName', 'Location', 'ContactPerson',
            'ContactPhone', 'ProductionCapacity', 'CreatedDate', 'UpdatedDate'
        ],
        'incremental_field': 'UpdatedDate'
    }
}

def create_spark_session():
    """创建Spark会话"""
    spark = SparkSession.builder \
        .appName("MySQL_to_Hive_ETL_Dynamic") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # 设置日志级别
    spark.sparkContext.setLogLevel("INFO")
    return spark

def test_mysql_connection():
    """测试MySQL连接"""
    try:
        import mysql.connector
        from mysql.connector import Error
        
        # 使用配置文件中的超时设置
        connect_timeout = MYSQL_CONFIG.get('connect_timeout', 30)
        
        connection = mysql.connector.connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            connection_timeout=connect_timeout,
            charset=MYSQL_CONFIG.get('charset', 'utf8mb4')
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            logger.info(f"成功连接到MySQL，版本: {version[0]}")
            
            # 检查数据库中的表
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            existing_tables = [table[0].lower() for table in tables]
            logger.info(f"数据库中的表: {existing_tables}")
            
            cursor.close()
            connection.close()
            return existing_tables
        
    except Error as e:
        logger.error(f"MySQL连接测试失败: {e}")
        raise
    except Exception as e:
        logger.error(f"未知错误: {e}")
        raise

def extract_table_data(spark, table_name, table_config, existing_tables):
    """从MySQL提取表数据"""
    
    # 检查表是否存在（不区分大小写）
    table_name_lower = table_name.lower()
    if table_name_lower not in existing_tables:
        logger.warning(f"表 {table_name} 在数据库中不存在，跳过")
        return None
    
    logger.info(f"开始提取表: {table_name}")
    
    jdbc_url = f"jdbc:mysql://{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
    
    # 构建查询 - 处理字段名大小写问题
    fields_str = ', '.join(table_config['fields'])
    query = f"(SELECT {fields_str} FROM {table_name}) as {table_name}"
    
    try:
        # 使用配置文件中的超时设置
        connect_timeout = MYSQL_CONFIG.get('connect_timeout', 60) * 1000  # 转换为毫秒
        socket_timeout = MYSQL_CONFIG.get('socket_timeout', 300) * 1000    # 转换为毫秒
        
        # 分批读取大表数据
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", MYSQL_CONFIG['user']) \
            .option("password", MYSQL_CONFIG['password']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("fetchsize", "5000") \
            .option("connectTimeout", str(connect_timeout)) \
            .option("socketTimeout", str(socket_timeout)) \
            .option("numPartitions", "4") \
            .load()
        
        record_count = df.count()
        logger.info(f"表 {table_name} 提取完成，记录数: {record_count}")
        
        if record_count > 0:
            # 显示前几行数据用于验证
            logger.info(f"表 {table_name} 样本数据:")
            df.show(5, truncate=False)
        
        return df
        
    except Exception as e:
        logger.error(f"提取表 {table_name} 失败: {e}")
        # 尝试简化查询
        try:
            logger.info(f"尝试使用简化查询提取表 {table_name}")
            simple_query = f"(SELECT * FROM {table_name} LIMIT 1000) as {table_name}"
            df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", simple_query) \
                .option("user", MYSQL_CONFIG['user']) \
                .option("password", MYSQL_CONFIG['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            logger.info(f"使用简化查询成功提取表 {table_name}，记录数: {df.count()}")
            return df
            
        except Exception as simple_error:
            logger.error(f"简化查询也失败: {simple_error}")
            return None

def create_hive_database(spark, hive_db):
    """创建Hive数据库"""
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
        spark.sql(f"USE {hive_db}")
        logger.info(f"Hive数据库 {hive_db} 已准备就绪")
    except Exception as e:
        logger.error(f"创建Hive数据库失败: {e}")
        raise

def load_to_hive(spark, df, table_name, hive_db):
    """加载数据到Hive"""
    if df is None or df.count() == 0:
        logger.warning(f"表 {table_name} 没有数据，跳过加载")
        return
    
    try:
        hive_table = f"{hive_db}.{table_name}"
        output_path = f"/user/bigdata/wudeli/processed/{table_name}"
        
        logger.info(f"写入Hive表: {hive_table}")
        logger.info(f"输出路径: {output_path}")
        
        # 写入Hive表
        df.write \
            .mode("overwrite") \
            .option("path", output_path) \
            .saveAsTable(hive_table)
        
        # 验证写入结果
        result_df = spark.sql(f"SELECT COUNT(*) as count FROM {hive_table}")
        count = result_df.collect()[0]['count']
        logger.info(f"表 {table_name} 成功写入Hive，记录数: {count}")
        
    except Exception as e:
        logger.error(f"写入Hive表 {table_name} 失败: {e}")
        # 尝试直接写入HDFS
        try:
            logger.info(f"尝试直接写入HDFS: {output_path}")
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            logger.info(f"表 {table_name} 成功写入HDFS")
        except Exception as hdfs_error:
            logger.error(f"写入HDFS也失败: {hdfs_error}")
            raise

def generate_summary_report(spark, hive_db, processed_tables):
    """生成处理摘要报告"""
    try:
        logger.info("生成处理摘要报告...")
        
        summary_data = []
        for table_name in processed_tables:
            try:
                hive_table = f"{hive_db}.{table_name}"
                result_df = spark.sql(f"SELECT COUNT(*) as count FROM {hive_table}")
                count = result_df.collect()[0]['count']
                summary_data.append((table_name, count, "SUCCESS"))
            except Exception as e:
                summary_data.append((table_name, 0, f"FAILED: {str(e)[:50]}"))
        
        # 创建摘要DataFrame
        summary_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("record_count", LongType(), True),
            StructField("status", StringType(), True)
        ])
        
        summary_df = spark.createDataFrame(summary_data, summary_schema)
        summary_df = summary_df.withColumn("processed_date", current_timestamp())
        
        # 保存摘要报告
        summary_path = "/user/bigdata/wudeli/reports/etl_summary"
        summary_df.write \
            .mode("overwrite") \
            .parquet(summary_path)
        
        logger.info("摘要报告已保存到: " + summary_path)
        
        # 显示摘要
        logger.info("ETL处理摘要:")
        summary_df.show(truncate=False)
        
    except Exception as e:
        logger.error(f"生成摘要报告失败: {e}")

def main():
    """主函数"""
    logger.info("开始MySQL到Hive的动态ETL作业")
    
    spark = None
    try:
        # 测试MySQL连接
        existing_tables = test_mysql_connection()
        
        # 创建Spark会话
        spark = create_spark_session()
        
        # 创建Hive数据库
        hive_db = "wudeli_analytics"
        create_hive_database(spark, hive_db)
        
        processed_tables = []
        
        # 处理每个表
        for table_name, table_config in TABLE_SCHEMAS.items():
            try:
                logger.info(f"处理表: {table_name}")
                
                # 提取数据
                df = extract_table_data(spark, table_name, table_config, existing_tables)
                
                if df is not None:
                    # 加载到Hive
                    load_to_hive(spark, df, table_name, hive_db)
                    processed_tables.append(table_name)
                else:
                    logger.warning(f"跳过表 {table_name}")
                
            except Exception as table_error:
                logger.error(f"处理表 {table_name} 时出错: {table_error}")
                # 继续处理其他表
                continue
        
        # 生成摘要报告
        if processed_tables:
            generate_summary_report(spark, hive_db, processed_tables)
        
        logger.info(f"ETL作业完成，成功处理 {len(processed_tables)} 个表")
        
    except Exception as e:
        logger.error(f"ETL作业失败: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()