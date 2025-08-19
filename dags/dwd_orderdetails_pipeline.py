from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import os
import yaml

# =============================================================================
# DEFAULT ARGS
# =============================================================================
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def load_config(file_path, default_config={}):
    """Generic function to load a YAML config file."""
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    else:
        logging.warning(f"配置文件 {file_path} 不存在，使用默认配置。")
        return default_config

def get_table_partition_strategy(spark, table_name, target_date, config):
    """根据配置获取表的最佳分区日期"""
    try:
        table_config = config.get('tables', {}).get(table_name, {})
        partition_strategy = table_config.get('partition_strategy', 'daily')
        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        available_dates = [p[0].split('=')[1] for p in partitions]
        if not available_dates:
            return target_date
        available_dates.sort()
        latest_date = max(available_dates)
        join_strategy = config.get('join_strategies', {}).get(partition_strategy, {})
        if join_strategy.get('use_latest_always', False):
            return latest_date
        if join_strategy.get('prefer_target_date', True):
            if target_date in available_dates:
                return target_date
            if join_strategy.get('fallback_to_latest', True):
                logging.warning(f"{table_name} 目标分区 dt={target_date} 不存在，回退到最新分区: dt={latest_date}")
                return latest_date
        return target_date
    except Exception as e:
        logging.error(f"检查 {table_name} 分区失败: {e}")
        return target_date

# =============================================================================
# MAIN SPARK ETL TASK
# =============================================================================
def run_dwd_orderdetails_etl(**context):
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.functions import (
        col, when, year, month, dayofmonth, dayofweek, quarter, coalesce, lit, 
        min, max, current_timestamp, length, avg, sum as spark_sum, count as spark_count,
        create_map
    )

    spark = None
    try:
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("DWD_OrderDetails_ETL_Pipeline") \
            .master(os.getenv('SPARK_MASTER_URL', 'local[*]')) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", os.getenv('SPARK_DRIVER_MEMORY', '4g')) \
            .config("spark.executor.memory", os.getenv('SPARK_EXECUTOR_MEMORY', '4g')) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .enableHiveSupport() \
            .getOrCreate()
        logging.info("✅ Spark session created successfully.")

        batch_date = context['ds']
        logging.info(f"Starting ETL for batch date: {batch_date}")

        partition_config = load_config('/opt/airflow/config/table_partition_strategy.yaml')
        table_partition_info = {t: get_table_partition_strategy(spark, t, batch_date, partition_config) for t in ['ods.Orders', 'ods.Customers', 'ods.Products', 'ods.Warehouses', 'ods.Factories']}
        
        query = f"""SELECT od.*, o.CustomerID, c.CustomerName, c.CustomerType, p.ProductName, p.Category as ProductCategory, p.Specification as ProductSpecification, w.WarehouseName, w.Manager as WarehouseManager, f.FactoryName, f.Location as FactoryLocation, o.OrderDate, o.Status as OrderStatus, o.PaymentMethod, o.PaymentStatus FROM ods.OrderDetails od LEFT JOIN ods.Orders o ON od.OrderID=o.OrderID AND o.dt='{table_partition_info["ods.Orders"]}' LEFT JOIN ods.Customers c ON o.CustomerID=c.CustomerID AND c.dt='{table_partition_info["ods.Customers"]}' LEFT JOIN ods.Products p ON od.ProductID=p.ProductID AND p.dt='{table_partition_info["ods.Products"]}' LEFT JOIN ods.Warehouses w ON od.WarehouseID=w.WarehouseID AND w.dt='{table_partition_info["ods.Warehouses"]}' LEFT JOIN ods.Factories f ON w.FactoryID=f.FactoryID AND f.dt='{table_partition_info["ods.Factories"]}' WHERE od.dt='{batch_date}'"""
        df = spark.sql(query)
        df.cache()
        
        if df.rdd.isEmpty():
            logging.warning("No records found. Skipping.")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_EMPTY_DATA')
            return

        record_count = df.count()
        logging.info(f"✅ Extracted {record_count} records.")

        logging.info("Starting data transformation...")
        maps = load_config('/opt/airflow/dags/config/orderdetail_status_mapping.yaml', {'orderdetail_status_mapping': {}, 'product_category_mapping': {}})
        status_map = create_map([lit(x) for c in maps['orderdetail_status_mapping'].items() for x in c])
        cat_map = create_map([lit(x) for c in maps['product_category_mapping'].items() for x in c])

        df = df.fillna({'ProductName':'Unknown Product','ProductCategory':'Unknown Category','WarehouseName':'Unknown Warehouse'}).withColumn("OrderDetailStatus", coalesce(status_map[col("OrderDetailStatus")], col("OrderDetailStatus"))).withColumn("ProductCategory", coalesce(cat_map[col("ProductCategory")], col("ProductCategory")))
        df = df.withColumn("LineTotal", col("Quantity")*col("UnitPrice")).withColumn("DiscountAmount", col("LineTotal")*col("Discount")/100).withColumn("NetAmount", col("LineTotal")-col("DiscountAmount"))
        df = df.withColumn("PriceCategory", when(col("UnitPrice")>=1000,"Premium").when(col("UnitPrice")>=500,"High").otherwise("Medium")).withColumn("IsHighValue", when(col("NetAmount")>=10000,True).otherwise(False))
        win = Window.partitionBy("WarehouseName")
        df = df.withColumn("WarehouseEfficiency", (spark_sum(when(col("OrderDetailStatus")=='Delivered',1).otherwise(0)).over(win)/spark_count(lit(1)).over(win))*100)
        df = df.withColumn("DataQualityScore", lit(100)-when(col('ProductName')=='Unknown Product',15).otherwise(0)-when(col('ProductCategory')=='Unknown Category',10).otherwise(0))
        df = df.withColumn("DataQualityLevel", when(col("DataQualityScore")<=70,"Poor").when(col("DataQualityScore")<=85,"Fair").otherwise("Good"))
        df = df.withColumn('etl_created_date',current_timestamp()).withColumn('etl_batch_id',lit(context['ds_nodash']))
        logging.info("✅ Transformation complete.")

        logging.info("Calculating statistics...")
        stats = df.agg(avg("UnitPrice").alias("avg_price"), spark_sum(when(col("IsHighValue"),1).otherwise(0)).alias("high_value_items")).collect()[0]
        qual_dist = {r['DataQualityLevel']:r['count'] for r in df.groupBy('DataQualityLevel').count().collect()}
        transform_stats = {'total_records':record_count, 'avg_unit_price':stats['avg_price'], 'high_value_items':stats['high_value_items'], 'quality_distribution':qual_dist}
        context['task_instance'].xcom_push(key='transform_stats', value=transform_stats)

        logging.info("Loading data to DWD layer...")
        table_name = "dwd_db.dwd_orderdetails"
        location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orderdetails"
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd_db")
        df.withColumn('dt', lit(batch_date)).write.mode("overwrite").partitionBy("dt").format("parquet").option("path",location).saveAsTable(table_name)
        df.unpersist()
        logging.info("✅ Data loaded successfully.")

        summary = {'total_records':record_count, 'partitions':[{'dt':batch_date, 'path':location}]}
        context['task_instance'].xcom_push(key='hdfs_load_summary', value=summary)
        context['task_instance'].xcom_push(key='table_name', value=table_name)
        context['task_instance'].xcom_push(key='status', value='SUCCESS')

    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

def create_orderdetails_hive_views(**context):
    if context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='status') == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping view creation as no data was processed.")
        return

    from pyspark.sql import SparkSession
    spark = None
    try:
        spark = SparkSession.builder.appName("CreateDWDViews").config("spark.sql.catalogImplementation","hive").config("spark.hadoop.hive.metastore.uris","thrift://hive-metastore:9083").enableHiveSupport().getOrCreate()
        spark.sql("USE dwd_db")
        views = ["CREATE OR REPLACE VIEW dwd_orderdetails_high_value AS SELECT * FROM dwd_orderdetails WHERE IsHighValue = true", "CREATE OR REPLACE VIEW dwd_orderdetails_product_summary AS SELECT ProductID, ProductName, ProductCategory, COUNT(*) as order_count, SUM(Quantity) as total_quantity, SUM(NetAmount) as total_amount FROM dwd_orderdetails GROUP BY ProductID, ProductName, ProductCategory"]
        for v in views: spark.sql(v)
        logging.info(f"✅ Successfully created {len(views)} views.")
    except Exception as e:
        logging.error(f"View creation failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

def validate_orderdetails_dwd(**context):
    if context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='status') == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping validation as no data was processed.")
        return

    stats = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='transform_stats')
    summary = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='hdfs_load_summary')
    
    issues = []
    if stats['total_records'] != summary['total_records']:
        issues.append("Record count mismatch between transform and load stages.")
    if stats['quality_distribution'].get('Poor', 0) / stats['total_records'] > 0.1:
        issues.append("High ratio of poor quality data.")

    if issues:
        logging.warning(f"Validation issues found: {issues}")
    else:
        logging.info("✅ Data validation passed.")

with DAG(
    'dwd_orderdetails_pipeline',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['dwd', 'orderdetails', 'refactored'],
    description='Refactored DWD OrderDetails ETL pipeline using a single Spark job.',
) as dag:

    start = DummyOperator(task_id='start')

    etl_task = PythonOperator(
        task_id='run_dwd_orderdetails_etl_task',
        python_callable=run_dwd_orderdetails_etl
    )

    create_views = PythonOperator(
        task_id='create_hive_views',
        python_callable=create_orderdetails_hive_views
    )

    validate_dwd = PythonOperator(
        task_id='validate_dwd_data',
        python_callable=validate_orderdetails_dwd
    )

    end = DummyOperator(task_id='end')

    start >> etl_task >> create_views >> validate_dwd >> end