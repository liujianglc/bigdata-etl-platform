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
    """根据配置获取表的最佳分区日期，能处理非分区表。"""
    try:
        table_config = config.get('tables', {}).get(table_name, {})
        partition_strategy = table_config.get('partition_strategy')

        # 如果策略是 aatest_only，我们知道它没有分区，直接返回None
        if partition_strategy == 'latest_only':
            logging.info(f"表 {table_name} 策略为 'latest_only'，不使用分区进行JOIN。")
            return None

        # 对于其他策略，我们假设它是分区的并继续
        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        available_dates = [p[0].split('=')[1] for p in partitions]
        
        if not available_dates:
            logging.warning(f"{table_name} 是分区表但未找到任何分区，使用目标日期: {target_date}")
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
        # 异常处理作为备用，以防配置和实际情况不符
        if "PARTITION_SCHEMA_IS_EMPTY" in str(e) or "is not partitioned" in str(e):
             logging.warning(f"表 {table_name} 配置为分区表，但实际上未找到分区。将不使用分区进行JOIN。")
             return None
        else:
            logging.error(f"检查 {table_name} 分区失败: {e}")
            return target_date # 回退

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
            .config("spark.sql.parquet.cacheMetadata", "false") \
            .config("spark.sql.hive.metastorePartitionPruning", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .enableHiveSupport() \
            .getOrCreate()
        logging.info("✅ Spark session created successfully.")

        batch_date = context['ds']
        logging.info(f"Starting ETL for batch date: {batch_date}")

        partition_config = load_config('/opt/airflow/config/table_partition_strategy.yaml')
        dim_tables_to_join = ['ods.Orders', 'ods.Customers', 'ods.Products', 'ods.Warehouses', 'ods.Factories']
        table_partition_info = {t: get_table_partition_strategy(spark, t, batch_date, partition_config) for t in dim_tables_to_join}
        
        # 动态构建JOIN子句
        join_clauses = []
        dim_tables_meta = {
            'ods.Orders': ('o', 'od.OrderID = o.OrderID'),
            'ods.Customers': ('c', 'o.CustomerID = c.CustomerID'),
            'ods.Products': ('p', 'od.ProductID = p.ProductID'),
            'ods.Warehouses': ('w', 'od.WarehouseID = w.WarehouseID'),
            'ods.Factories': ('f', 'w.FactoryID = f.FactoryID')
        }

        for table, (alias, join_cond) in dim_tables_meta.items():
            partition_date = table_partition_info.get(table)
            partition_filter = f" AND {alias}.dt = '{partition_date}'" if partition_date else ""
            join_clauses.append(f"LEFT JOIN {table} {alias} ON {join_cond}{partition_filter}")

        join_sql = "\n".join(join_clauses)

        # 构建最终查询
        query = f"""SELECT od.*, o.CustomerID, c.CustomerName, c.CustomerType, p.ProductName, p.Category as ProductCategory, p.Specification as ProductSpecification, w.WarehouseName, w.Manager as WarehouseManager, f.FactoryName, f.Location as FactoryLocation, o.OrderDate, o.Status as OrderStatus, o.PaymentMethod, o.PaymentStatus FROM ods.OrderDetails od {join_sql} WHERE od.dt='{batch_date}'"""
        
        logging.info(f"Executing dynamically generated query:\n{query}")
        df = spark.sql(query)
        logging.info('Executed query successfully.')
        
        # Use limit(1).count() instead of rdd.isEmpty() for better performance
        record_count = df.limit(1).count()
        if record_count == 0:
            logging.warning("No records found for this batch.")
            table_name = "dwd_db.dwd_orderdetails"
            location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orderdetails"
            
            # Use the utility function to handle empty partition
            from dags.utils.empty_partition_handler import handle_empty_partition
            status = handle_empty_partition(spark, df, table_name, batch_date, context, location)
            
            context['task_instance'].xcom_push(key='status', value=status)
            context['task_instance'].xcom_push(key='table_name', value=table_name)
            return

        # Cache after we know there's data
        df.cache()
        record_count = df.count()
        logging.info(f"✅ Extracted {record_count} records.")

        # ... (The rest of the transformation and loading logic remains the same) ...
        logging.info("Starting data transformation...")
        maps = load_config('/opt/airflow/dags/config/orderdetail_status_mapping.yaml', {'orderdetail_status_mapping': {}, 'product_category_mapping': {}})
        status_map = create_map([lit(x) for c in maps['orderdetail_status_mapping'].items() for x in c])
        cat_map = create_map([lit(x) for c in maps['product_category_mapping'].items() for x in c])

        df = df.fillna({'ProductName':'Unknown Product','ProductCategory':'Unknown Category','WarehouseName':'Unknown Warehouse'}).withColumn("OrderDetailStatus", coalesce(status_map[col("Status")], col("Status"))).withColumn("ProductCategory", coalesce(cat_map[col("ProductCategory")], col("ProductCategory")))
        df = df.withColumn("LineTotal", col("Quantity")*col("UnitPrice")).withColumn("DiscountAmount", col("LineTotal")*col("Discount")/100).withColumn("NetAmount", col("LineTotal")-col("DiscountAmount"))
        df = df.withColumn("PriceCategory", when(col("UnitPrice")>=1000,"Premium").when(col("UnitPrice")>=500,"High").otherwise("Medium")).withColumn("IsHighValue", when(col("NetAmount")>=10000,True).otherwise(False)).withColumn("IsDiscounted", when(col("Discount") > 0, True).otherwise(False))
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
        
        # Use replaceWhere approach - much more efficient and avoids cache issues
        df.withColumn('dt', lit(batch_date)) \
          .write \
          .mode("overwrite") \
          .partitionBy("dt") \
          .format("parquet") \
          .option("replaceWhere", f"dt = '{batch_date}'") \
          .option("path", location) \
          .saveAsTable(table_name)

        # Refresh metadata after writing new partition
        spark.sql("MSCK REPAIR TABLE dwd_db.dwd_orderdetails")
        spark.catalog.refreshTable("dwd_db.dwd_orderdetails")

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
    status = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='status')
    if status in ['SKIPPED_EMPTY_DATA']:
        logging.warning("Skipping view creation as no data was processed.")
        return

    from pyspark.sql import SparkSession
    spark = None
    try:
        spark = SparkSession.builder.appName("CreateDWDViews").config("spark.sql.catalogImplementation","hive").config("spark.hadoop.hive.metastore.uris","thrift://hive-metastore:9083").enableHiveSupport().getOrCreate()
        spark.sql("USE dwd_db")
        views = [
            # 1. 高价值订单明细视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_high_value AS
            SELECT *
            FROM dwd_orderdetails
            WHERE IsHighValue = true
              AND DataQualityLevel IN ('Good', 'Excellent')
            """,
            
            # 2. 折扣商品视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_discounted AS
            SELECT *
            FROM dwd_orderdetails
            WHERE IsDiscounted = true
              AND Discount > 0
            """,
            
            # 3. 产品销售统计视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_product_summary AS
            SELECT 
                ProductID,
                ProductName,
                ProductCategory,
                COUNT(*) as order_count,
                SUM(Quantity) as total_quantity,
                SUM(NetAmount) as total_amount,
                AVG(UnitPrice) as avg_unit_price,
                AVG(Discount) as avg_discount
            FROM dwd_orderdetails
            GROUP BY ProductID, ProductName, ProductCategory
            """,
            
            # 4. 仓库效率视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_warehouse_performance AS
            SELECT 
                WarehouseID,
                WarehouseName,
                FactoryName,
                COUNT(*) as total_items,
                SUM(CASE WHEN OrderDetailStatus = 'Delivered' THEN 1 ELSE 0 END) as delivered_items,
                AVG(WarehouseEfficiency) as efficiency_score
            FROM dwd_orderdetails
            GROUP BY WarehouseID, WarehouseName, FactoryName
            """
        ]
        for v in views: spark.sql(v)
        logging.info(f"✅ Successfully created {len(views)} views.")
    except Exception as e:
        logging.error(f"View creation failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

def validate_orderdetails_dwd(**context):
    status = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='status')
    if status == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping validation as no data was processed.")
        return
    elif status == 'SUCCESS_EMPTY_PARTITION':
        logging.info("✅ Empty partition validation passed.")
        return

    stats = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='transform_stats')
    summary = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='hdfs_load_summary')
    
    issues = []
    if stats['total_records'] != summary['total_records']:
        issues.append("Record count mismatch between transform and load stages.")
    if stats['total_records'] > 0 and stats['quality_distribution'].get('Poor', 0) / stats['total_records'] > 0.1:
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
