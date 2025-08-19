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
        logging.warning(f"Config file {file_path} not found, using default.")
        return default_config

def get_table_partition_strategy(spark, table_name, target_date, config):
    """Gets the optimal partition date for a table based on configuration."""
    try:
        table_config = config.get('tables', {}).get(table_name, {})
        partition_strategy = table_config.get('partition_strategy')

        if partition_strategy == 'latest_only':
            logging.info(f"Table {table_name} strategy is 'latest_only', not using partition for JOIN.")
            return None

        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        available_dates = [p[0].split('=')[1] for p in partitions]
        
        if not available_dates:
            logging.warning(f"{table_name} is a partitioned table but no partitions were found, using target date: {target_date}")
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
                logging.warning(f"{table_name} target partition dt={target_date} not found, falling back to latest: dt={latest_date}")
                return latest_date
        
        return target_date
            
    except Exception as e:
        if "PARTITION_SCHEMA_IS_EMPTY" in str(e) or "is not partitioned" in str(e):
             logging.warning(f"Table {table_name} is configured as partitioned, but no partitions found. Not using partition for JOIN.")
             return None
        else:
            logging.error(f"Failed to check partition for {table_name}: {e}")
            return target_date # Fallback

# =============================================================================
# MAIN SPARK ETL TASK
# =============================================================================
def run_dwd_orders_etl(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, when, datediff, year, month, dayofmonth, dayofweek, quarter, coalesce, lit, 
        min, max, current_timestamp, create_map, avg, sum as spark_sum, count as spark_count
    )

    spark = None
    try:
        logging.info("Initializing Spark session for DWD Orders ETL...")
        spark = SparkSession.builder \
            .appName("DWD_Orders_ETL_Pipeline") \
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
        logging.info(f"Starting DWD Orders ETL for batch date: {batch_date}")

        partition_config = load_config('/opt/airflow/config/table_partition_strategy.yaml')
        
        customers_partition = get_table_partition_strategy(spark, 'ods.Customers', batch_date, partition_config)
        employees_partition = get_table_partition_strategy(spark, 'ods.Employees', batch_date, partition_config)

        join_clauses = []
        dim_tables_meta = {
            'ods.Customers': ('c', 'o.CustomerID = c.CustomerID', customers_partition),
            'ods.Employees': ('e', 'o.CreatedBy = e.EmployeeID', employees_partition)
        }

        for table, (alias, join_cond, partition_date) in dim_tables_meta.items():
            partition_filter = f" AND {alias}.dt = '{partition_date}'" if partition_date else ""
            join_clauses.append(f"LEFT JOIN {table} {alias} ON {join_cond}{partition_filter}")
        
        join_sql = "\n".join(join_clauses)

        query = f"""
        SELECT 
            o.*,
            c.CustomerName,
            c.CustomerType,
            e.EmployeeName as CreatedByName,
            e.Department as CreatedByDepartment
        FROM ods.Orders o
        {join_sql}
        WHERE o.dt = '{batch_date}'
        """
        
        logging.info(f"Executing dynamically generated query:\n{query}")
        df = spark.sql(query)
        df.cache()
        
        record_count = df.count()
        if record_count == 0:
            logging.warning("No records found for this batch. Skipping.")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_EMPTY_DATA')
            return
        
        logging.info(f"✅ Extracted {record_count} records.")
        
        logging.info("Starting data transformation...")
        
        df = df.fillna({
            'CustomerName': 'Unknown Customer', 'CustomerType': 'Regular',
            'ShippingAddress': 'Address Not Provided', 'Remarks': '', 'Discount': 0,
            'CreatedByName': 'Unknown Employee', 'CreatedByDepartment': 'Unknown Department'
        })

        map_config = load_config('/opt/airflow/dags/config/order_status_mapping.yaml', {})
        order_status_map = create_map([lit(x) for c in map_config.get('order_status_mapping', {}).items() for x in c])
        payment_status_map = create_map([lit(x) for c in map_config.get('payment_status_mapping', {}).items() for x in c])

        df = df.withColumn("OrderStatus", coalesce(order_status_map[col("OrderStatus")], col("OrderStatus")))
        df = df.withColumn("PaymentStatus", coalesce(payment_status_map[col("PaymentStatus")], col("PaymentStatus")))

        df = df.withColumn("LeadTimeDays", datediff(col("RequiredDate"), col("OrderDate"))) \
              .withColumn("ProcessingDays", datediff(col("ShippedDate"), col("CreatedDate"))) \
              .withColumn("IsDelayed", col("ShippedDate") > col("RequiredDate"))) \
              .withColumn("DeliveryStatus",
                         when(col("OrderStatus").isin(["Cancelled"]), "Cancelled")
                         .when(col("OrderStatus").isin(["Delivered"]), "Completed")
                         .when((col("ShippedDate").isNotNull()) & (col("ShippedDate") <= col("RequiredDate")), "OnTime")
                         .when((col("ShippedDate").isNotNull()) & (col("ShippedDate") > col("RequiredDate")), "Late")
                         .when((col("RequiredDate") < lit(batch_date)) & col("ShippedDate").isNull(), "Overdue")
                         .otherwise("Pending"))) \
              .withColumn("OrderYear", year(col("OrderDate"))) \
              .withColumn("OrderMonth", month(col("OrderDate"))) \
              .withColumn("OrderDay", dayofmonth(col("OrderDate"))) \
              .withColumn("OrderDayOfWeek", dayofweek(col("OrderDate"))) \
              .withColumn("OrderQuarter", quarter(col("OrderDate"))) \
              .withColumn("NetAmount", col("TotalAmount") - col("Discount"))) \
              .withColumn("OrderSizeCategory",
                         when(col("TotalAmount") >= 10000, "Large")
                         .when(col("TotalAmount") >= 5000, "Medium")
                         .when(col("TotalAmount") >= 1000, "Small")
                         .otherwise("Micro"))) \
              .withColumn("OrderPriority", 
                         when(col("CustomerType") == 'VIP', 'High')
                         .when(col("TotalAmount") >= 10000, "High")
                         .when(col("TotalAmount") >= 5000, "Medium")
                         .otherwise('Low'))

        df = df.withColumn("DataQualityScore", 
            lit(100) -
            when(col('CustomerName') == 'Unknown Customer', 15).otherwise(0) -
            when(col('ShippingAddress') == 'Address Not Provided', 10).otherwise(0) -
            when(col('RequiredDate').isNull(), 5).otherwise(0) -
            when(col('TotalAmount') <= 0, 25).otherwise(0)
        )
        df = df.withColumn("DataQualityLevel", 
            when(col("DataQualityScore") <= 70, "Poor")
            .when(col("DataQualityScore") <= 85, "Fair")
            .otherwise("Good")
        )

        df = df.withColumn('etl_created_date', current_timestamp()) \
               .withColumn('etl_batch_id', lit(context['ds_nodash']))

        logging.info("✅ Transformation complete.")

        logging.info("Calculating statistics for validation...")
        delayed_orders = df.filter(col('IsDelayed') == True).count()
        avg_processing_days = df.agg(avg("ProcessingDays")).collect()[0][0]
        qual_dist = {r['DataQualityLevel']:r['count'] for r in df.groupBy('DataQualityLevel').count().collect()}
        status_dist = {r['OrderStatus']:r['count'] for r in df.groupBy('OrderStatus').count().collect()}
        priority_dist = {r['OrderPriority']:r['count'] for r in df.groupBy('OrderPriority').count().collect()}

        transform_stats = {
            'total_records': record_count, 'quality_distribution': qual_dist,
            'status_distribution': status_dist, 'priority_distribution': priority_dist,
            'delayed_orders': delayed_orders, 'avg_processing_days': avg_processing_days
        }
        context['task_instance'].xcom_push(key='transform_stats', value=transform_stats)

        logging.info("Loading data to DWD layer...")
        table_name = "dwd_db.dwd_orders"
        location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders"
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd_db")
        
        df.withColumn('dt', lit(batch_date)) \
          .write.mode("overwrite").partitionBy("dt").format("parquet").option("path", location).saveAsTable(table_name)
        
        df.unpersist()
        logging.info("✅ Data loaded successfully.")

        summary = {'total_records': record_count, 'partitions': [{'dt': batch_date, 'path': location}]}
        context['task_instance'].xcom_push(key='hdfs_load_summary', value=summary)
        context['task_instance'].xcom_push(key='table_name', value=table_name)
        context['task_instance'].xcom_push(key='status', value='SUCCESS')

    except Exception as e:
        logging.error(f"DWD Orders ETL failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

def create_orders_hive_views(**context):
    """Creates Hive views for the DWD Orders table."""
    if context['task_instance'].xcom_pull(task_ids='run_dwd_orders_etl_task', key='status') == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping view creation as no data was processed.")
        return

    from pyspark.sql import SparkSession
    spark = None
    try:
        spark = SparkSession.builder.appName("CreateDWDOrderViews").config("spark.sql.catalogImplementation","hive").config("spark.hadoop.hive.metastore.uris","thrift://hive-metastore:9083").enableHiveSupport().getOrCreate()
        spark.sql("USE dwd_db")
        
        views_sql = [
            "CREATE OR REPLACE VIEW dwd_orders_active AS SELECT * FROM dwd_orders WHERE OrderStatus IN ('Pending', 'Confirmed', 'Shipping', 'Shipped')",
            "CREATE OR REPLACE VIEW dwd_orders_delayed AS SELECT * FROM dwd_orders WHERE IsDelayed = true",
            "CREATE OR REPLACE VIEW dwd_orders_high_value AS SELECT * FROM dwd_orders WHERE OrderPriority = 'High' AND TotalAmount >= 10000",
            """
            CREATE OR REPLACE VIEW dwd_orders_status_summary AS
            SELECT OrderStatus, COUNT(*) as order_count, SUM(TotalAmount) as total_amount,
                   AVG(TotalAmount) as avg_amount, COUNT(CASE WHEN IsDelayed = true THEN 1 END) as delayed_count
            FROM dwd_orders GROUP BY OrderStatus
            """
        ]
        
        for view_sql in views_sql:
            spark.sql(view_sql)
        
        logging.info(f"✅ Successfully created {len(views_sql)} views.")
    except Exception as e:
        logging.error(f"View creation failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

def validate_orders_dwd(**context):
    """Validates the DWD Orders data quality."""
    if context['task_instance'].xcom_pull(task_ids='run_dwd_orders_etl_task', key='status') == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping validation as no data was processed.")
        return

    stats = context['task_instance'].xcom_pull(task_ids='run_dwd_orders_etl_task', key='transform_stats')
    
    issues = []
    if stats['total_records'] > 0:
        poor_quality_ratio = stats['quality_distribution'].get('Poor', 0) / stats['total_records']
        if poor_quality_ratio > 0.1:
            issues.append(f"High ratio of poor quality data: {poor_quality_ratio:.2%}")
        
        delayed_ratio = stats['delayed_orders'] / stats['total_records']
        if delayed_ratio > 0.2:
            issues.append(f"High ratio of delayed orders: {delayed_ratio:.2%}")

    if issues:
        logging.warning(f"Validation issues found: {issues}")
    else:
        logging.info("✅ Data validation passed.")

with DAG(
    'dwd_orders_pipeline',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['dwd', 'orders', 'refactored'],
    description='Refactored DWD Orders ETL pipeline using a single Spark job.',
) as dag:

    start = DummyOperator(task_id='start')

    etl_task = PythonOperator(
        task_id='run_dwd_orders_etl_task',
        python_callable=run_dwd_orders_etl
    )

    create_views = PythonOperator(
        task_id='create_hive_views',
        python_callable=create_orders_hive_views
    )

    validate_dwd = PythonOperator(
        task_id='validate_dwd_data',
        python_callable=validate_orders_dwd
    )

    end = DummyOperator(task_id='end')

    start >> etl_task >> create_views >> validate_dwd >> end
end