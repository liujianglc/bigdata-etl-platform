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
    from pyspark.sql.types import DecimalType

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
            .config("spark.sql.parquet.cacheMetadata", "false") \
            .config("spark.sql.hive.metastorePartitionPruning", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.parquet.writeLegacyFormat", "false") \
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
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
        
        # Handle schema compatibility issues - convert INT96 to proper date types
        from pyspark.sql.functions import to_date, to_timestamp
        from pyspark.sql.types import DateType, TimestampType
        
        # Check and convert date columns that might be stored as INT96
        date_columns = ['OrderDate', 'RequiredDate', 'ShippedDate']
        timestamp_columns = ['CreatedDate', 'UpdatedDate']
        
        for col_name in date_columns:
            if col_name in df.columns:
                current_type = dict(df.dtypes)[col_name]
                if current_type != 'date':
                    logging.info(f"Converting {col_name} from {current_type} to date")
                    df = df.withColumn(col_name, to_date(col(col_name)))
        
        for col_name in timestamp_columns:
            if col_name in df.columns:
                current_type = dict(df.dtypes)[col_name]
                if current_type != 'timestamp':
                    logging.info(f"Converting {col_name} from {current_type} to timestamp")
                    df = df.withColumn(col_name, to_timestamp(col(col_name)))
        
        
        # Use limit(1).count() instead of rdd.isEmpty() for better performance
        record_count = df.limit(1).count()
        if record_count == 0:
            logging.warning("No records found for this batch.")
            table_name = "dwd_db.dwd_orders"
            location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders"
            
            # Create an empty DataFrame with the full transformed schema
            # This ensures empty partitions have the same schema as data partitions
            from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType, BooleanType, IntegerType
            
            transformed_schema = StructType([
                StructField("OrderID", IntegerType(), True),
                StructField("CustomerID", IntegerType(), True),
                StructField("OrderDate", DateType(), True),
                StructField("RequiredDate", DateType(), True),
                StructField("ShippedDate", DateType(), True),
                StructField("Status", StringType(), True),
                StructField("PaymentMethod", StringType(), True),
                StructField("PaymentStatus", StringType(), True),
                StructField("TotalAmount", DecimalType(10,2), True),
                StructField("Discount", DecimalType(10,2), True),
                StructField("ShippingAddress", StringType(), True),
                StructField("ShippingMethod", StringType(), True),
                StructField("Remarks", StringType(), True),
                StructField("CreatedBy", IntegerType(), True),
                StructField("CreatedDate", TimestampType(), True),
                StructField("UpdatedDate", TimestampType(), True),
                StructField("CustomerName", StringType(), True),
                StructField("CustomerType", StringType(), True),
                StructField("CreatedByName", StringType(), True),
                StructField("CreatedByDepartment", StringType(), True),
                # Transformed columns
                StructField("OrderStatus", StringType(), True),
                StructField("LeadTimeDays", IntegerType(), True),
                StructField("ProcessingDays", IntegerType(), True),
                StructField("IsDelayed", BooleanType(), True),
                StructField("DeliveryStatus", StringType(), True),
                StructField("OrderYear", IntegerType(), True),
                StructField("OrderMonth", IntegerType(), True),
                StructField("OrderDay", IntegerType(), True),
                StructField("OrderDayOfWeek", IntegerType(), True),
                StructField("OrderQuarter", IntegerType(), True),
                StructField("NetAmount", DecimalType(10,2), True),
                StructField("OrderSizeCategory", StringType(), True),
                StructField("OrderPriority", StringType(), True),
                StructField("DataQualityScore", IntegerType(), True),
                StructField("DataQualityLevel", StringType(), True),
                StructField("etl_created_date", TimestampType(), True),
                StructField("etl_batch_id", StringType(), True),
                StructField("is_empty_partition", BooleanType(), True),
            ])
            
            empty_df = spark.createDataFrame([], transformed_schema)
            
            # Use the utility function to handle empty partition
            from utils.empty_partition_handler import handle_empty_partition
            status = handle_empty_partition(spark, empty_df, table_name, batch_date, context, location)
            
            context['task_instance'].xcom_push(key='status', value=status)
            context['task_instance'].xcom_push(key='table_name', value=table_name)
            return

        # Cache after we know there's data
        df.cache()
        record_count = df.count()
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

        df = df.withColumn("OrderStatus", coalesce(order_status_map[col("Status")], col("Status")))
        df = df.withColumn("PaymentStatus", coalesce(payment_status_map[col("PaymentStatus")], col("PaymentStatus")))

        df = df.withColumn("LeadTimeDays", datediff(col("RequiredDate"), col("OrderDate"))) \
               .withColumn("ProcessingDays", datediff(col("ShippedDate"), col("CreatedDate"))) \
               .withColumn("IsDelayed", col("ShippedDate") > col("RequiredDate")) \
               .withColumn("DeliveryStatus", 
                         when(col("OrderStatus").isin(["Cancelled"]), "Cancelled")
                         .when(col("OrderStatus").isin(["Delivered"]), "Completed")
                         .when((col("ShippedDate").isNotNull()) & (col("ShippedDate") <= col("RequiredDate")), "OnTime")
                         .when((col("ShippedDate").isNotNull()) & (col("ShippedDate") > col("RequiredDate")), "Late")
                         .when((col("RequiredDate") < lit(batch_date)) & col("ShippedDate").isNull(), "Overdue")
                         .otherwise("Pending")) \
               .withColumn("OrderYear", year(col("OrderDate"))) \
               .withColumn("OrderMonth", month(col("OrderDate"))) \
               .withColumn("OrderDay", dayofmonth(col("OrderDate"))) \
               .withColumn("OrderDayOfWeek", dayofweek(col("OrderDate"))) \
               .withColumn("OrderQuarter", quarter(col("OrderDate"))) \
               .withColumn("NetAmount", (col("TotalAmount") - col("Discount"))) \
               .withColumn("OrderSizeCategory",
                         when(col("TotalAmount") >= 10000, "Large")
                         .when(col("TotalAmount") >= 5000, "Medium")
                         .when(col("TotalAmount") >= 1000, "Small")
                         .otherwise("Micro")) \
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
               .withColumn('etl_batch_id', lit(context['ds_nodash'])) \
               .withColumn('is_empty_partition', lit(False))

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
        
        # Clear any existing cache for this table to avoid file conflicts
        try:
            spark.catalog.uncacheTable("dwd_db.dwd_orders")
        except:
            pass  # Table might not be cached
        
        # Use a more robust write approach
        df_with_partition = df.withColumn('dt', lit(batch_date))
        
        # Write the data using overwrite mode with explicit partition handling
        df_with_partition.write \
          .mode("overwrite") \
          .partitionBy("dt") \
          .format("parquet") \
          .option("path", location) \
          .saveAsTable(table_name)

        # Comprehensive metadata refresh
        try:
            spark.sql("MSCK REPAIR TABLE dwd_db.dwd_orders")
        except Exception as e:
            logging.warning(f"MSCK REPAIR failed: {e}")
        
        try:
            spark.sql("REFRESH TABLE dwd_db.dwd_orders")
        except Exception as e:
            logging.warning(f"REFRESH TABLE failed: {e}")
        
        # Clear catalog cache to ensure fresh metadata
        try:
            spark.catalog.clearCache()
        except Exception as e:
            logging.warning(f"Clear cache failed: {e}")

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
    status = context['task_instance'].xcom_pull(task_ids='run_dwd_orders_etl_task', key='status')
    if status in ['SKIPPED_EMPTY_DATA', 'SUCCESS_EMPTY_PARTITION']:
        logging.warning(f"Skipping view creation as status is {status}.")
        return

    from pyspark.sql import SparkSession
    spark = None
    try:
        spark = SparkSession.builder.appName("CreateDWDOrderViews") \
            .config("spark.sql.catalogImplementation","hive") \
            .config("spark.hadoop.hive.metastore.uris","thrift://hive-metastore:9083") \
            .config("spark.sql.parquet.cacheMetadata", "false") \
            .config("spark.sql.hive.metastorePartitionPruning", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .config("spark.sql.parquet.writeLegacyFormat", "false") \
            .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sq
        spark.sql("USE dwd_db")
        
        # Refresh table metadata before checking schema
        try:
            spark.sql("REFRESH TABLE dwd_orders")
            spark.catalog.clearCache()
        except Exception as e:
            logging.warning(f"Failed to refresh table metadata: {e}")
        
        # Check if the table has transformed columns by examining the schema
        try:
            table_columns = [col.name for col in spark.table("dwd_orders").schema.fields]
            has_transformed_columns = all(col in table_columns for col in ['OrderStatus', 'DataQualityLevel', 'IsDelayed', 'OrderPriority'])
            has_empty_partition_flag = 'is_empty_partition' in table_columns
            
            if not has_transformed_columns:
                logging.warning("Table doesn't have transformed columns (likely empty partition), skipping view creation.")
                return
                
            logging.info(f"Table schema check: transformed_columns={has_transformed_columns}, empty_partition_flag={has_empty_partition_flag}")
        except Exception as e:
            logging.warning(f"Could not check table schema: {e}, skipping view creation.")
            return
        
        # 根据表是否有空分区标志来构建不同的视图
        empty_partition_filter = " AND (is_empty_partition IS NULL OR is_empty_partition = false)" if has_empty_partition_flag else ""
        
        views_sql =  [
            # 1. 当前活跃订单视图
            f"""
            CREATE OR REPLACE VIEW dwd_orders_active AS
            SELECT *
            FROM dwd_orders
            WHERE OrderStatus IN ('Pending', 'Confirmed', 'Shipping', 'Shipped')
              AND DataQualityLevel IN ('Good', 'Excellent'){empty_partition_filter}
            """,
            
            # 2. 延期订单视图  
            f"""
            CREATE OR REPLACE VIEW dwd_orders_delayed AS
            SELECT *
            FROM dwd_orders
            WHERE IsDelayed = true
              AND OrderStatus NOT IN ('Cancelled', 'Delivered'){empty_partition_filter}
            """,
            
            # 3. 各状态订单统计视图
            f"""
            CREATE OR REPLACE VIEW dwd_orders_status_summary AS
            SELECT 
                OrderStatus,
                COUNT(*) as order_count,
                SUM(TotalAmount) as total_amount,
                AVG(TotalAmount) as avg_amount,
                COUNT(CASE WHEN IsDelayed = true THEN 1 END) as delayed_count
            FROM dwd_orders
            WHERE 1=1{empty_partition_filter}
            GROUP BY OrderStatus
            """,
            
            # 4. 高价值订单视图
            f"""
            CREATE OR REPLACE VIEW dwd_orders_high_value AS
            SELECT *
            FROM dwd_orders
            WHERE OrderPriority = 'High'
              AND TotalAmount >= 10000{empty_partition_filter}
            """
        ]
        
        for view_sql in views_sql:
            spark.sql(view_sql)
        
        spark.sql("MSCK REPAIR TABLE dwd_db.dwd_orders")
        spark.sql("REFRESH TABLE dwd_db.dwd_orders")
        spark.catalog.clearCache()
        logging.info(f"✅ Successfully created {len(views_sql)} views.")
    except Exception as e:
        logging.error(f"View creation failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

def validate_orders_dwd(**context):
    """Validates the DWD Orders data quality."""
    status = context['task_instance'].xcom_pull(task_ids='run_dwd_orders_etl_task', key='status')
    if status == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping validation as no data was processed.")
        return
    elif status == 'SUCCESS_EMPTY_PARTITION':
        logging.info("✅ Empty partition validation passed.")
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