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
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.sql.hive.convertMetastoreParquet", "false") \
            .config("spark.sql.parquet.mergeSchema", "true") \
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

        df = df.withColumn("OrderStatus", coalesce(order_status_map[col("Status")], col("Status")))
        df = df.withColumn("PaymentStatus", coalesce(payment_status_map[col("PaymentStatus")], col("PaymentStatus")))

        # Cast decimal columns to avoid precision issues
        df = df.withColumn("TotalAmount", col("TotalAmount").cast("decimal(20,2)")) \
               .withColumn("Discount", col("Discount").cast("decimal(20,2)"))

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
               .withColumn("NetAmount", (col("TotalAmount") - col("Discount")).cast("decimal(20,2)")) \
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
        
        # Cast all columns to ensure consistent data types
        df = df.withColumn("OrderID", col("OrderID").cast("string")) \
               .withColumn("CustomerID", col("CustomerID").cast("string")) \
               .withColumn("CreatedBy", col("CreatedBy").cast("string"))
        
        # Drop and recreate table to handle schema conflicts
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            logging.info("Dropped existing table to resolve schema conflicts")
        except Exception as e:
            logging.warning(f"Could not drop table (may not exist): {e}")
        
        # Create table with explicit schema
        create_table_sql = f"""
        CREATE TABLE {table_name} (
            OrderID string,
            CustomerID string,
            OrderDate date,
            RequiredDate date,
            ShippedDate date,
            Status string,
            TotalAmount decimal(20,2),
            Discount decimal(20,2),
            PaymentStatus string,
            ShippingAddress string,
            CreatedDate timestamp,
            CreatedBy string,
            Remarks string,
            CustomerName string,
            CustomerType string,
            CreatedByName string,
            CreatedByDepartment string,
            OrderStatus string,
            LeadTimeDays int,
            ProcessingDays int,
            IsDelayed boolean,
            DeliveryStatus string,
            OrderYear int,
            OrderMonth int,
            OrderDay int,
            OrderDayOfWeek int,
            OrderQuarter int,
            NetAmount decimal(20,2),
            OrderSizeCategory string,
            OrderPriority string,
            DataQualityScore int,
            DataQualityLevel string,
            etl_created_date timestamp,
            etl_batch_id string
        )
        PARTITIONED BY (dt string)
        STORED AS PARQUET
        LOCATION '{location}'
        """
        spark.sql(create_table_sql)
        
        # Write data with schema enforcement
        df.withColumn('dt', lit(batch_date)) \
          .write.mode("overwrite") \
          .partitionBy("dt") \
          .format("parquet") \
          .option("path", location) \
          .saveAsTable(table_name)
        
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
        
        views_sql =  [
            # 1. 当前活跃订单视图
            """
            CREATE OR REPLACE VIEW dwd_orders_active AS
            SELECT *
            FROM dwd_orders
            WHERE OrderStatus IN ('Pending', 'Confirmed', 'Shipping', 'Shipped')
              AND DataQualityLevel IN ('Good', 'Excellent')
            """,
            
            # 2. 延期订单视图  
            """
            CREATE OR REPLACE VIEW dwd_orders_delayed AS
            SELECT *
            FROM dwd_orders
            WHERE IsDelayed = true
              AND OrderStatus NOT IN ('Cancelled', 'Delivered')
            """,
            
            # 3. 各状态订单统计视图
            """
            CREATE OR REPLACE VIEW dwd_orders_status_summary AS
            SELECT 
                OrderStatus,
                COUNT(*) as order_count,
                SUM(TotalAmount) as total_amount,
                AVG(TotalAmount) as avg_amount,
                COUNT(CASE WHEN IsDelayed = true THEN 1 END) as delayed_count
            FROM dwd_orders
            GROUP BY OrderStatus
            """,
            
            # 4. 高价值订单视图
            """
            CREATE OR REPLACE VIEW dwd_orders_high_value AS
            SELECT *
            FROM dwd_orders
            WHERE OrderPriority = 'High'
              AND TotalAmount >= 10000
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

def load_analytics_to_hdfs(**context):
    """将所有分析结果加载到HDFS"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    import pandas as pd
    import logging
    import os
    import tempfile
    from datetime import datetime
    
    spark = None
    try:
        # 获取所有分析结果文件
        daily_file = context['task_instance'].xcom_pull(task_ids='aggregate_daily_orders', key='daily_summary_file')
        monthly_file = context['task_instance'].xcom_pull(task_ids='aggregate_monthly_orders', key='monthly_summary_file')
        customer_file = context['task_instance'].xcom_pull(task_ids='aggregate_customer_analytics', key='customer_analytics_file')
        
        # 获取临时目录用于清理
        daily_temp_dir = context['task_instance'].xcom_pull(task_ids='aggregate_daily_orders', key='daily_temp_dir')
        monthly_temp_dir = context['task_instance'].xcom_pull(task_ids='aggregate_monthly_orders', key='monthly_temp_dir')
        customer_temp_dir = context['task_instance'].xcom_pull(task_ids='aggregate_customer_analytics', key='customer_temp_dir')
        
        # 创建Spark会话 - 优化内存配置
        spark = SparkSession.builder \
            .appName("DWS Analytics Load to HDFS") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logging.info("✅ Spark会话创建成功")
        
        # 确保DWS数据库存在
        spark.sql("CREATE DATABASE IF NOT EXISTS dws_db")
        spark.sql("USE dws_db")
        logging.info("✅ DWS数据库已准备就绪")
        
        batch_date = context['ds']
        tables_created = []
        
        # 1. 加载日汇总数据
        if daily_file and os.path.exists(daily_file):
            daily_df = pd.read_parquet(daily_file)
            if len(daily_df) > 0:
                spark_daily_df = spark.createDataFrame(daily_df)
                
                # 写入日汇总表
                table_location = "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_daily_summary"
                
                # 检查表是否存在
                try:
                    spark.sql("DESCRIBE dws_orders_daily_summary")
                    # 删除当天分区
                    spark.sql(f"ALTER TABLE dws_orders_daily_summary DROP IF EXISTS PARTITION (dt='{batch_date}')")
                    logging.info("删除已存在的日汇总分区")
                except Exception:
                    logging.info("日汇总表不存在，将创建新表")
                
                spark_daily_df.coalesce(1).write.mode("append").partitionBy("dt") \
                    .option("path", table_location).saveAsTable("dws_orders_daily_summary")
                
                tables_created.append("dws_orders_daily_summary")
                logging.info(f"✅ 日汇总数据已加载: {len(daily_df)} 条记录")
        
        # 2. 加载月汇总数据
        if monthly_file and os.path.exists(monthly_file):
            monthly_df = pd.read_parquet(monthly_file)
            if len(monthly_df) > 0:
                spark_monthly_df = spark.createDataFrame(monthly_df)
                
                table_location = "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_monthly_summary"
                
                try:
                    spark.sql("DESCRIBE dws_orders_monthly_summary")
                    spark.sql(f"ALTER TABLE dws_orders_monthly_summary DROP IF EXISTS PARTITION (dt='{batch_date}')")
                    logging.info("删除已存在的月汇总分区")
                except Exception:
                    logging.info("月汇总表不存在，将创建新表")
                
                spark_monthly_df.coalesce(1).write.mode("append").partitionBy("dt") \
                    .option("path", table_location).saveAsTable("dws_orders_monthly_summary")
                
                tables_created.append("dws_orders_monthly_summary")
                logging.info(f"✅ 月汇总数据已加载: {len(monthly_df)} 条记录")
        
        # 3. 加载客户分析数据
        if customer_file and os.path.exists(customer_file):
            customer_df = pd.read_parquet(customer_file)
            if len(customer_df) > 0:
                spark_customer_df = spark.createDataFrame(customer_df)
                
                table_location = "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_customer_analytics"
                
                try:
                    spark.sql("DESCRIBE dws_customer_analytics")
                    spark.sql(f"ALTER TABLE dws_customer_analytics DROP IF EXISTS PARTITION (dt='{batch_date}')")
                    logging.info("删除已存在的客户分析分区")
                except Exception:
                    logging.info("客户分析表不存在，将创建新表")
                
                spark_customer_df.coalesce(1).write.mode("append").partitionBy("dt") \
                    .option("path", table_location).saveAsTable("dws_customer_analytics")
                
                tables_created.append("dws_customer_analytics")
                logging.info(f"✅ 客户分析数据已加载: {len(customer_df)} 条记录")
        
        # 刷新所有表的元数据
        for table in tables_created:
            spark.sql(f"REFRESH TABLE {table}")
            logging.info(f"已刷新表元数据: {table}")
        
        # 清理临时目录
        import shutil
        for temp_dir in [daily_temp_dir, monthly_temp_dir, customer_temp_dir]:
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logging.info(f"已清理临时目录: {temp_dir}")
                except Exception as e:
                    logging.warning(f"清理临时目录失败: {e}")
        
        # 构建加载摘要
        load_summary = {
            'tables_created': tables_created,
            'batch_date': batch_date,
            'total_tables': len(tables_created)
        }
        
        logging.info(f"✅ DWS数据加载完成: {load_summary}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='load_summary', value=load_summary)
        context['task_instance'].xcom_push(key='tables_created', value=tables_created)
        
        return load_summary
        
    except Exception as e:
        logging.error(f"DWS数据加载失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logging.info("Spark会话已关闭")
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def aggregate_daily_orders(**context):
    """聚合日订单数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min
    import pandas as pd
    import tempfile
    import os
    
    spark = None
    temp_dir = None
    try:
        spark = SparkSession.builder \
            .appName("Daily Orders Aggregation") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        
        batch_date = context['ds']
        
        # 聚合日订单数据
        daily_agg = spark.sql(f"""
            SELECT 
                '{batch_date}' as dt,
                OrderDate as order_date,
                COUNT(*) as total_orders,
                SUM(TotalAmount) as total_amount,
                AVG(TotalAmount) as avg_order_amount,
                COUNT(CASE WHEN OrderStatus = 'Completed' THEN 1 END) as completed_orders,
                COUNT(CASE WHEN IsDelayed = true THEN 1 END) as delayed_orders,
                MAX(TotalAmount) as max_order_amount,
                MIN(TotalAmount) as min_order_amount
            FROM dwd_db.dwd_orders 
            WHERE dt = '{batch_date}'
            GROUP BY OrderDate
        """)
        
        # 转换为Pandas DataFrame并保存到临时文件
        daily_pandas_df = daily_agg.toPandas()
        temp_dir = tempfile.mkdtemp(prefix='daily_orders_')
        daily_file = os.path.join(temp_dir, 'daily_summary.parquet')
        daily_pandas_df.to_parquet(daily_file, index=False)
        
        # 保存文件路径到XCom
        context['task_instance'].xcom_push(key='daily_summary_file', value=daily_file)
        context['task_instance'].xcom_push(key='daily_temp_dir', value=temp_dir)
        
        logging.info(f"✅ 日订单聚合完成，生成 {len(daily_pandas_df)} 条记录")
        return daily_file
        
    except Exception as e:
        logging.error(f"日订单聚合失败: {e}")
        raise
    finally:
        if spark:
            spark.stop()

def aggregate_monthly_orders(**context):
    """聚合月订单数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min
    import pandas as pd
    import tempfile
    import os
    
    spark = None
    temp_dir = None
    try:
        spark = SparkSession.builder \
            .appName("Monthly Orders Aggregation") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        
        batch_date = context['ds']
        
        # 聚合月订单数据
        monthly_agg = spark.sql(f"""
            SELECT 
                '{batch_date}' as dt,
                OrderYear as order_year,
                OrderMonth as order_month,
                COUNT(*) as total_orders,
                SUM(TotalAmount) as total_amount,
                AVG(TotalAmount) as avg_order_amount,
                COUNT(CASE WHEN OrderStatus = 'Completed' THEN 1 END) as completed_orders,
                COUNT(CASE WHEN IsDelayed = true THEN 1 END) as delayed_orders,
                COUNT(DISTINCT CustomerID) as unique_customers,
                MAX(TotalAmount) as max_order_amount,
                MIN(TotalAmount) as min_order_amount
            FROM dwd_db.dwd_orders 
            WHERE dt = '{batch_date}'
            GROUP BY OrderYear, OrderMonth
        """)
        
        # 转换为Pandas DataFrame并保存到临时文件
        monthly_pandas_df = monthly_agg.toPandas()
        temp_dir = tempfile.mkdtemp(prefix='monthly_orders_')
        monthly_file = os.path.join(temp_dir, 'monthly_summary.parquet')
        monthly_pandas_df.to_parquet(monthly_file, index=False)
        
        # 保存文件路径到XCom
        context['task_instance'].xcom_push(key='monthly_summary_file', value=monthly_file)
        context['task_instance'].xcom_push(key='monthly_temp_dir', value=temp_dir)
        
        logging.info(f"✅ 月订单聚合完成，生成 {len(monthly_pandas_df)} 条记录")
        return monthly_file
        
    except Exception as e:
        logging.error(f"月订单聚合失败: {e}")
        raise
    finally:
        if spark:
            spark.stop()

def aggregate_customer_analytics(**context):
    """聚合客户分析数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, first
    import pandas as pd
    import tempfile
    import os
    
    spark = None
    temp_dir = None
    try:
        spark = SparkSession.builder \
            .appName("Customer Analytics Aggregation") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        
        batch_date = context['ds']
        
        # 聚合客户分析数据
        customer_agg = spark.sql(f"""
            SELECT 
                '{batch_date}' as dt,
                CustomerID,
                MAX(CustomerName) as customer_name,
                MAX(CustomerType) as customer_type,
                COUNT(*) as total_orders,
                SUM(TotalAmount) as total_spent,
                AVG(TotalAmount) as avg_order_amount,
                COUNT(CASE WHEN OrderStatus = 'Completed' THEN 1 END) as completed_orders,
                COUNT(CASE WHEN IsDelayed = true THEN 1 END) as delayed_orders,
                MAX(TotalAmount) as max_order_amount,
                MIN(TotalAmount) as min_order_amount,
                COUNT(CASE WHEN OrderPriority = 'High' THEN 1 END) as high_priority_orders,
                AVG(DataQualityScore) as avg_data_quality_score
            FROM dwd_db.dwd_orders 
            WHERE dt = '{batch_date}'
            GROUP BY CustomerID
        """)
        
        # 转换为Pandas DataFrame并保存到临时文件
        customer_pandas_df = customer_agg.toPandas()
        temp_dir = tempfile.mkdtemp(prefix='customer_analytics_')
        customer_file = os.path.join(temp_dir, 'customer_analytics.parquet')
        customer_pandas_df.to_parquet(customer_file, index=False)
        
        # 保存文件路径到XCom
        context['task_instance'].xcom_push(key='customer_analytics_file', value=customer_file)
        context['task_instance'].xcom_push(key='customer_temp_dir', value=temp_dir)
        
        logging.info(f"✅ 客户分析聚合完成，生成 {len(customer_pandas_df)} 条记录")
        return customer_file
        
    except Exception as e:
        logging.error(f"客户分析聚合失败: {e}")
        raise
    finally:
        if spark:
            spark.stop()

with DAG(
    'dwd_orders_pipeline',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['dwd', 'orders', 'refactored', 'dws', 'analytics'],
    description='Complete DWD Orders ETL pipeline with DWS analytics aggregation.',
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

    # DWS层聚合任务
    aggregate_daily = PythonOperator(
        task_id='aggregate_daily_orders',
        python_callable=aggregate_daily_orders
    )

    aggregate_monthly = PythonOperator(
        task_id='aggregate_monthly_orders',
        python_callable=aggregate_monthly_orders
    )

    aggregate_customer = PythonOperator(
        task_id='aggregate_customer_analytics',
        python_callable=aggregate_customer_analytics
    )

    # 加载到HDFS任务
    load_to_hdfs = PythonOperator(
        task_id='load_analytics_to_hdfs',
        python_callable=load_analytics_to_hdfs
    )

    end = DummyOperator(task_id='end')

    # 任务依赖关系
    start >> etl_task >> create_views >> validate_dwd
    validate_dwd >> [aggregate_daily, aggregate_monthly, aggregate_customer]
    [aggregate_daily, aggregate_monthly, aggregate_customer] >> load_to_hdfs >> end
end