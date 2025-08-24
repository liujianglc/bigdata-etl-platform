from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def run_dws_orders_analytics_etl(**context):
    """A single, optimized Spark job for all Orders DWS aggregations."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, lit, date_format, year, month, datediff, desc
    from pyspark.sql.types import DecimalType, DoubleType
    from config.data_types_config import AMOUNT, AVERAGE, RATE, DAYS, FREQUENCY
    import logging
    import os
    from datetime import datetime, timedelta

    spark = None
    try:
        spark = SparkSession.builder \
            .appName("DWS_Orders_Analytics_ETL") \
            .master(os.getenv('SPARK_MASTER_URL', 'local[*]')) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", os.getenv('SPARK_DRIVER_MEMORY', '4g')) \
            .config("spark.executor.memory", os.getenv('SPARK_EXECUTOR_MEMORY', '4g')) \
            .enableHiveSupport() \
            .getOrCreate()
        logging.info("✅ Spark session created successfully.")

        batch_date = context['ds']
        current_month_str = datetime.strptime(batch_date, '%Y-%m-%d').strftime('%Y-%m')
        start_date_30d = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

        spark.sql("CREATE DATABASE IF NOT EXISTS dws_db")
        spark.sql("USE dws_db")
        
        # Enable dynamic partition mode for better handling
        spark.sql("SET hive.exec.dynamic.partition = true")
        spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
        spark.sql("SET spark.sql.sources.partitionOverwriteMode = dynamic")
        
        # Drop existing tables to ensure schema updates are applied
        try:
            spark.sql("DROP TABLE IF EXISTS dws_orders_daily_summary")
            spark.sql("DROP TABLE IF EXISTS dws_orders_monthly_summary")
            spark.sql("DROP TABLE IF EXISTS dws_customer_analytics")
            logging.info("✅ Dropped existing tables for schema refresh")
        except Exception as e:
            logging.warning(f"Could not drop existing tables: {e}")

        # 检查源表是否存在
        try:
            spark.sql("DESCRIBE TABLE dwd_db.dwd_orders")
            logging.info("✅ Source table dwd_db.dwd_orders exists")
        except Exception as e:
            logging.error(f"❌ Source table dwd_db.dwd_orders not found: {e}")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_NO_SOURCE_TABLE')
            return
        
        # Load DWD data and filter out empty partitions
        dwd_df_30d = spark.table("dwd_db.dwd_orders").filter(
            col("dt").between(start_date_30d, batch_date) &
            (col("is_empty_partition").isNull() | (col("is_empty_partition") == False))
        )
        dwd_df_30d.cache()
        
        # 记录数据范围和统计信息
        total_rows = dwd_df_30d.count()
        logging.info(f"📊 DWD data statistics:")
        logging.info(f"  - Date range: {start_date_30d} to {batch_date}")
        logging.info(f"  - Total rows in 30-day window: {total_rows}")
        
        if total_rows == 0:
            logging.warning("No DWD data for the last 30 days, skipping all aggregations.")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_EMPTY_DATA')
            return
        
        # 检查每天的数据分布
        try:
            daily_counts = dwd_df_30d.groupBy("dt").count().orderBy("dt").collect()
            logging.info(f"📅 Daily data distribution:")
            for row in daily_counts:
                logging.info(f"  - {row['dt']}: {row['count']} records")
        except Exception as e:
            logging.warning(f"Could not get daily data distribution: {e}")

        # 跟踪实际创建的表
        tables_created_list = []
        
        # --- 1. Daily Aggregation ---
        logging.info(f"🔄 Starting Daily Aggregation for date: {batch_date}")
        daily_df = dwd_df_30d.filter(col("dt") == batch_date)
        daily_count = daily_df.count()
        logging.info(f"📊 Daily data count for {batch_date}: {daily_count} records")
        
        # Check if this date had an empty partition in DWD
        try:
            empty_partition_check = spark.sql(f"""
                SELECT COUNT(*) as cnt 
                FROM dwd_db.dwd_orders 
                WHERE dt = '{batch_date}' 
                AND is_empty_partition = true
            """).collect()[0]['cnt']
            has_empty_partition = empty_partition_check > 0
        except:
            has_empty_partition = False
        
        if daily_count > 0:
            daily_summary = daily_df.groupBy(date_format(col("OrderDate"), "yyyy-MM-dd").alias("order_date")).agg(
                count("*").alias("total_orders"),
                sum("TotalAmount").cast(AMOUNT).alias("total_amount"),
                avg("TotalAmount").cast(AVERAGE).alias("avg_order_value"),
                sum("NetAmount").cast(AMOUNT).alias("total_net_amount"),
                count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders"),
                count(when(col("OrderStatus") == "Cancelled", 1)).alias("cancelled_orders"),
                count(when(col("IsDelayed") == True, 1)).alias("delayed_orders"),
                count(when(col("OrderPriority") == "High", 1)).alias("high_priority_orders"),
                count(when(col("CustomerType") == "VIP", 1)).alias("vip_orders"),
                avg("ProcessingDays").cast(DAYS).alias("avg_processing_days"),
                max("TotalAmount").cast(AMOUNT).alias("max_order_amount"),
                min("TotalAmount").cast(AMOUNT).alias("min_order_amount"),
                count(when(col("DataQualityLevel") == "Good", 1)).alias("good_quality_orders"),
                count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_orders")
            ).withColumn("completion_rate", ((col("completed_orders") / col("total_orders") * 100)).cast(RATE)) \
             .withColumn("cancellation_rate", ((col("cancelled_orders") / col("total_orders") * 100)).cast(RATE)) \
             .withColumn("delay_rate", ((col("delayed_orders") / col("total_orders") * 100)).cast(RATE)) \
             .withColumn("data_quality_score", (((col("good_quality_orders") * 4 + (col("total_orders") - col("good_quality_orders") - col("poor_quality_orders")) * 2) / col("total_orders"))).cast(RATE))
            
            try:
                daily_summary.withColumn("dt", lit(batch_date)) \
                    .write.mode("overwrite").partitionBy("dt").format("parquet") \
                    .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_daily_summary") \
                    .saveAsTable("dws_orders_daily_summary")
                
                # Verify table was created successfully
                spark.sql("REFRESH TABLE dws_db.dws_orders_daily_summary")
                row_count = spark.sql("SELECT COUNT(*) as cnt FROM dws_db.dws_orders_daily_summary").collect()[0]['cnt']
                tables_created_list.append("dws_orders_daily_summary")
                logging.info(f"✅ Daily aggregation complete and loaded. Row count: {row_count}")
            except Exception as e:
                logging.error(f"❌ Failed to create daily summary table: {e}")
                raise
        elif has_empty_partition:
            # Create zero-value aggregation for empty partition days
            logging.info(f"Creating zero-value daily summary for empty partition date: {batch_date}")
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
            
            zero_summary_schema = StructType([
                StructField("order_date", StringType(), True),
                StructField("total_orders", IntegerType(), True),
                StructField("total_amount", DecimalType(20,2), True),
                StructField("avg_order_value", DecimalType(20,2), True),
                StructField("total_net_amount", DecimalType(20,2), True),
                StructField("completed_orders", IntegerType(), True),
                StructField("cancelled_orders", IntegerType(), True),
                StructField("delayed_orders", IntegerType(), True),
                StructField("high_priority_orders", IntegerType(), True),
                StructField("vip_orders", IntegerType(), True),
                StructField("avg_processing_days", DecimalType(10,2), True),
                StructField("max_order_amount", DecimalType(20,2), True),
                StructField("min_order_amount", DecimalType(20,2), True),
                StructField("good_quality_orders", IntegerType(), True),
                StructField("poor_quality_orders", IntegerType(), True),
                StructField("completion_rate", DecimalType(10,2), True),
                StructField("cancellation_rate", DecimalType(10,2), True),
                StructField("delay_rate", DecimalType(10,2), True),
                StructField("data_quality_score", DecimalType(10,2), True),
                StructField("dt", StringType(), True)
            ])
            
            zero_data = [(batch_date, 0, 0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, 0.0, 0.0, batch_date)]
            zero_summary_df = spark.createDataFrame(zero_data, zero_summary_schema)
            
            zero_summary_df.write.mode("overwrite").partitionBy("dt").format("parquet") \
                .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_daily_summary") \
                .saveAsTable("dws_orders_daily_summary")
            
            tables_created_list.append("dws_orders_daily_summary")
            logging.info("✅ Zero-value daily aggregation created for empty partition.")
        else:
            logging.warning(f"No data found for date {batch_date}, skipping daily summary table creation.")

        # --- 2. Monthly Aggregation ---
        logging.info(f"🔄 Starting Monthly Aggregation for month: {current_month_str}")
        monthly_df = dwd_df_30d.filter(date_format(col("OrderDate"), "yyyy-MM") == current_month_str)
        monthly_count = monthly_df.count()
        logging.info(f"📊 Monthly data count for {current_month_str}: {monthly_count} records")
        
        if monthly_count > 0:
            monthly_summary = monthly_df.groupBy(date_format(col("OrderDate"), "yyyy-MM").alias("year_month")).agg(
                count("*").alias("total_orders"),
                sum("TotalAmount").cast(AMOUNT).alias("total_amount"),
                avg("TotalAmount").cast(AVERAGE).alias("avg_order_value"),
                sum("NetAmount").cast(AMOUNT).alias("total_net_amount"),
                count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders"),
                count(when(col("OrderStatus") == "Cancelled", 1)).alias("cancelled_orders"),
                count(when(col("IsDelayed") == True, 1)).alias("delayed_orders"),
                count(when(col("OrderPriority") == "High", 1)).alias("high_priority_orders"),
                count(when(col("CustomerType") == "VIP", 1)).alias("vip_orders"),
                avg("ProcessingDays").cast(DAYS).alias("avg_processing_days"),
                max("TotalAmount").cast(AMOUNT).alias("max_order_amount"),
                min("TotalAmount").cast(AMOUNT).alias("min_order_amount"),
                count(when(col("CustomerType") == "VIP", 1)).alias("vip_customer_orders"),
                count(when(col("CustomerType") == "Regular", 1)).alias("regular_customer_orders"),
                count(when(col("OrderSizeCategory") == "Large", 1)).alias("large_orders"),
                count(when(col("OrderSizeCategory") == "Medium", 1)).alias("medium_orders"),
                count(when(col("OrderSizeCategory") == "Small", 1)).alias("small_orders"),
                count(when(col("OrderSizeCategory") == "Micro", 1)).alias("micro_orders")
            ).withColumn("completion_rate", ((col("completed_orders") / col("total_orders") * 100)).cast(DoubleType())) \
             .withColumn("cancellation_rate", ((col("cancelled_orders") / col("total_orders") * 100)).cast(DoubleType())) \
             .withColumn("delay_rate", ((col("delayed_orders") / col("total_orders") * 100)).cast(DoubleType())) \
             .withColumn("vip_ratio", ((col("vip_orders") / col("total_orders") * 100)).cast(DoubleType())) \
             .withColumn("large_order_ratio", ((col("large_orders") / col("total_orders") * 100)).cast(DoubleType()))

            try:
                monthly_summary.withColumn("dt", lit(batch_date)) \
                    .write.mode("overwrite").partitionBy("dt").format("parquet") \
                    .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_monthly_summary") \
                    .saveAsTable("dws_orders_monthly_summary")
                
                # Verify table was created successfully
                spark.sql("REFRESH TABLE dws_db.dws_orders_monthly_summary")
                row_count = spark.sql("SELECT COUNT(*) as cnt FROM dws_db.dws_orders_monthly_summary").collect()[0]['cnt']
                tables_created_list.append("dws_orders_monthly_summary")
                logging.info(f"✅ Monthly aggregation complete and loaded. Row count: {row_count}")
            except Exception as e:
                logging.error(f"❌ Failed to create monthly summary table: {e}")
                raise
        else:
            logging.warning(f"No data found for month {current_month_str}, skipping monthly summary table creation.")

        # --- 3. Customer Analytics ---
        logging.info(f"🔄 Starting Customer Analytics for the last 30 days.")
        
        # 检查客户数据的质量
        try:
            customer_count = dwd_df_30d.select("CustomerID").distinct().count()
            logging.info(f"📊 Customer analytics data:")
            logging.info(f"  - Total records: {total_rows}")
            logging.info(f"  - Unique customers: {customer_count}")
            
            # 检查必要的字段是否存在
            required_columns = ["CustomerID", "CustomerName", "CustomerType", "TotalAmount", "OrderDate", "OrderStatus"]
            missing_columns = []
            for col_name in required_columns:
                if col_name not in dwd_df_30d.columns:
                    missing_columns.append(col_name)
            
            if missing_columns:
                logging.error(f"❌ Missing required columns for customer analytics: {missing_columns}")
                raise Exception(f"Missing required columns: {missing_columns}")
            
            logging.info("✅ All required columns present for customer analytics")
            
        except Exception as e:
            logging.error(f"❌ Failed to analyze customer data structure: {e}")
            raise
        
        customer_analytics = dwd_df_30d.groupBy("CustomerID", "CustomerName", "CustomerType").agg(
            count("*").alias("total_orders"),
            sum("TotalAmount").cast(AMOUNT).alias("total_spent"),
            avg("TotalAmount").cast(AVERAGE).alias("avg_order_value"),
            sum("NetAmount").cast(AMOUNT).alias("total_net_spent"),
            max("OrderDate").alias("last_order_date"),
            min("OrderDate").alias("first_order_date"),
            count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders"),
            count(when(col("OrderStatus") == "Cancelled", 1)).alias("cancelled_orders"),
            count(when(col("IsDelayed") == True, 1)).alias("delayed_orders"),
            count(when(col("OrderPriority") == "High", 1)).alias("high_priority_orders"),
            avg("ProcessingDays").cast(DAYS).alias("avg_processing_days"),
            max("TotalAmount").cast(AMOUNT).alias("max_order_amount"),
            count(when(col("OrderSizeCategory") == "Large", 1)).alias("large_orders"),
            count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_orders")
        ).withColumn("completion_rate", ((col("completed_orders") / col("total_orders") * 100)).cast(RATE)) \
         .withColumn("cancellation_rate", ((col("cancelled_orders") / col("total_orders") * 100)).cast(RATE)) \
         .withColumn("delay_rate", ((col("delayed_orders") / col("total_orders") * 100)).cast(RATE)) \
         .withColumn("days_since_last_order", datediff(lit(batch_date), col("last_order_date"))) \
         .withColumn("customer_lifetime_days", datediff(col("last_order_date"), col("first_order_date")) + 1) \
         .withColumn("order_frequency", (col("total_orders") / (col("customer_lifetime_days") / 30.0)).cast(FREQUENCY)) \
         .withColumn("customer_segment",
                    when(col("total_spent") >= 50000, "Platinum")
                    .when(col("total_spent") >= 20000, "Gold")
                    .when(col("total_spent") >= 5000, "Silver")
                    .otherwise("Bronze")) \
         .withColumn("customer_status",
                    when(col("days_since_last_order") <= 7, "Active")
                    .when(col("days_since_last_order") <= 30, "Recent")
                    .when(col("days_since_last_order") <= 90, "Inactive")
                    .otherwise("Dormant"))
        
        try:
            # 检查聚合结果
            customer_analytics_count = customer_analytics.count()
            logging.info(f"📊 Customer analytics aggregation result: {customer_analytics_count} customer records")
            
            if customer_analytics_count == 0:
                logging.warning("⚠️  Customer analytics aggregation resulted in 0 records")
                # 仍然创建空表以保持一致性
            
            customer_analytics.withColumn("dt", lit(batch_date)) \
                .write.mode("overwrite").partitionBy("dt").format("parquet") \
                .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_customer_analytics") \
                .saveAsTable("dws_customer_analytics")
            
            # Verify table was created successfully
            spark.sql("REFRESH TABLE dws_db.dws_customer_analytics")
            row_count = spark.sql("SELECT COUNT(*) as cnt FROM dws_db.dws_customer_analytics").collect()[0]['cnt']
            tables_created_list.append("dws_customer_analytics")
            logging.info(f"✅ Customer analytics complete and loaded. Row count: {row_count}")
            
        except Exception as e:
            logging.error(f"❌ Failed to create customer analytics table: {e}")
            logging.error(f"❌ Error details: {str(e)}")
            # 添加更详细的错误信息
            try:
                logging.info("🔍 Attempting to show sample data for debugging:")
                sample_data = dwd_df_30d.limit(5).toPandas()
                logging.info(f"Sample data columns: {sample_data.columns.tolist()}")
                logging.info(f"Sample data shape: {sample_data.shape}")
            except Exception as debug_e:
                logging.warning(f"Could not get sample data for debugging: {debug_e}")
            raise

        dwd_df_30d.unpersist()
        context['task_instance'].xcom_push(key='status', value='SUCCESS')
        context['task_instance'].xcom_push(key='tables_created', value=tables_created_list)
        logging.info(f"✅ ETL completed successfully. Tables created: {tables_created_list}")

    except Exception as e:
        logging.error(f"DWS Analytics ETL failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()


def create_analytics_views(**context):
    """Creates DWS analysis views."""
    from pyspark.sql import SparkSession
    import logging
    
    if context['task_instance'].xcom_pull(task_ids='run_dws_analytics_etl_task', key='status') == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping view creation as no data was processed.")
        return

    spark = None
    try:
        spark = SparkSession.builder \
            .appName("CreateDWSViews") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("USE dws_db")
        
        # 获取已创建的表列表
        tables_created_from_etl = context['task_instance'].xcom_pull(task_ids='run_dws_analytics_etl_task', key='tables_created') or []
        logging.info(f"Tables reported as created by ETL: {tables_created_from_etl}")
        
        # 验证表是否真正存在
        verified_tables = []
        for table_name in tables_created_from_etl:
            try:
                spark.sql(f"DESCRIBE TABLE dws_db.{table_name}")
                verified_tables.append(table_name)
                logging.info(f"✅ Verified table exists: {table_name}")
            except Exception as e:
                logging.warning(f"❌ Table {table_name} not found or not accessible: {e}")
        
        logging.info(f"Final verified tables list: {verified_tables}")
        
        if not verified_tables:
            logging.error("No verified tables exist. Cannot create views.")
            return
        
        # 详细记录表依赖关系
        logging.info("Table dependencies for view creation:")
        logging.info(f"  - dws_orders_weekly_trend depends on: dws_orders_daily_summary")
        logging.info(f"  - dws_monthly_business_metrics depends on: dws_orders_monthly_summary")
        logging.info(f"  - dws_customer_value_segments depends on: dws_customer_analytics")
        
        # Create comprehensive views based on existing tables
        views_sql = []
        tables_created = verified_tables  # Use verified_tables for consistency
        
        # 如果日汇总表存在，创建相关视图
        if 'dws_orders_daily_summary' in tables_created:
            views_sql.extend([
                # 最近7天趋势视图
                """
                CREATE OR REPLACE VIEW dws_orders_weekly_trend AS
                SELECT 
                    order_date,
                    total_orders,
                    total_amount,
                    completion_rate,
                    delay_rate,
                    LAG(total_orders, 1) OVER (ORDER BY order_date) as prev_day_orders,
                    LAG(total_amount, 1) OVER (ORDER BY order_date) as prev_day_amount
                FROM dws_db.dws_orders_daily_summary
                WHERE order_date >= date_sub(current_date(), 7)
                ORDER BY order_date DESC
                """,
                
                # 日汇总KPI视图
                """
                CREATE OR REPLACE VIEW dws_daily_kpi AS
                SELECT 
                    order_date,
                    total_orders,
                    total_amount,
                    avg_order_value,
                    completion_rate,
                    cancellation_rate,
                    delay_rate,
                    CASE 
                        WHEN completion_rate >= 95 THEN 'Excellent'
                        WHEN completion_rate >= 90 THEN 'Good'
                        WHEN completion_rate >= 80 THEN 'Fair'
                        ELSE 'Poor'
                    END as performance_grade
                FROM dws_db.dws_orders_daily_summary
                """
            ])
        
        # 如果月汇总表存在，创建相关视图
        if 'dws_orders_monthly_summary' in tables_created:
            views_sql.extend([
                # 月度趋势视图
                """
                CREATE OR REPLACE VIEW dws_orders_monthly_trend AS
                SELECT 
                    year_month,
                    total_orders,
                    total_amount,
                    avg_order_value,
                    vip_ratio,
                    large_order_ratio,
                    completion_rate,
                    LAG(total_orders, 1) OVER (ORDER BY year_month) as prev_month_orders,
                    LAG(total_amount, 1) OVER (ORDER BY year_month) as prev_month_amount
                FROM dws_db.dws_orders_monthly_summary
                ORDER BY year_month DESC
                """,
                
                # 月度业务指标视图
                """
                CREATE OR REPLACE VIEW dws_monthly_business_metrics AS
                SELECT 
                    year_month,
                    total_orders,
                    total_amount,
                    vip_orders,
                    large_orders,
                    ROUND(vip_ratio, 2) as vip_percentage,
                    ROUND(large_order_ratio, 2) as large_order_percentage,
                    ROUND(completion_rate, 2) as completion_percentage,
                    ROUND(avg_processing_days, 1) as avg_processing_days
                FROM dws_db.dws_orders_monthly_summary
                """
            ])
        
        # 如果客户分析表存在，创建相关视图
        if 'dws_customer_analytics' in tables_created:
            views_sql.extend([
                # 客户价值分段视图
                """
                CREATE OR REPLACE VIEW dws_customer_value_segments AS
                SELECT 
                    customer_segment,
                    COUNT(*) as customer_count,
                    SUM(total_spent) as segment_revenue,
                    AVG(total_spent) as avg_customer_value,
                    AVG(total_orders) as avg_orders_per_customer,
                    AVG(avg_order_value) as avg_order_value,
                    SUM(CASE WHEN customer_status = 'Active' THEN 1 ELSE 0 END) as active_customers
                FROM dws_db.dws_customer_analytics
                GROUP BY customer_segment
                ORDER BY 
                    CASE customer_segment 
                        WHEN 'Platinum' THEN 1
                        WHEN 'Gold' THEN 2
                        WHEN 'Silver' THEN 3
                        WHEN 'Bronze' THEN 4
                    END
                """,
                
                # 客户状态分析视图
                """
                CREATE OR REPLACE VIEW dws_customer_status_analysis AS
                SELECT 
                    customer_status,
                    COUNT(*) as customer_count,
                    SUM(total_spent) as status_revenue,
                    AVG(days_since_last_order) as avg_days_since_last_order,
                    AVG(order_frequency) as avg_monthly_frequency
                FROM dws_db.dws_customer_analytics
                GROUP BY customer_status
                """,
                
                # 高价值客户视图
                """
                CREATE OR REPLACE VIEW dws_high_value_customers AS
                SELECT 
                    CustomerID,
                    CustomerName,
                    CustomerType,
                    customer_segment,
                    customer_status,
                    total_spent,
                    total_orders,
                    avg_order_value,
                    completion_rate,
                    days_since_last_order,
                    order_frequency
                FROM dws_db.dws_customer_analytics
                WHERE customer_segment IN ('Platinum', 'Gold')
                   OR total_spent >= 20000
                ORDER BY total_spent DESC
                """
            ])
        
        # Create views one by one with error handling
        created_views = []
        for i, view_sql in enumerate(views_sql):
            try:
                spark.sql(view_sql.strip())
                # Extract view name from SQL for logging
                view_name = view_sql.split('VIEW')[1].split('AS')[0].strip()
                
                # Refresh the view to ensure metadata is synchronized
                try:
                    spark.sql(f"REFRESH TABLE dws_db.{view_name}")
                    logging.info(f"🔄 Refreshed view metadata: {view_name}")
                except Exception as refresh_e:
                    logging.warning(f"⚠️  Could not refresh view {view_name}: {refresh_e}")
                
                created_views.append(view_name)
                logging.info(f"✅ Successfully created view: {view_name}")
            except Exception as e:
                logging.error(f"❌ Failed to create view #{i+1}: {e}")
                # Continue creating other views even if one fails
        
        if created_views:
            logging.info(f"✅ Successfully created {len(created_views)} views: {created_views}")
            
            # Final refresh to ensure all metadata is synchronized
            try:
                spark.sql("REFRESH")
                logging.info("🔄 Performed final metadata refresh for all tables and views")
            except Exception as final_refresh_e:
                logging.warning(f"⚠️  Could not perform final metadata refresh: {final_refresh_e}")
        else:
            logging.warning("⚠️  No views were created successfully")
            
    except Exception as e:
        logging.error(f"View creation process failed: {e}", exc_info=True)
        # Don't raise the exception to avoid failing the entire DAG
        # Just log the error and continue
    finally:
        if spark: 
            spark.stop()

with DAG(
    'dws_orders_analytics',
    default_args=default_args,
    schedule_interval='0 4 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['dws', 'analytics', 'refactored'],
    description='Refactored DWS Orders analytics pipeline using a single Spark job.',
) as dag:

    start = DummyOperator(task_id='start')

    # In a real scenario, a dependency check task would be here.
    # check_dependencies_task = ...

    etl_task = PythonOperator(
        task_id='run_dws_analytics_etl_task',
        python_callable=run_dws_orders_analytics_etl
    )

    create_views = PythonOperator(
        task_id='create_analytics_views',
        python_callable=create_analytics_views
    )

    end = DummyOperator(task_id='end')

    start >> etl_task >> create_views >> end
