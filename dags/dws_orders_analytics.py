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

        dwd_df_30d = spark.table("dwd_db.dwd_orders").filter(col("dt").between(start_date_30d, batch_date))
        dwd_df_30d.cache()

        if dwd_df_30d.rdd.isEmpty():
            logging.warning("No DWD data for the last 30 days, skipping all aggregations.")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_EMPTY_DATA')
            return

        # --- 1. Daily Aggregation ---
        logging.info(f"Starting Daily Aggregation for date: {batch_date}")
        daily_df = dwd_df_30d.filter(col("dt") == batch_date)
        if not daily_df.rdd.isEmpty():
            daily_summary = daily_df.groupBy(date_format(col("OrderDate"), "yyyy-MM-dd").alias("order_date")).agg(
                count("*").alias("total_orders"),
                sum("TotalAmount").alias("total_amount"),
                avg("TotalAmount").alias("avg_order_value"),
                sum("NetAmount").alias("total_net_amount"),
                count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders"),
                count(when(col("OrderStatus") == "Cancelled", 1)).alias("cancelled_orders"),
                count(when(col("IsDelayed") == True, 1)).alias("delayed_orders"),
                count(when(col("OrderPriority") == "High", 1)).alias("high_priority_orders"),
                count(when(col("CustomerType") == "VIP", 1)).alias("vip_orders"),
                avg("ProcessingDays").alias("avg_processing_days"),
                max("TotalAmount").alias("max_order_amount"),
                min("TotalAmount").alias("min_order_amount"),
                count(when(col("DataQualityLevel") == "Good", 1)).alias("good_quality_orders"),
                count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_orders")
            ).withColumn("completion_rate", (col("completed_orders") / col("total_orders") * 100)) \
             .withColumn("cancellation_rate", (col("cancelled_orders") / col("total_orders") * 100)) \
             .withColumn("delay_rate", (col("delayed_orders") / col("total_orders") * 100)) \
             .withColumn("data_quality_score", ((col("good_quality_orders") * 4 + (col("total_orders") - col("good_quality_orders") - col("poor_quality_orders")) * 2) / col("total_orders")))
            
            daily_summary.withColumn("dt", lit(batch_date)) \
                .write.mode("overwrite").partitionBy("dt").format("parquet") \
                .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_daily_summary") \
                .saveAsTable("dws_orders_daily_summary")
            logging.info("✅ Daily aggregation complete and loaded.")

        # --- 2. Monthly Aggregation ---
        logging.info(f"Starting Monthly Aggregation for month: {current_month_str}")
        monthly_df = dwd_df_30d.filter(date_format(col("OrderDate"), "yyyy-MM") == current_month_str)
        if not monthly_df.rdd.isEmpty():
            monthly_summary = monthly_df.groupBy(date_format(col("OrderDate"), "yyyy-MM").alias("year_month")).agg(
                count("*").alias("total_orders"),
                sum("TotalAmount").alias("total_amount"),
                avg("TotalAmount").alias("avg_order_value"),
                sum("NetAmount").alias("total_net_amount"),
                count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders"),
                count(when(col("OrderStatus") == "Cancelled", 1)).alias("cancelled_orders"),
                count(when(col("IsDelayed") == True, 1)).alias("delayed_orders"),
                count(when(col("OrderPriority") == "High", 1)).alias("high_priority_orders"),
                count(when(col("CustomerType") == "VIP", 1)).alias("vip_orders"),
                avg("ProcessingDays").alias("avg_processing_days"),
                max("TotalAmount").alias("max_order_amount"),
                min("TotalAmount").alias("min_order_amount"),
                count(when(col("CustomerType") == "VIP", 1)).alias("vip_customer_orders"),
                count(when(col("CustomerType") == "Regular", 1)).alias("regular_customer_orders"),
                count(when(col("OrderSizeCategory") == "Large", 1)).alias("large_orders"),
                count(when(col("OrderSizeCategory") == "Medium", 1)).alias("medium_orders"),
                count(when(col("OrderSizeCategory") == "Small", 1)).alias("small_orders"),
                count(when(col("OrderSizeCategory") == "Micro", 1)).alias("micro_orders")
            ).withColumn("completion_rate", (col("completed_orders") / col("total_orders") * 100)) \
             .withColumn("cancellation_rate", (col("cancelled_orders") / col("total_orders") * 100)) \
             .withColumn("delay_rate", (col("delayed_orders") / col("total_orders") * 100)) \
             .withColumn("vip_ratio", (col("vip_orders") / col("total_orders") * 100)) \
             .withColumn("large_order_ratio", (col("large_orders") / col("total_orders") * 100))

            monthly_summary.withColumn("dt", lit(batch_date)) \
                .write.mode("overwrite").partitionBy("dt").format("parquet") \
                .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_monthly_summary") \
                .saveAsTable("dws_orders_monthly_summary")
            logging.info("✅ Monthly aggregation complete and loaded.")

        # --- 3. Customer Analytics ---
        logging.info(f"Starting Customer Analytics for the last 30 days.")
        customer_analytics = dwd_df_30d.groupBy("CustomerID", "CustomerName", "CustomerType").agg(
            count("*").alias("total_orders"),
            sum("TotalAmount").alias("total_spent"),
            avg("TotalAmount").alias("avg_order_value"),
            sum("NetAmount").alias("total_net_spent"),
            max("OrderDate").alias("last_order_date"),
            min("OrderDate").alias("first_order_date"),
            count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders"),
            count(when(col("OrderStatus") == "Cancelled", 1)).alias("cancelled_orders"),
            count(when(col("IsDelayed") == True, 1)).alias("delayed_orders"),
            count(when(col("OrderPriority") == "High", 1)).alias("high_priority_orders"),
            avg("ProcessingDays").alias("avg_processing_days"),
            max("TotalAmount").alias("max_order_amount"),
            count(when(col("OrderSizeCategory") == "Large", 1)).alias("large_orders"),
            count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_orders")
        ).withColumn("completion_rate", (col("completed_orders") / col("total_orders") * 100)) \
         .withColumn("cancellation_rate", (col("cancelled_orders") / col("total_orders") * 100)) \
         .withColumn("delay_rate", (col("delayed_orders") / col("total_orders") * 100)) \
         .withColumn("days_since_last_order", datediff(lit(batch_date), col("last_order_date"))) \
         .withColumn("customer_lifetime_days", datediff(col("last_order_date"), col("first_order_date")) + 1) \
         .withColumn("order_frequency", col("total_orders") / (col("customer_lifetime_days") / 30.0)) \
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
        
        customer_analytics.withColumn("dt", lit(batch_date)) \
            .write.mode("overwrite").partitionBy("dt").format("parquet") \
            .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_customer_analytics") \
            .saveAsTable("dws_customer_analytics")
        logging.info("✅ Customer analytics complete and loaded.")

        dwd_df_30d.unpersist()
        context['task_instance'].xcom_push(key='status', value='SUCCESS')
        context['task_instance'].xcom_push(key='tables_created', value=['dws_orders_daily_summary', 'dws_orders_monthly_summary', 'dws_customer_analytics'])

    except Exception as e:
        logging.error(f"DWS Analytics ETL failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()


def create_analytics_views(**context):
    """Creates DWS analysis views."""
    if context['task_instance'].xcom_pull(task_ids='run_dws_analytics_etl_task', key='status') == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping view creation as no data was processed.")
        return

    from pyspark.sql import SparkSession
    import logging
    spark = None
    try:
        spark = SparkSession.builder.appName("CreateDWSViews").config("spark.sql.catalogImplementation","hive").config("spark.hadoop.hive.metastore.uris","thrift://hive-metastore:9083").enableHiveSupport().getOrCreate()
        spark.sql("USE dws_db")
        
        views_sql = [
            "CREATE OR REPLACE VIEW dws_orders_weekly_trend AS SELECT order_date, total_orders, total_amount FROM dws_orders_daily_summary WHERE order_date >= date_sub(current_date(), 7) ORDER BY order_date DESC",
            "CREATE OR REPLACE VIEW dws_orders_monthly_trend AS SELECT year_month, total_orders, total_amount, avg_order_value FROM dws_orders_monthly_summary ORDER BY year_month DESC",
            "CREATE OR REPLACE VIEW dws_high_value_customers AS SELECT CustomerID, CustomerName, total_spent, total_orders, last_order_date, customer_segment FROM dws_customer_analytics ORDER BY total_spent DESC LIMIT 100"
        ]
        
        for view_sql in views_sql:
            spark.sql(view_sql)
        logging.info(f"✅ Successfully created {len(views_sql)} views.")
    except Exception as e:
        logging.error(f"View creation failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

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
