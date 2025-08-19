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
    """A single Spark job to perform daily, monthly, and customer analytics."""
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
        spark.sql("CREATE DATABASE IF NOT EXISTS dws_db")
        spark.sql("USE dws_db")

        # --- 1. Daily Aggregation ---
        logging.info(f"Starting Daily Aggregation for date: {batch_date}")
        dwd_orders_df = spark.table("dwd_db.dwd_orders").filter(col("dt") == batch_date)
        dwd_orders_df.cache()

        if dwd_orders_df.rdd.isEmpty():
            logging.warning("No DWD data for today, skipping all aggregations.")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_EMPTY_DATA')
            return

        daily_summary = dwd_orders_df.groupBy(date_format(col("OrderDate"), "yyyy-MM-dd").alias("order_date")).agg(
            count("*").alias("total_orders"),
            sum("TotalAmount").alias("total_amount"),
            avg("TotalAmount").alias("avg_order_value"),
            sum("NetAmount").alias("total_net_amount"),
            count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders"),
            count(when(col("OrderStatus") == "Cancelled", 1)).alias("cancelled_orders"),
            count(when(col("IsDelayed") == True, 1)).alias("delayed_orders")
        ).withColumn("dt", lit(batch_date))
        
        daily_summary.write.mode("overwrite").partitionBy("dt").format("parquet") \
            .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_daily_summary") \
            .saveAsTable("dws_orders_daily_summary")
        logging.info("✅ Daily aggregation complete and loaded.")

        # --- 2. Monthly Aggregation ---
        current_month = datetime.strptime(batch_date, '%Y-%m-%d').strftime('%Y-%m')
        logging.info(f"Starting Monthly Aggregation for month: {current_month}")
        # Re-use the cached dwd_orders_df if it contains the whole month, otherwise read again.
        # For simplicity and correctness, we read the whole month's data.
        monthly_dwd_df = spark.table("dwd_db.dwd_orders").filter(date_format(col("OrderDate"), "yyyy-MM") == current_month)
        
        monthly_summary = monthly_dwd_df.groupBy(date_format(col("OrderDate"), "yyyy-MM").alias("year_month")).agg(
            count("*").alias("total_orders"),
            sum("TotalAmount").alias("total_amount"),
            avg("TotalAmount").alias("avg_order_value"),
            sum("NetAmount").alias("total_net_amount"),
            count(when(col("OrderStatus") == "Delivered", 1)).alias("completed_orders")
        ).withColumn("dt", lit(batch_date))
        
        monthly_summary.write.mode("overwrite").partitionBy("dt").format("parquet") \
            .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_monthly_summary") \
            .saveAsTable("dws_orders_monthly_summary")
        logging.info("✅ Monthly aggregation complete and loaded.")

        # --- 3. Customer Analytics ---
        start_date_30d = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        logging.info(f"Starting Customer Analytics for period: {start_date_30d} to {batch_date}")
        customer_dwd_df = spark.table("dwd_db.dwd_orders").filter(col("OrderDate").between(start_date_30d, batch_date))

        customer_analytics = customer_dwd_df.groupBy("CustomerID", "CustomerName", "CustomerType").agg(
            count("*").alias("total_orders"),
            sum("TotalAmount").alias("total_spent"),
            avg("TotalAmount").alias("avg_order_value"),
            max("OrderDate").alias("last_order_date")
        ).withColumn("days_since_last_order", datediff(lit(batch_date), col("last_order_date"))) \
         .withColumn("dt", lit(batch_date))
        
        customer_analytics.write.mode("overwrite").partitionBy("dt").format("parquet") \
            .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_customer_analytics") \
            .saveAsTable("dws_customer_analytics")
        logging.info("✅ Customer analytics complete and loaded.")

        dwd_orders_df.unpersist()

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
            "CREATE OR REPLACE VIEW dws_high_value_customers AS SELECT CustomerID, CustomerName, total_spent, total_orders, last_order_date FROM dws_customer_analytics ORDER BY total_spent DESC LIMIT 100"
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
