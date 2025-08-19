from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import os

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def run_dws_orderdetails_analytics_etl(**context):
    """A single, optimized Spark job for all OrderDetails DWS aggregations."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, lit, date_format, desc
    from datetime import datetime, timedelta

    spark = None
    try:
        spark = SparkSession.builder \
            .appName("DWS_OrderDetails_Analytics_ETL") \
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
        start_date_30d = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

        spark.sql("CREATE DATABASE IF NOT EXISTS dws_db")
        spark.sql("USE dws_db")

        # Read data for the last 30 days once and cache it
        logging.info(f"Reading data from dwd_db.dwd_orderdetails for period: {start_date_30d} to {batch_date}")
        dwd_df_30d = spark.table("dwd_db.dwd_orderdetails").filter(col("dt").between(start_date_30d, batch_date))
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
                count("*").alias("total_items"),
                sum("NetAmount").alias("total_amount"),
                sum("Quantity").alias("total_quantity"),
                avg("UnitPrice").alias("avg_unit_price"),
                count(when(col("IsHighValue") == True, 1)).alias("high_value_items")
            )
            daily_summary.withColumn("dt", lit(batch_date)) \
                .write.mode("overwrite").partitionBy("dt").format("parquet") \
                .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orderdetails_daily_summary") \
                .saveAsTable("dws_orderdetails_daily_summary")
            logging.info("✅ Daily aggregation complete and loaded.")

        # --- 2. Product Analytics ---
        logging.info("Starting Product Analytics for the last 30 days.")
        product_analytics = dwd_df_30d.groupBy("ProductID", "ProductName", "ProductCategory").agg(
            count("*").alias("total_orders"),
            sum("Quantity").alias("total_quantity_sold"),
            sum("NetAmount").alias("total_revenue"),
            avg("UnitPrice").alias("avg_unit_price"),
            count(when(col("IsHighValue") == True, 1)).alias("high_value_orders")
        ).withColumn("product_tier", when(col("total_revenue") >= 100000, "Tier 1").when(col("total_revenue") >= 50000, "Tier 2").otherwise("Tier 3"))
        product_analytics.withColumn("dt", lit(batch_date)) \
            .write.mode("overwrite").partitionBy("dt").format("parquet") \
            .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_product_analytics") \
            .saveAsTable("dws_product_analytics")
        logging.info("✅ Product analytics complete and loaded.")

        # --- 3. Warehouse Analytics ---
        logging.info("Starting Warehouse Analytics for the last 30 days.")
        warehouse_analytics = dwd_df_30d.groupBy("WarehouseID", "WarehouseName", "FactoryName").agg(
            count("*").alias("total_items_processed"),
            sum("Quantity").alias("total_quantity_handled"),
            sum("NetAmount").alias("total_value_handled"),
            avg("WarehouseEfficiency").alias("avg_efficiency_score")
        ).withColumn("warehouse_performance_grade", when(col("avg_efficiency_score") >= 95, "A").when(col("avg_efficiency_score") >= 90, "B").otherwise("C"))
        warehouse_analytics.withColumn("dt", lit(batch_date)) \
            .write.mode("overwrite").partitionBy("dt").format("parquet") \
            .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_warehouse_analytics") \
            .saveAsTable("dws_warehouse_analytics")
        logging.info("✅ Warehouse analytics complete and loaded.")

        dwd_df_30d.unpersist()
        context['task_instance'].xcom_push(key='status', value='SUCCESS')
        context['task_instance'].xcom_push(key='tables_created', value=['dws_orderdetails_daily_summary', 'dws_product_analytics', 'dws_warehouse_analytics'])

    except Exception as e:
        logging.error(f"DWS Orderdetails Analytics ETL failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()

def create_analytics_views(**context):
    """Creates DWS analysis views for OrderDetails."""
    if context['task_instance'].xcom_pull(task_ids='run_dws_orderdetails_analytics_etl', key='status') == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping view creation as no data was processed.")
        return

    from pyspark.sql import SparkSession
    spark = None
    try:
        spark = SparkSession.builder.appName("CreateDWSOrderDetailsViews").config("spark.sql.catalogImplementation","hive").config("spark.hadoop.hive.metastore.uris","thrift://hive-metastore:9083").enableHiveSupport().getOrCreate()
        spark.sql("USE dws_db")
        
        views_sql = [
            "CREATE OR REPLACE VIEW dws_product_performance AS SELECT ProductName, product_tier, total_revenue, total_quantity_sold FROM dws_product_analytics ORDER BY total_revenue DESC",
            "CREATE OR REPLACE VIEW dws_warehouse_performance AS SELECT WarehouseName, FactoryName, total_items_processed, total_value_handled, avg_efficiency_score, warehouse_performance_grade FROM dws_warehouse_analytics ORDER BY avg_efficiency_score DESC"
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
    'dws_orderdetails_analytics',
    default_args=default_args,
    schedule_interval='0 5 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['dws', 'analytics', 'orderdetails', 'refactored'],
    description='Refactored DWS OrderDetails analytics pipeline using a single Spark job.',
) as dag:

    start = DummyOperator(task_id='start')
    # A dependency check task would be here in a real scenario.

    etl_task = PythonOperator(
        task_id='run_dws_orderdetails_analytics_etl',
        python_callable=run_dws_orderdetails_analytics_etl
    )

    create_views_task = PythonOperator(
        task_id='create_analytics_views',
        python_callable=create_analytics_views
    )

    end = DummyOperator(task_id='end')

    start >> etl_task >> create_views_task >> end
