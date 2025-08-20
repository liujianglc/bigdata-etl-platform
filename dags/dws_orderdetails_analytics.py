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
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, lit, date_format, desc, countDistinct
    from pyspark.sql.types import DecimalType, DoubleType
    from config.data_types_config import AMOUNT, AVERAGE, RATE, DAYS, FREQUENCY
    import logging
    import os
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
        
        # Enable dynamic partition mode for better handling
        spark.sql("SET hive.exec.dynamic.partition = true")
        spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
        spark.sql("SET spark.sql.sources.partitionOverwriteMode = dynamic")
        
        # Drop existing tables to ensure schema updates are applied
        try:
            spark.sql("DROP TABLE IF EXISTS dws_product_analytics")
            spark.sql("DROP TABLE IF EXISTS dws_warehouse_analytics")
            spark.sql("DROP TABLE IF EXISTS dws_orderdetails_daily_summary")
            logging.info("✅ Dropped existing tables for schema refresh")
        except Exception as e:
            logging.warning(f"Could not drop existing tables: {e}")

        dwd_df_30d = spark.table("dwd_db.dwd_orderdetails").filter(col("dt").between(start_date_30d, batch_date))
        dwd_df_30d.cache()

        # Use limit(1).count() instead of rdd.isEmpty() for better performance
        if dwd_df_30d.limit(1).count() == 0:
            logging.warning("No DWD data for the last 30 days, skipping all aggregations.")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_EMPTY_DATA')
            return

        # 跟踪实际创建的表
        tables_created_list = []
        
        # --- 1. Daily Aggregation ---
        logging.info(f"Starting Daily Aggregation for date: {batch_date}")
        daily_df = dwd_df_30d.filter(col("dt") == batch_date)
        if daily_df.limit(1).count() > 0:
            daily_summary = daily_df.groupBy(date_format(col("OrderDate"), "yyyy-MM-dd").alias("order_date")).agg(
                count("*").alias("total_items"),
                sum("NetAmount").cast(AMOUNT).alias("total_amount"),
                sum("Quantity").cast(AMOUNT).alias("total_quantity"),
                avg("UnitPrice").cast(AVERAGE).alias("avg_unit_price"),
                avg("Discount").cast(AVERAGE).alias("avg_discount"),
                count(when(col("IsHighValue") == True, 1)).alias("high_value_items"),
                count(when(col("IsDiscounted") == True, 1)).alias("discounted_items"),
                count(when(col("OrderDetailStatus") == "Delivered", 1)).alias("delivered_items"),
                count(when(col("OrderDetailStatus") == "Cancelled", 1)).alias("cancelled_items"),
                count(when(col("PriceCategory") == "Premium", 1)).alias("premium_items"),
                max("UnitPrice").cast(AMOUNT).alias("max_unit_price"),
                min("UnitPrice").cast(AMOUNT).alias("min_unit_price")
            ).withColumn("avg_item_value", (col("total_amount") / col("total_items")).cast(AVERAGE)) \
             .withColumn("discount_rate", ((col("discounted_items") / col("total_items") * 100)).cast(RATE)) \
             .withColumn("high_value_rate", ((col("high_value_items") / col("total_items") * 100)).cast(RATE)) \
             .withColumn("delivery_rate", ((col("delivered_items") / col("total_items") * 100)).cast(RATE)) \
             .withColumn("cancellation_rate", ((col("cancelled_items") / col("total_items") * 100)).cast(RATE))
            
            daily_summary_with_dt = daily_summary.withColumn("dt", lit(batch_date))
            daily_summary_with_dt.write.mode("overwrite").partitionBy("dt").format("parquet").option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orderdetails_daily_summary").saveAsTable("dws_orderdetails_daily_summary")
            tables_created_list.append("dws_orderdetails_daily_summary")
            logging.info("✅ Daily aggregation complete and loaded.")
        else:
            logging.warning(f"No data found for date {batch_date}, skipping daily summary table creation.")

        # --- 2. Product Analytics ---
        logging.info("Starting Product Analytics for the last 30 days.")
        product_analytics = dwd_df_30d.groupBy(
            col("ProductID").alias("product_id"),
            col("ProductName").alias("product_name"),
            col("ProductCategory").alias("product_category"),
            col("ProductSpecification").alias("product_specification")
        ).agg(
            count("*").alias("total_orders"),
            sum("Quantity").cast(AMOUNT).alias("total_quantity_sold"),
            sum("NetAmount").cast(AMOUNT).alias("total_revenue"),
            avg("UnitPrice").cast(AVERAGE).alias("avg_unit_price"),
            avg("Discount").cast(AVERAGE).alias("avg_discount"),
            count(when(col("IsHighValue") == True, 1)).alias("high_value_orders"),
            count(when(col("IsDiscounted") == True, 1)).alias("discounted_orders"),
            count(when(col("OrderDetailStatus") == "Delivered", 1)).alias("delivered_orders"),
            count(when(col("OrderDetailStatus") == "Cancelled", 1)).alias("cancelled_orders")
        ).withColumn("avg_revenue_per_order", (col("total_revenue") / col("total_orders")).cast(AVERAGE)) \
         .withColumn("delivery_rate", (col("delivered_orders") / col("total_orders") * 100).cast(RATE)) \
         .withColumn("cancellation_rate", (col("cancelled_orders") / col("total_orders") * 100).cast(RATE)) \
         .withColumn("product_tier",
                    when(col("total_revenue") >= 100000, "Tier 1")
                    .when(col("total_revenue") >= 50000, "Tier 2")
                    .when(col("total_revenue") >= 10000, "Tier 3")
                    .otherwise("Tier 4")) \
         .withColumn("demand_level",
                    when(col("total_quantity_sold") >= 1000, "High")
                    .when(col("total_quantity_sold") >= 500, "Medium")
                    .when(col("total_quantity_sold") >= 100, "Low")
                    .otherwise("Very Low")) \
         .withColumn("product_performance_score",
                    ((col("delivery_rate") * 0.4) + 
                     ((100 - col("cancellation_rate")) * 0.3) + 
                     (when(col("total_revenue") >= 100000, 100)
                      .when(col("total_revenue") >= 50000, 80)
                      .when(col("total_revenue") >= 10000, 60)
                      .otherwise(40) * 0.3)).cast(AVERAGE))

        product_analytics_with_dt = product_analytics.withColumn("dt", lit(batch_date))
        product_analytics_with_dt.write.mode("overwrite").partitionBy("dt").format("parquet").option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_product_analytics").saveAsTable("dws_product_analytics")
        tables_created_list.append("dws_product_analytics")
        logging.info("✅ Product analytics complete and loaded.")

        # --- 3. Warehouse Analytics ---
        logging.info("Starting Warehouse Analytics for the last 30 days.")
        warehouse_analytics = dwd_df_30d.groupBy(
            col("WarehouseID").alias("warehouse_id"),
            col("WarehouseName").alias("warehouse_name"),
            col("WarehouseManager").alias("warehouse_manager"),
            col("FactoryName").alias("factory_name"),
            col("FactoryLocation").alias("factory_location")
        ).agg(
            count("*").alias("total_items_processed"),
            sum("Quantity").cast(AMOUNT).alias("total_quantity_handled"),
            sum("NetAmount").cast(AMOUNT).alias("total_value_handled"),
            countDistinct("OrderID").alias("unique_orders"),
            countDistinct("ProductID").alias("unique_products"),
            avg("UnitPrice").cast(AVERAGE).alias("avg_item_price"),
            count(when(col("OrderDetailStatus") == "Delivered", 1)).alias("delivered_items"),
            avg("WarehouseEfficiency").cast(AVERAGE).alias("avg_efficiency_score")
        ).withColumn("delivery_rate", ((col("delivered_items") / col("total_items_processed") * 100)).cast(RATE)) \
         .withColumn("avg_value_per_item", (col("total_value_handled") / col("total_items_processed")).cast(AVERAGE)) \
         .withColumn("warehouse_performance_grade",
                    when(col("delivery_rate") >= 95, "A")
                    .when(col("delivery_rate") >= 90, "B")
                    .when(col("delivery_rate") >= 80, "C")
                    .otherwise("D"))

        warehouse_analytics_with_dt = warehouse_analytics.withColumn("dt", lit(batch_date))
        warehouse_analytics_with_dt.write.mode("overwrite").partitionBy("dt").format("parquet").option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_warehouse_analytics").saveAsTable("dws_warehouse_analytics")
        tables_created_list.append("dws_warehouse_analytics")
        logging.info("✅ Warehouse analytics complete and loaded.")

        dwd_df_30d.unpersist()
        context['task_instance'].xcom_push(key='status', value='SUCCESS')
        context['task_instance'].xcom_push(key='tables_created', value=tables_created_list)
        logging.info(f"✅ ETL completed successfully. Tables created: {tables_created_list}")

    except Exception as e:
        logging.error(f"DWS OrderDetails Analytics ETL failed: {e}", exc_info=True)
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
        
        # 获取已创建的表列表
        tables_created = context['task_instance'].xcom_pull(task_ids='run_dws_orderdetails_analytics_etl', key='tables_created') or []
        logging.info(f"Tables reported as created: {tables_created}")
        
        # 验证表是否真正存在
        verified_tables = []
        for table_name in tables_created:
            try:
                spark.sql(f"DESCRIBE TABLE dws_db.{table_name}")
                verified_tables.append(table_name)
                logging.info(f"✅ Verified table exists: {table_name}")
            except Exception as e:
                logging.warning(f"❌ Table {table_name} not found or not accessible: {e}")
        
        tables_created = verified_tables
        logging.info(f"Final verified tables list: {tables_created}")
        
        if not tables_created:
            logging.error("No verified tables exist. Cannot create views.")
            return
        
        # 详细记录表依赖关系
        logging.info("Table dependencies for view creation:")
        logging.info(f"  - dws_orderdetails_weekly_trend depends on: dws_orderdetails_daily_summary")
        logging.info(f"  - dws_product_category_analysis depends on: dws_product_analytics")
        logging.info(f"  - dws_warehouse_performance depends on: dws_warehouse_analytics")
        
        # Drop existing views first to avoid conflicts
        drop_views = [
            "DROP VIEW IF EXISTS dws_product_performance",
            "DROP VIEW IF EXISTS dws_warehouse_performance",
            "DROP VIEW IF EXISTS dws_orderdetails_weekly_trend",
            "DROP VIEW IF EXISTS dws_orderdetails_daily_kpi",
            "DROP VIEW IF EXISTS dws_top_performing_products",
            "DROP VIEW IF EXISTS dws_product_category_analysis",
            "DROP VIEW IF EXISTS dws_warehouse_performance_ranking",
            "DROP VIEW IF EXISTS dws_factory_summary"
        ]
        
        for drop_sql in drop_views:
            try:
                spark.sql(drop_sql)
            except Exception as e:
                logging.warning(f"Could not drop view: {e}")
        
        # 基础视图 - 只有在相应表存在时才创建
        views_sql = []
        
        # 检查并创建基础视图
        if 'dws_product_analytics' in tables_created:
            views_sql.append("CREATE OR REPLACE VIEW dws_product_performance AS SELECT product_name, product_tier, total_revenue, total_quantity_sold, demand_level, product_performance_score FROM dws_product_analytics ORDER BY product_performance_score DESC")
        
        if 'dws_warehouse_analytics' in tables_created:
            views_sql.append("CREATE OR REPLACE VIEW dws_warehouse_performance AS SELECT warehouse_name, factory_name, total_items_processed, total_value_handled, delivery_rate, avg_efficiency_score, warehouse_performance_grade FROM dws_warehouse_analytics ORDER BY avg_efficiency_score DESC")
        
        # 如果日汇总表存在，创建相关视图
        if 'dws_orderdetails_daily_summary' in tables_created:
            views_sql.extend([
                # 最近7天明细趋势视图
                """
                CREATE OR REPLACE VIEW dws_orderdetails_weekly_trend AS
                SELECT 
                    order_date,
                    total_items,
                    total_amount,
                    total_quantity,
                    delivery_rate,
                    discount_rate,
                    LAG(total_items, 1) OVER (ORDER BY order_date) as prev_day_items,
                    LAG(total_amount, 1) OVER (ORDER BY order_date) as prev_day_amount
                FROM dws_orderdetails_daily_summary
                WHERE order_date >= date_sub(current_date(), 7)
                ORDER BY order_date DESC
                """,
                
                # 日汇总KPI视图
                """
                CREATE OR REPLACE VIEW dws_orderdetails_daily_kpi AS
                SELECT 
                    order_date,
                    total_items,
                    total_amount,
                    avg_item_value,
                    delivery_rate,
                    cancellation_rate,
                    discount_rate,
                    high_value_rate,
                    CASE 
                        WHEN delivery_rate >= 95 THEN 'Excellent'
                        WHEN delivery_rate >= 90 THEN 'Good'
                        WHEN delivery_rate >= 80 THEN 'Fair'
                        ELSE 'Poor'
                    END as performance_grade
                FROM dws_orderdetails_daily_summary
                """
            ])
        
        # 如果产品分析表存在，创建相关视图
        if 'dws_product_analytics' in tables_created:
            views_sql.extend([
                # 产品绩效排行视图
                """
                CREATE OR REPLACE VIEW dws_top_performing_products AS
                SELECT 
                    product_id,
                    product_name,
                    product_category,
                    product_tier,
                    demand_level,
                    total_revenue,
                    total_orders,
                    avg_revenue_per_order,
                    delivery_rate,
                    product_performance_score
                FROM dws_product_analytics
                WHERE product_tier IN ('Tier 1', 'Tier 2')
                ORDER BY product_performance_score DESC
                """,
                
                # 产品类别分析视图
                """
                CREATE OR REPLACE VIEW dws_product_category_analysis AS
                SELECT 
                    product_category,
                    COUNT(*) as product_count,
                    SUM(total_revenue) as category_revenue,
                    AVG(total_revenue) as avg_product_revenue,
                    AVG(delivery_rate) as avg_delivery_rate,
                    AVG(product_performance_score) as avg_performance_score
                FROM dws_product_analytics
                GROUP BY product_category
                ORDER BY category_revenue DESC
                """
            ])
        
        # 如果仓库分析表存在，创建相关视图
        if 'dws_warehouse_analytics' in tables_created:
            views_sql.extend([
                # 仓库绩效排行视图
                """
                CREATE OR REPLACE VIEW dws_warehouse_performance_ranking AS
                SELECT 
                    warehouse_id,
                    warehouse_name,
                    warehouse_manager,
                    factory_name,
                    warehouse_performance_grade,
                    delivery_rate,
                    total_items_processed,
                    total_value_handled,
                    avg_efficiency_score
                FROM dws_warehouse_analytics
                ORDER BY delivery_rate DESC, total_value_handled DESC
                """,
                
                # 工厂级别汇总视图
                """
                CREATE OR REPLACE VIEW dws_factory_summary AS
                SELECT 
                    factory_name,
                    factory_location,
                    COUNT(*) as warehouse_count,
                    SUM(total_items_processed) as total_items,
                    SUM(total_value_handled) as total_value,
                    AVG(delivery_rate) as avg_delivery_rate,
                    AVG(avg_efficiency_score) as avg_efficiency
                FROM dws_warehouse_analytics
                GROUP BY factory_name, factory_location
                ORDER BY total_value DESC
                """
            ])
        
        # 创建视图并刷新
        successful_views = 0
        failed_views = 0
        
        for i, view_sql in enumerate(views_sql):
            try:
                # 提取视图名称用于更好的日志记录
                import re
                view_match = re.search(r'(?:CREATE|CREATE OR REPLACE)\s+VIEW\s+(\w+)', view_sql, re.IGNORECASE)
                view_name = view_match.group(1) if view_match else f"view_{i+1}"
                
                logging.info(f"Creating view: {view_name}")
                spark.sql(view_sql)
                successful_views += 1
                logging.info(f"✅ Successfully created view {view_name} ({i+1}/{len(views_sql)})")
                
                # 刷新视图
                try:
                    spark.sql(f"REFRESH TABLE {view_name}")
                    logging.info(f"✅ Successfully refreshed view {view_name}")
                except Exception as refresh_e:
                    logging.warning(f"Could not refresh view {view_name}: {refresh_e}")
                    
            except Exception as e:
                failed_views += 1
                view_name = view_match.group(1) if 'view_match' in locals() and view_match else f"view_{i+1}"
                logging.error(f"❌ Failed to create view {view_name}: {e}")
                logging.error(f"SQL: {view_sql[:200]}...")
                # 继续处理其他视图，不要因为一个视图失败就停止所有操作
                continue
                
        logging.info(f"✅ View creation summary: {successful_views} successful, {failed_views} failed out of {len(views_sql)} total views.")
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
