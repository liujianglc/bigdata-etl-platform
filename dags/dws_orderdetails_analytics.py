from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def aggregate_daily_orderdetails(**context):
    """聚合每日订单明细数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, coalesce, lit, date_format
    import logging
    import os
    import tempfile
    from datetime import datetime, timedelta
    
    spark = None
    try:
        # 创建Spark会话 - 优化内存配置
        spark = SparkSession.builder \
            .appName("DWS Daily OrderDetails Aggregation") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.cores", "1") \
            .config("spark.network.timeout", "300s") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.local.dir", "/tmp/spark") \
            .config("spark.worker.dir", "/tmp/spark-worker") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.host", "localhost") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "1") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200") \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=200") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logging.info("✅ Spark会话创建成功")
        
        # 检查DWD OrderDetails表是否存在
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            logging.info(f"可用数据库: {db_names}")
            
            target_db = 'dwd_db' if 'dwd_db' in db_names else 'default'
            logging.info(f"使用数据库: {target_db}")
            
            spark.sql(f"USE {target_db}")
            
            tables = spark.sql("SHOW TABLES").collect()
            table_names = [table[1] for table in tables]
            logging.info(f"{target_db}中的表: {table_names}")
            
            target_table = 'dwd_orderdetails' if 'dwd_orderdetails' in table_names else None
            
            if target_table is None:
                logging.error(f"❌ 在 {target_db} 数据库中未找到 dwd_orderdetails 表")
                raise Exception(f"DWD OrderDetails表不存在，请先运行 dwd_orderdetails_pipeline")
            
            logging.info(f"✅ 使用 {target_db}.{target_table} 表")
            full_table_name = f"{target_db}.{target_table}"
        except Exception as e:
            logging.error(f"❌ DWD OrderDetails表不存在: {e}")
            raise Exception("DWD OrderDetails表不存在，请先运行 dwd_orderdetails_pipeline")
        
        batch_date = context['ds']
        logging.info(f"处理日期: {batch_date}")
        
        # 读取DWD OrderDetails数据
        orderdetails_df = spark.sql(f"""
            SELECT *
            FROM {full_table_name}
            WHERE dt = '{batch_date}'
        """)
        
        record_count = orderdetails_df.count()
        logging.info(f"读取到 {record_count} 条DWD订单明细记录")
        
        if record_count == 0:
            logging.warning("没有找到当天的DWD数据")
            daily_summary = spark.createDataFrame([], schema="order_date string, total_items bigint, total_amount double")
        else:
            # 按日期聚合订单明细数据
            daily_summary = orderdetails_df.groupBy(
                date_format(col("OrderDate"), "yyyy-MM-dd").alias("order_date")
            ).agg(
                count("*").alias("total_items"),
                sum("NetAmount").alias("total_amount"),
                sum("Quantity").alias("total_quantity"),
                avg("UnitPrice").alias("avg_unit_price"),
                avg("Discount").alias("avg_discount"),
                count(when(col("IsHighValue") == True, 1)).alias("high_value_items"),
                count(when(col("IsDiscounted") == True, 1)).alias("discounted_items"),
                count(when(col("OrderDetailStatus") == "Delivered", 1)).alias("delivered_items"),
                count(when(col("OrderDetailStatus") == "Cancelled", 1)).alias("cancelled_items"),
                count(when(col("PriceCategory") == "Premium", 1)).alias("premium_items"),
                count(when(col("QuantityCategory") == "Bulk", 1)).alias("bulk_items"),
                max("UnitPrice").alias("max_unit_price"),
                min("UnitPrice").alias("min_unit_price"),
                count(when(col("DataQualityLevel") == "Excellent", 1)).alias("excellent_quality_items"),
                count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_items")
            ).withColumn("avg_item_value", 
                        col("total_amount") / col("total_items")
            ).withColumn("discount_rate",
                        (col("discounted_items") / col("total_items") * 100)
            ).withColumn("high_value_rate",
                        (col("high_value_items") / col("total_items") * 100)
            ).withColumn("delivery_rate",
                        (col("delivered_items") / col("total_items") * 100)
            ).withColumn("cancellation_rate",
                        (col("cancelled_items") / col("total_items") * 100)
            ).withColumn("data_quality_score",
                        ((col("excellent_quality_items") * 4 + 
                          (col("total_items") - col("excellent_quality_items") - col("poor_quality_items")) * 2) / 
                         col("total_items"))
            ).withColumn("etl_created_date", lit(datetime.now())) \
             .withColumn("etl_batch_id", lit(context['ds_nodash'])) \
             .withColumn("dt", lit(batch_date))
        
        # 缓存结果
        daily_summary.cache()
        summary_count = daily_summary.count()
        logging.info(f"生成 {summary_count} 条日汇总记录")
        
        # 显示汇总统计
        if summary_count > 0:
            summary_stats = daily_summary.collect()
            for row in summary_stats:
                logging.info(f"日期 {row['order_date']}: 明细 {row['total_items']} 条, "
                           f"金额 {row['total_amount']:.2f}, 交付率 {row['delivery_rate']:.1f}%")
        
        # 保存到临时文件
        temp_dir = tempfile.mkdtemp(prefix='daily_orderdetails_')
        temp_file = os.path.join(temp_dir, f'daily_orderdetails_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
        pandas_df = daily_summary.toPandas()
        daily_summary.unpersist()
        
        pandas_df.to_parquet(temp_file, index=False)
        logging.info(f"日汇总数据已保存到: {temp_file}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='daily_summary_file', value=temp_file)
        context['task_instance'].xcom_push(key='daily_temp_dir', value=temp_dir)
        context['task_instance'].xcom_push(key='daily_record_count', value=summary_count)
        
        return temp_file
        
    except Exception as e:
        logging.error(f"日汇总聚合失败: {e}")
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

def aggregate_product_analytics(**context):
    """聚合产品分析数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, coalesce, lit, desc
    import logging
    import os
    import tempfile
    from datetime import datetime, timedelta
    
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("DWS Product Analytics") \
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
        
        # 检查DWD OrderDetails表是否存在
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            
            target_db = 'dwd_db' if 'dwd_db' in db_names else 'default'
            spark.sql(f"USE {target_db}")
            
            tables = spark.sql("SHOW TABLES").collect()
            table_names = [table[1] for table in tables]
            
            target_table = 'dwd_orderdetails' if 'dwd_orderdetails' in table_names else None
            
            if target_table is None:
                raise Exception(f"DWD OrderDetails表不存在，请先运行 dwd_orderdetails_pipeline")
            
            full_table_name = f"{target_db}.{target_table}"
        except Exception as e:
            logging.error(f"❌ DWD OrderDetails表不存在: {e}")
            raise Exception("DWD OrderDetails表不存在，请先运行 dwd_orderdetails_pipeline")
        
        batch_date = context['ds']
        
        # 计算最近30天的产品分析数据
        start_date = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logging.info(f"分析时间范围: {start_date} 到 {batch_date}")
        
        # 读取最近30天的订单明细数据
        orderdetails_df = spark.sql(f"""
            SELECT *
            FROM {full_table_name}
            WHERE OrderDate >= '{start_date}' AND OrderDate <= '{batch_date}'
        """)
        
        record_count = orderdetails_df.count()
        logging.info(f"读取到最近30天 {record_count} 条订单明细记录")
        
        if record_count == 0:
            logging.warning("没有找到最近30天的订单明细数据")
            product_analytics = spark.createDataFrame([], schema="product_id string, product_name string")
        else:
            # 按产品聚合分析
            product_analytics = orderdetails_df.groupBy(
                col("ProductID").alias("product_id"),
                col("ProductName").alias("product_name"),
                col("ProductCategory").alias("product_category"),
                col("ProductSpecification").alias("product_specification")
            ).agg(
                count("*").alias("total_orders"),
                sum("Quantity").alias("total_quantity_sold"),
                sum("NetAmount").alias("total_revenue"),
                avg("UnitPrice").alias("avg_unit_price"),
                avg("Discount").alias("avg_discount"),
                max("UnitPrice").alias("max_unit_price"),
                min("UnitPrice").alias("min_unit_price"),
                count(when(col("IsHighValue") == True, 1)).alias("high_value_orders"),
                count(when(col("IsDiscounted") == True, 1)).alias("discounted_orders"),
                count(when(col("OrderDetailStatus") == "Delivered", 1)).alias("delivered_orders"),
                count(when(col("OrderDetailStatus") == "Cancelled", 1)).alias("cancelled_orders"),
                count(when(col("QuantityCategory") == "Bulk", 1)).alias("bulk_orders"),
                count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_orders"),
                avg("ProductValueDensity").alias("avg_value_density")
            ).withColumn("avg_order_quantity",
                        col("total_quantity_sold") / col("total_orders")
            ).withColumn("avg_revenue_per_order",
                        col("total_revenue") / col("total_orders")
            ).withColumn("delivery_rate",
                        (col("delivered_orders") / col("total_orders") * 100)
            ).withColumn("cancellation_rate",
                        (col("cancelled_orders") / col("total_orders") * 100)
            ).withColumn("discount_rate",
                        (col("discounted_orders") / col("total_orders") * 100)
            ).withColumn("bulk_order_rate",
                        (col("bulk_orders") / col("total_orders") * 100)
            ).withColumn("product_performance_score",
                        (col("delivery_rate") * 0.4 + 
                         (100 - col("cancellation_rate")) * 0.3 + 
                         col("avg_revenue_per_order") / 100 * 0.3)
            ).withColumn("product_tier",
                        when(col("total_revenue") >= 100000, "Tier 1")
                        .when(col("total_revenue") >= 50000, "Tier 2")
                        .when(col("total_revenue") >= 10000, "Tier 3")
                        .otherwise("Tier 4")
            ).withColumn("demand_level",
                        when(col("total_orders") >= 100, "High Demand")
                        .when(col("total_orders") >= 50, "Medium Demand")
                        .when(col("total_orders") >= 10, "Low Demand")
                        .otherwise("Very Low Demand")
            ).withColumn("etl_created_date", lit(datetime.now())) \
             .withColumn("etl_batch_id", lit(context['ds_nodash'])) \
             .withColumn("analysis_period_start", lit(start_date)) \
             .withColumn("analysis_period_end", lit(batch_date)) \
             .withColumn("dt", lit(batch_date))
        
        # 缓存并统计
        product_analytics.cache()
        analytics_count = product_analytics.count()
        logging.info(f"生成 {analytics_count} 条产品分析记录")
        
        # 显示产品分层统计
        if analytics_count > 0:
            tier_stats = product_analytics.groupBy("product_tier").count().collect()
            logging.info("产品分层分布:")
            for row in tier_stats:
                logging.info(f"  {row['product_tier']}: {row['count']} 个产品")
            
            demand_stats = product_analytics.groupBy("demand_level").count().collect()
            logging.info("需求水平分布:")
            for row in demand_stats:
                logging.info(f"  {row['demand_level']}: {row['count']} 个产品")
        
        # 保存到临时文件
        temp_dir = tempfile.mkdtemp(prefix='product_analytics_')
        temp_file = os.path.join(temp_dir, f'product_analytics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
        pandas_df = product_analytics.toPandas()
        product_analytics.unpersist()
        
        pandas_df.to_parquet(temp_file, index=False)
        logging.info(f"产品分析数据已保存到: {temp_file}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='product_analytics_file', value=temp_file)
        context['task_instance'].xcom_push(key='product_temp_dir', value=temp_dir)
        context['task_instance'].xcom_push(key='product_record_count', value=analytics_count)
        
        return temp_file
        
    except Exception as e:
        logging.error(f"产品分析聚合失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def aggregate_warehouse_analytics(**context):
    """聚合仓库分析数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, coalesce, lit, desc
    import logging
    import os
    import tempfile
    from datetime import datetime, timedelta
    
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("DWS Warehouse Analytics") \
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
        
        # 检查DWD OrderDetails表是否存在
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            
            target_db = 'dwd_db' if 'dwd_db' in db_names else 'default'
            spark.sql(f"USE {target_db}")
            
            tables = spark.sql("SHOW TABLES").collect()
            table_names = [table[1] for table in tables]
            
            target_table = 'dwd_orderdetails' if 'dwd_orderdetails' in table_names else None
            
            if target_table is None:
                raise Exception(f"DWD OrderDetails表不存在，请先运行 dwd_orderdetails_pipeline")
            
            full_table_name = f"{target_db}.{target_table}"
        except Exception as e:
            logging.error(f"❌ DWD OrderDetails表不存在: {e}")
            raise Exception("DWD OrderDetails表不存在，请先运行 dwd_orderdetails_pipeline")
        
        batch_date = context['ds']
        
        # 计算最近30天的仓库分析数据
        start_date = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logging.info(f"分析时间范围: {start_date} 到 {batch_date}")
        
        # 读取最近30天的订单明细数据
        orderdetails_df = spark.sql(f"""
            SELECT *
            FROM {full_table_name}
            WHERE OrderDate >= '{start_date}' AND OrderDate <= '{batch_date}'
        """)
        
        record_count = orderdetails_df.count()
        logging.info(f"读取到最近30天 {record_count} 条订单明细记录")
        
        if record_count == 0:
            logging.warning("没有找到最近30天的订单明细数据")
            warehouse_analytics = spark.createDataFrame([], schema="warehouse_id string, warehouse_name string")
        else:
            # 按仓库聚合分析
            warehouse_analytics = orderdetails_df.groupBy(
                col("WarehouseID").alias("warehouse_id"),
                col("WarehouseName").alias("warehouse_name"),
                col("WarehouseManager").alias("warehouse_manager"),
                col("FactoryName").alias("factory_name"),
                col("FactoryLocation").alias("factory_location")
            ).agg(
                count("*").alias("total_items_processed"),
                sum("Quantity").alias("total_quantity_handled"),
                sum("NetAmount").alias("total_value_handled"),
                count("OrderID").alias("unique_orders"),
                count("ProductID").alias("unique_products"),
                avg("UnitPrice").alias("avg_item_price"),
                count(when(col("OrderDetailStatus") == "Delivered", 1)).alias("delivered_items"),
                count(when(col("OrderDetailStatus") == "Cancelled", 1)).alias("cancelled_items"),
                count(when(col("OrderDetailStatus") == "Pending", 1)).alias("pending_items"),
                count(when(col("IsHighValue") == True, 1)).alias("high_value_items"),
                count(when(col("QuantityCategory") == "Bulk", 1)).alias("bulk_items"),
                count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_items"),
                avg("WarehouseEfficiency").alias("avg_efficiency_score")
            ).withColumn("delivery_rate",
                        (col("delivered_items") / col("total_items_processed") * 100)
            ).withColumn("cancellation_rate",
                        (col("cancelled_items") / col("total_items_processed") * 100)
            ).withColumn("pending_rate",
                        (col("pending_items") / col("total_items_processed") * 100)
            ).withColumn("avg_value_per_item",
                        col("total_value_handled") / col("total_items_processed")
            ).withColumn("avg_quantity_per_item",
                        col("total_quantity_handled") / col("total_items_processed")
            ).withColumn("warehouse_performance_grade",
                        when(col("delivery_rate") >= 95, "A")
                        .when(col("delivery_rate") >= 90, "B")
                        .when(col("delivery_rate") >= 80, "C")
                        .otherwise("D")
            ).withColumn("warehouse_capacity_utilization",
                        when(col("total_items_processed") >= 1000, "High")
                        .when(col("total_items_processed") >= 500, "Medium")
                        .otherwise("Low")
            ).withColumn("etl_created_date", lit(datetime.now())) \
             .withColumn("etl_batch_id", lit(context['ds_nodash'])) \
             .withColumn("analysis_period_start", lit(start_date)) \
             .withColumn("analysis_period_end", lit(batch_date)) \
             .withColumn("dt", lit(batch_date))
        
        # 缓存并统计
        warehouse_analytics.cache()
        analytics_count = warehouse_analytics.count()
        logging.info(f"生成 {analytics_count} 条仓库分析记录")
        
        # 显示仓库绩效统计
        if analytics_count > 0:
            grade_stats = warehouse_analytics.groupBy("warehouse_performance_grade").count().collect()
            logging.info("仓库绩效等级分布:")
            for row in grade_stats:
                logging.info(f"  等级 {row['warehouse_performance_grade']}: {row['count']} 个仓库")
            
            utilization_stats = warehouse_analytics.groupBy("warehouse_capacity_utilization").count().collect()
            logging.info("仓库利用率分布:")
            for row in utilization_stats:
                logging.info(f"  {row['warehouse_capacity_utilization']}: {row['count']} 个仓库")
        
        # 保存到临时文件
        temp_dir = tempfile.mkdtemp(prefix='warehouse_analytics_')
        temp_file = os.path.join(temp_dir, f'warehouse_analytics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
        pandas_df = warehouse_analytics.toPandas()
        warehouse_analytics.unpersist()
        
        pandas_df.to_parquet(temp_file, index=False)
        logging.info(f"仓库分析数据已保存到: {temp_file}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='warehouse_analytics_file', value=temp_file)
        context['task_instance'].xcom_push(key='warehouse_temp_dir', value=temp_dir)
        context['task_instance'].xcom_push(key='warehouse_record_count', value=analytics_count)
        
        return temp_file
        
    except Exception as e:
        logging.error(f"仓库分析聚合失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def load_orderdetails_analytics_to_hdfs(**context):
    """将所有OrderDetails分析结果加载到HDFS"""
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
        daily_file = context['task_instance'].xcom_pull(task_ids='aggregate_daily_orderdetails', key='daily_summary_file')
        product_file = context['task_instance'].xcom_pull(task_ids='aggregate_product_analytics', key='product_analytics_file')
        warehouse_file = context['task_instance'].xcom_pull(task_ids='aggregate_warehouse_analytics', key='warehouse_analytics_file')
        
        # 获取临时目录用于清理
        daily_temp_dir = context['task_instance'].xcom_pull(task_ids='aggregate_daily_orderdetails', key='daily_temp_dir')
        product_temp_dir = context['task_instance'].xcom_pull(task_ids='aggregate_product_analytics', key='product_temp_dir')
        warehouse_temp_dir = context['task_instance'].xcom_pull(task_ids='aggregate_warehouse_analytics', key='warehouse_temp_dir')
        
        # 创建Spark会话 - 优化内存配置
        spark = SparkSession.builder \
            .appName("DWS OrderDetails Analytics Load to HDFS") \
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
                table_location = "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orderdetails_daily_summary"
                
                # 检查表是否存在
                try:
                    spark.sql("DESCRIBE dws_orderdetails_daily_summary")
                    # 删除当天分区
                    spark.sql(f"ALTER TABLE dws_orderdetails_daily_summary DROP IF EXISTS PARTITION (dt='{batch_date}')")
                    logging.info("删除已存在的日汇总分区")
                except Exception:
                    logging.info("日汇总表不存在，将创建新表")
                
                spark_daily_df.coalesce(1).write.mode("append").partitionBy("dt") \
                    .option("path", table_location).saveAsTable("dws_orderdetails_daily_summary")
                
                tables_created.append("dws_orderdetails_daily_summary")
                logging.info(f"✅ 日汇总数据已加载: {len(daily_df)} 条记录")
        
        # 2. 加载产品分析数据
        if product_file and os.path.exists(product_file):
            product_df = pd.read_parquet(product_file)
            if len(product_df) > 0:
                spark_product_df = spark.createDataFrame(product_df)
                
                table_location = "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_product_analytics"
                
                try:
                    spark.sql("DESCRIBE dws_product_analytics")
                    spark.sql(f"ALTER TABLE dws_product_analytics DROP IF EXISTS PARTITION (dt='{batch_date}')")
                    logging.info("删除已存在的产品分析分区")
                except Exception:
                    logging.info("产品分析表不存在，将创建新表")
                
                spark_product_df.coalesce(1).write.mode("append").partitionBy("dt") \
                    .option("path", table_location).saveAsTable("dws_product_analytics")
                
                tables_created.append("dws_product_analytics")
                logging.info(f"✅ 产品分析数据已加载: {len(product_df)} 条记录")
        
        # 3. 加载仓库分析数据
        if warehouse_file and os.path.exists(warehouse_file):
            warehouse_df = pd.read_parquet(warehouse_file)
            if len(warehouse_df) > 0:
                spark_warehouse_df = spark.createDataFrame(warehouse_df)
                
                table_location = "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_warehouse_analytics"
                
                try:
                    spark.sql("DESCRIBE dws_warehouse_analytics")
                    spark.sql(f"ALTER TABLE dws_warehouse_analytics DROP IF EXISTS PARTITION (dt='{batch_date}')")
                    logging.info("删除已存在的仓库分析分区")
                except Exception:
                    logging.info("仓库分析表不存在，将创建新表")
                
                spark_warehouse_df.coalesce(1).write.mode("append").partitionBy("dt") \
                    .option("path", table_location).saveAsTable("dws_warehouse_analytics")
                
                tables_created.append("dws_warehouse_analytics")
                logging.info(f"✅ 仓库分析数据已加载: {len(warehouse_df)} 条记录")
        
        # 刷新所有表的元数据
        for table in tables_created:
            spark.sql(f"REFRESH TABLE {table}")
            logging.info(f"已刷新表元数据: {table}")
        
        # 清理临时目录
        import shutil
        for temp_dir in [daily_temp_dir, product_temp_dir, warehouse_temp_dir]:
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
        
        logging.info(f"✅ DWS OrderDetails数据加载完成: {load_summary}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='load_summary', value=load_summary)
        context['task_instance'].xcom_push(key='tables_created', value=tables_created)
        
        return load_summary
        
    except Exception as e:
        logging.error(f"DWS OrderDetails数据加载失败: {e}")
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

def create_orderdetails_analytics_views(**context):
    """创建DWS OrderDetails分析视图"""
    from pyspark.sql import SparkSession
    import logging
    
    spark = None
    try:
        # 获取创建的表信息
        tables_created = context['task_instance'].xcom_pull(task_ids='load_orderdetails_analytics_to_hdfs', key='tables_created')
        
        # 创建Spark会话 - 最小内存配置
        spark = SparkSession.builder \
            .appName("Create DWS OrderDetails Analytics Views") \
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
        
        spark.sql("USE dws_db")
        
        # 创建分析视图
        views_sql = []
        
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
        
        # 执行视图创建
        views_created = 0
        for i, view_sql in enumerate(views_sql, 1):
            try:
                spark.sql(view_sql)
                views_created += 1
                logging.info(f"✅ 视图 {i} 创建成功")
            except Exception as e:
                logging.warning(f"⚠️ 视图 {i} 创建失败: {e}")
        
        # 验证视图创建结果
        try:
            views = spark.sql("SHOW VIEWS").collect()
            dws_views = [row[1] for row in views if 'orderdetails' in row[1] or 'product' in row[1] or 'warehouse' in row[1]]
            logging.info(f"创建的DWS OrderDetails相关视图: {dws_views}")
        except Exception as e:
            logging.warning(f"无法列出视图: {e}")
        
        logging.info(f"✅ DWS OrderDetails分析视图创建完成，成功创建 {views_created} 个视图")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='views_created_count', value=views_created)
        context['task_instance'].xcom_push(key='available_tables', value=tables_created)
        
        return views_created
        
    except Exception as e:
        logging.error(f"DWS OrderDetails视图创建失败: {e}")
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

def validate_orderdetails_analytics_data(**context):
    """验证DWS OrderDetails分析数据质量"""
    import logging
    
    try:
        # 获取相关信息
        load_summary = context['task_instance'].xcom_pull(task_ids='load_orderdetails_analytics_to_hdfs', key='load_summary')
        views_created = context['task_instance'].xcom_pull(task_ids='create_orderdetails_analytics_views', key='views_created_count')
        
        # 获取各阶段的记录数
        daily_count = context['task_instance'].xcom_pull(task_ids='aggregate_daily_orderdetails', key='daily_record_count')
        product_count = context['task_instance'].xcom_pull(task_ids='aggregate_product_analytics', key='product_record_count')
        warehouse_count = context['task_instance'].xcom_pull(task_ids='aggregate_warehouse_analytics', key='warehouse_record_count')
        
        # 数据质量验证结果
        validation_results = {
            'tables_created': load_summary['total_tables'],
            'views_created': views_created,
            'data_summary': {
                'daily_records': daily_count or 0,
                'product_records': product_count or 0,
                'warehouse_records': warehouse_count or 0
            },
            'batch_date': load_summary['batch_date']
        }
        
        # 验证结果评估
        quality_score = 100
        issues = []
        
        # 检查表创建情况
        expected_tables = 3  # 日汇总、产品分析、仓库分析
        if load_summary['total_tables'] < expected_tables:
            quality_score -= 20
            issues.append(f"预期创建 {expected_tables} 个表，实际创建 {load_summary['total_tables']} 个")
        
        # 检查视图创建情况
        if views_created == 0:
            quality_score -= 15
            issues.append("没有成功创建任何分析视图")
        elif views_created < 5:
            quality_score -= 5
            issues.append(f"创建的视图数量较少: {views_created}")
        
        # 检查数据记录数
        total_records = (daily_count or 0) + (product_count or 0) + (warehouse_count or 0)
        if total_records == 0:
            quality_score -= 30
            issues.append("所有分析表都没有数据")
        elif total_records < 10:
            quality_score -= 10
            issues.append(f"分析数据记录数较少: {total_records}")
        
        validation_results['overall_quality_score'] = quality_score
        validation_results['issues'] = issues
        validation_results['validation_status'] = 'PASSED' if quality_score >= 80 else 'WARNING' if quality_score >= 60 else 'FAILED'
        
        # 记录验证结果
        logging.info("=== DWS OrderDetails Analytics 数据质量验证结果 ===")
        logging.info(f"总体质量分数: {quality_score}/100")
        logging.info(f"验证状态: {validation_results['validation_status']}")
        logging.info(f"创建表数: {load_summary['total_tables']}")
        logging.info(f"创建视图数: {views_created}")
        logging.info(f"数据记录统计:")
        logging.info(f"  - 日汇总: {daily_count or 0} 条")
        logging.info(f"  - 产品分析: {product_count or 0} 条")
        logging.info(f"  - 仓库分析: {warehouse_count or 0} 条")
        
        if issues:
            logging.warning("发现的问题:")
            for issue in issues:
                logging.warning(f"  - {issue}")
        else:
            logging.info("✅ 所有验证检查通过")
        
        # 保存验证结果
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        
        return validation_results
        
    except Exception as e:
        logging.error(f"数据验证失败: {e}")
        raise

# 创建DAG
with DAG(
    'dws_orderdetails_analytics',
    default_args=default_args,
    schedule_interval='0 4 * * *',  # 每日凌晨4点执行（在dwd_orderdetails_pipeline的3点之后）
    catchup=False,
    max_active_runs=1,
    tags=['dws', 'analytics', 'orderdetails', 'aggregation', 'downstream'],
    description='DWS层OrderDetails分析表ETL流程 - 基于DWD层数据进行多维度聚合分析',
) as dag:

    # 依赖检查任务
    def check_dwd_dependencies(**context):
        """检查DWD层依赖是否就绪"""
        from pyspark.sql import SparkSession
        import logging
        
        spark = None
        try:
            spark = SparkSession.builder \
                .appName("Check DWD Dependencies") \
                .master("local[1]") \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "1g") \
                .config("spark.ui.enabled", "false") \
                .enableHiveSupport() \
                .getOrCreate()
            
            # 检查DWD OrderDetails表是否存在且有当天数据
            batch_date = context['ds']
            
            try:
                databases = spark.sql("SHOW DATABASES").collect()
                db_names = [db[0] for db in databases]
                logging.info(f"可用数据库: {db_names}")
                
                target_db = 'dwd_db' if 'dwd_db' in db_names else 'default'
                logging.info(f"使用数据库: {target_db}")
                
                spark.sql(f"USE {target_db}")
                
                tables = spark.sql("SHOW TABLES").collect()
                table_names = [table[1] for table in tables]
                logging.info(f"{target_db}中的表: {table_names}")
                
                target_table = 'dwd_orderdetails' if 'dwd_orderdetails' in table_names else None
                
                if target_table is None:
                    logging.error(f"❌ 在 {target_db} 数据库中未找到 dwd_orderdetails 表")
                    raise Exception(f"在 {target_db} 数据库中未找到 dwd_orderdetails 表，请先运行 dwd_orderdetails_pipeline 创建DWD层数据")
                
                logging.info(f"使用表: {target_table}")
                
                # 检查表结构
                spark.sql(f"DESCRIBE {target_table}")
                logging.info(f"✅ {target_table}表结构检查通过")
                
                # 检查是否有当天分区数据
                count = spark.sql(f"SELECT COUNT(*) FROM {target_table} WHERE dt = '{batch_date}'").collect()[0][0]
                
                if count > 0:
                    logging.info(f"✅ {target_table}表存在且有数据: {count} 条记录")
                else:
                    logging.warning(f"⚠️ {target_table}表存在但当天无数据")
                    # 检查最近的数据
                    try:
                        recent_data = spark.sql(f"SELECT dt, COUNT(*) as cnt FROM {target_table} GROUP BY dt ORDER BY dt DESC LIMIT 3").collect()
                        logging.info("最近的数据分区:")
                        for row in recent_data:
                            logging.info(f"  {row['dt']}: {row['cnt']} 条记录")
                    except Exception as e:
                        logging.warning(f"无法获取历史分区数据: {e}")
                        
            except Exception as e:
                logging.error(f"❌ DWD OrderDetails表检查失败: {e}")
                logging.error("请按以下步骤解决:")
                logging.error("1. 确保 mysql_to_hive_sync_dag 已成功运行")
                logging.error("2. 运行 dwd_orderdetails_pipeline 创建DWD层数据")
                logging.error("3. 检查HDFS和Hive服务是否正常")
                raise Exception("DWD OrderDetails表不可用，请先运行 dwd_orderdetails_pipeline")
            
            logging.info("✅ DWD依赖检查通过")
            return "dwd_dependencies_ready"
            
        except Exception as e:
            logging.error(f"DWD依赖检查失败: {e}")
            raise
        finally:
            if spark:
                spark.stop()

    check_dependencies_task = PythonOperator(
        task_id='check_dwd_dependencies',
        python_callable=check_dwd_dependencies,
        doc_md="检查DWD层dwd_orderdetails_pipeline是否已成功运行并产生所需数据"
    )

    # 开始任务
    start_task = DummyOperator(
        task_id='start_dws_orderdetails_analytics_pipeline',
        doc_md="""
        ## DWS OrderDetails Analytics Pipeline 开始
        
        这个流程专门处理OrderDetails的DWS层分析ETL:
        1. 检查DWD层依赖（dwd_orderdetails_pipeline的输出）
        2. 聚合日汇总数据
        3. 生成产品分析数据  
        4. 生成仓库分析数据
        5. 加载所有分析结果到HDFS
        6. 创建分析视图
        7. 执行数据质量验证
        """
    )

    # 1. 日汇总聚合
    daily_agg_task = PythonOperator(
        task_id='aggregate_daily_orderdetails',
        python_callable=aggregate_daily_orderdetails,
        doc_md="按日期聚合订单明细数据，生成日汇总指标"
    )

    # 2. 产品分析聚合
    product_agg_task = PythonOperator(
        task_id='aggregate_product_analytics',
        python_callable=aggregate_product_analytics,
        doc_md="按产品聚合分析数据，生成产品销售和绩效分析"
    )

    # 3. 仓库分析聚合
    warehouse_agg_task = PythonOperator(
        task_id='aggregate_warehouse_analytics',
        python_callable=aggregate_warehouse_analytics,
        doc_md="按仓库聚合分析数据，生成仓库效率和绩效分析"
    )

    # 4. 加载到HDFS
    load_task = PythonOperator(
        task_id='load_orderdetails_analytics_to_hdfs',
        python_callable=load_orderdetails_analytics_to_hdfs,
        doc_md="将所有OrderDetails分析结果加载到HDFS的DWS层表中"
    )

    # 5. 创建分析视图
    create_views_task = PythonOperator(
        task_id='create_orderdetails_analytics_views',
        python_callable=create_orderdetails_analytics_views,
        doc_md="创建DWS层OrderDetails分析视图，便于业务查询和报表"
    )

    # 6. 数据验证
    validate_task = PythonOperator(
        task_id='validate_orderdetails_analytics_data',
        python_callable=validate_orderdetails_analytics_data,
        doc_md="执行DWS层OrderDetails数据质量验证和完整性检查"
    )

    # 7. HDFS验证
    verify_hdfs_task = BashOperator(
        task_id='verify_hdfs_orderdetails_analytics',
        bash_command='''
        echo "=== 验证DWS OrderDetails分析表HDFS结构 ==="
        echo "检查DWS数据库目录:"
        curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dws_db.db?op=LISTSTATUS" | python -m json.tool || echo "DWS目录检查失败"
        
        echo "检查OrderDetails分析表:"
        for table in dws_orderdetails_daily_summary dws_product_analytics dws_warehouse_analytics; do
            echo "检查表: $table"
            curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dws_db.db/$table?op=LISTSTATUS" | python -m json.tool || echo "$table 检查失败"
        done
        
        echo "DWS OrderDetails HDFS验证完成"
        ''',
        doc_md="验证HDFS中的DWS OrderDetails分析表结构和数据文件"
    )

    # 结束任务
    end_task = DummyOperator(
        task_id='end_dws_orderdetails_analytics_pipeline',
        doc_md="DWS OrderDetails Analytics Pipeline 完成"
    )

    # 定义任务依赖关系
    start_task >> check_dependencies_task
    
    # 三个聚合任务可以并行执行
    check_dependencies_task >> [daily_agg_task, product_agg_task, warehouse_agg_task]
    
    # 等待所有聚合完成后再加载
    [daily_agg_task, product_agg_task, warehouse_agg_task] >> load_task
    
    # 后续任务串行执行
    load_task >> create_views_task >> validate_task >> verify_hdfs_task >> end_task