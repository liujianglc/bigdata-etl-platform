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

def aggregate_daily_orders(**context):
    """聚合每日订单数据"""
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
            .appName("DWS Daily Orders Aggregation") \
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
        
        # 检查DWD Orders表是否存在
        try:
            # 首先检查数据库（兼容不同的数据库命名）
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            logging.info(f"可用数据库: {db_names}")
            
            # 检查可能的数据库名称（优先使用 dwd_db，与 dwd_orders_pipeline 保持一致）
            possible_db_names = ['dwd_db']
            target_db = None
            
            # 优先使用 dwd_db 数据库
            if 'dwd_db' in db_names:
                target_db = 'dwd_db'
            else:
                # 如果 dwd_db 不存在，再尝试其他数据库
                for db_name in possible_db_names[1:]:  # 跳过 dwd_db，从第二个开始
                    if db_name in db_names:
                        target_db = db_name
                        break
            
            # 如果没有找到合适的数据库，则使用默认的dwd_db
            if target_db is None:
                target_db = 'dwd_db'
                logging.warning(f"未找到预期的数据库 {possible_db_names}，将使用默认数据库: {target_db}")
            
            logging.info(f"使用数据库: {target_db}")
            
            spark.sql(f"USE {target_db}")
            
            # 检查可能的表名
            tables = spark.sql("SHOW TABLES").collect()
            table_names = [table[1] for table in tables]
            logging.info(f"{target_db}中的表: {table_names}")
            
            possible_table_names = ['dwd_orders', 'orders']
            target_table = None
            for table_name in possible_table_names:
                if table_name in table_names:
                    target_table = table_name
                    break
            
            if target_table is None:
                logging.error(f"❌ 在 {target_db} 数据库中未找到预期的表 {possible_table_names}")
                raise Exception(f"DWD Orders表不存在，请先运行 dwd_orders_pipeline")
            
            logging.info(f"✅ 使用 {target_db}.{target_table} 表")
            full_table_name = f"{target_db}.{target_table}"
        except Exception as e:
            logging.error(f"❌ DWD Orders表不存在: {e}")
            raise Exception("DWD Orders表不存在，请先运行 dwd_orders_pipeline")
        
        batch_date = context['ds']
        logging.info(f"处理日期: {batch_date}")
        
        # 读取DWD Orders数据
        orders_df = spark.sql(f"""
            SELECT *
            FROM {full_table_name}
            WHERE dt = '{batch_date}'
        """)
        
        record_count = orders_df.count()
        logging.info(f"读取到 {record_count} 条DWD订单记录")
        
        if record_count == 0:
            logging.warning("没有找到当天的DWD数据")
            # 创建空的聚合结果
            daily_summary = spark.createDataFrame([], schema="order_date string, total_orders bigint, total_amount double")
        else:
            # 按日期聚合订单数据
            daily_summary = orders_df.groupBy(
                date_format(col("OrderDate"), "yyyy-MM-dd").alias("order_date")
            ).agg(
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
                count(when(col("DataQualityLevel") == "Excellent", 1)).alias("excellent_quality_orders"),
                count(when(col("DataQualityLevel") == "Poor", 1)).alias("poor_quality_orders")
            ).withColumn("completion_rate", 
                        (col("completed_orders") / col("total_orders") * 100)
            ).withColumn("cancellation_rate",
                        (col("cancelled_orders") / col("total_orders") * 100)
            ).withColumn("delay_rate",
                        (col("delayed_orders") / col("total_orders") * 100)
            ).withColumn("data_quality_score",
                        ((col("excellent_quality_orders") * 4 + 
                          (col("total_orders") - col("excellent_quality_orders") - col("poor_quality_orders")) * 2) / 
                         col("total_orders"))
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
                logging.info(f"日期 {row['order_date']}: 订单 {row['total_orders']} 笔, "
                           f"金额 {row['total_amount']:.2f}, 完成率 {row['completion_rate']:.1f}%")
        
        # 保存到临时文件
        temp_dir = tempfile.mkdtemp(prefix='daily_orders_')
        temp_file = os.path.join(temp_dir, f'daily_orders_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
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

def aggregate_monthly_orders(**context):
    """聚合每月订单数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, coalesce, lit, date_format, year, month
    import logging
    import os
    import tempfile
    from datetime import datetime, timedelta
    
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("DWS Monthly Orders Aggregation") \
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
        
        # 检查DWD Orders表是否存在
        try:
            # 首先检查数据库（兼容不同的数据库命名）
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            logging.info(f"可用数据库: {db_names}")
            
            # 检查可能的数据库名称（优先使用 dwd_db，与 dwd_orders_pipeline 保持一致）
            possible_db_names = ['dwd_db', 'wudeli_analytics']
            target_db = None
            
            # 优先使用 dwd_db 数据库
            if 'dwd_db' in db_names:
                target_db = 'dwd_db'
            else:
                # 如果 dwd_db 不存在，再尝试其他数据库
                for db_name in possible_db_names[1:]:  # 跳过 dwd_db，从第二个开始
                    if db_name in db_names:
                        target_db = db_name
                        break
            
            # 如果没有找到合适的数据库，则使用默认的dwd_db
            if target_db is None:
                target_db = 'dwd_db'
                logging.warning(f"未找到预期的数据库 {possible_db_names}，将使用默认数据库: {target_db}")
            
            logging.info(f"使用数据库: {target_db}")
            
            spark.sql(f"USE {target_db}")
            
            # 检查可能的表名
            tables = spark.sql("SHOW TABLES").collect()
            table_names = [table[1] for table in tables]
            logging.info(f"{target_db}中的表: {table_names}")
            
            possible_table_names = ['dwd_orders', 'orders']
            target_table = None
            for table_name in possible_table_names:
                if table_name in table_names:
                    target_table = table_name
                    break
            
            if target_table is None:
                logging.error(f"❌ 在 {target_db} 数据库中未找到预期的表 {possible_table_names}")
                raise Exception(f"DWD Orders表不存在，请先运行 dwd_orders_pipeline")
            
            logging.info(f"✅ 使用 {target_db}.{target_table} 表")
            full_table_name = f"{target_db}.{target_table}"
        except Exception as e:
            logging.error(f"❌ DWD Orders表不存在: {e}")
            raise Exception("DWD Orders表不存在，请先运行 dwd_orders_pipeline")
        
        batch_date = context['ds']
        current_month = datetime.strptime(batch_date, '%Y-%m-%d').strftime('%Y-%m')
        
        logging.info(f"处理月份: {current_month}")
        
        # 读取当月所有DWD Orders数据
        orders_df = spark.sql(f"""
            SELECT *
            FROM {full_table_name}
            WHERE date_format(OrderDate, 'yyyy-MM') = '{current_month}'
        """)
        
        record_count = orders_df.count()
        logging.info(f"读取到当月 {record_count} 条订单记录")
        
        if record_count == 0:
            logging.warning("没有找到当月的订单数据")
            monthly_summary = spark.createDataFrame([], schema="year_month string, total_orders bigint")
        else:
            # 按月聚合
            monthly_summary = orders_df.groupBy(
                date_format(col("OrderDate"), "yyyy-MM").alias("year_month")
            ).agg(
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
                # 按客户类型统计
                count(when(col("CustomerType") == "VIP", 1)).alias("vip_customer_orders"),
                count(when(col("CustomerType") == "Regular", 1)).alias("regular_customer_orders"),
                # 按订单规模统计
                count(when(col("OrderSizeCategory") == "Large", 1)).alias("large_orders"),
                count(when(col("OrderSizeCategory") == "Medium", 1)).alias("medium_orders"),
                count(when(col("OrderSizeCategory") == "Small", 1)).alias("small_orders"),
                count(when(col("OrderSizeCategory") == "Micro", 1)).alias("micro_orders"),
                # 按支付方式统计
                count(when(col("PaymentMethod") == "Credit Card", 1)).alias("credit_card_orders"),
                count(when(col("PaymentMethod") == "Cash", 1)).alias("cash_orders"),
                count(when(col("PaymentMethod") == "Bank Transfer", 1)).alias("bank_transfer_orders")
            ).withColumn("completion_rate", 
                        (col("completed_orders") / col("total_orders") * 100)
            ).withColumn("cancellation_rate",
                        (col("cancelled_orders") / col("total_orders") * 100)
            ).withColumn("delay_rate",
                        (col("delayed_orders") / col("total_orders") * 100)
            ).withColumn("vip_ratio",
                        (col("vip_orders") / col("total_orders") * 100)
            ).withColumn("large_order_ratio",
                        (col("large_orders") / col("total_orders") * 100)
            ).withColumn("etl_created_date", lit(datetime.now())) \
             .withColumn("etl_batch_id", lit(context['ds_nodash'])) \
             .withColumn("dt", lit(batch_date))
        
        # 缓存并统计
        monthly_summary.cache()
        summary_count = monthly_summary.count()
        logging.info(f"生成 {summary_count} 条月汇总记录")
        
        # 显示统计信息
        if summary_count > 0:
            summary_stats = monthly_summary.collect()
            for row in summary_stats:
                logging.info(f"月份 {row['year_month']}: 订单 {row['total_orders']} 笔, "
                           f"金额 {row['total_amount']:.2f}, VIP占比 {row['vip_ratio']:.1f}%")
        
        # 保存到临时文件
        temp_dir = tempfile.mkdtemp(prefix='monthly_orders_')
        temp_file = os.path.join(temp_dir, f'monthly_orders_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
        pandas_df = monthly_summary.toPandas()
        monthly_summary.unpersist()
        
        pandas_df.to_parquet(temp_file, index=False)
        logging.info(f"月汇总数据已保存到: {temp_file}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='monthly_summary_file', value=temp_file)
        context['task_instance'].xcom_push(key='monthly_temp_dir', value=temp_dir)
        context['task_instance'].xcom_push(key='monthly_record_count', value=summary_count)
        
        return temp_file
        
    except Exception as e:
        logging.error(f"月汇总聚合失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def aggregate_customer_analytics(**context):
    """聚合客户分析数据"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, coalesce, lit, datediff, desc
    import logging
    import os
    import tempfile
    from datetime import datetime
    
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("DWS Customer Analytics") \
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
        
        # 检查DWD Orders表是否存在
        try:
            # 首先检查数据库（兼容不同的数据库命名）
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            logging.info(f"可用数据库: {db_names}")
            
            # 检查可能的数据库名称（优先使用 dwd_db，与 dwd_orders_pipeline 保持一致）
            possible_db_names = ['dwd_db', 'wudeli_analytics']
            target_db = None
            
            # 优先使用 dwd_db 数据库
            if 'dwd_db' in db_names:
                target_db = 'dwd_db'
            else:
                # 如果 dwd_db 不存在，再尝试其他数据库
                for db_name in possible_db_names[1:]:  # 跳过 dwd_db，从第二个开始
                    if db_name in db_names:
                        target_db = db_name
                        break
            
            # 如果没有找到合适的数据库，则使用默认的dwd_db
            if target_db is None:
                target_db = 'dwd_db'
                logging.warning(f"未找到预期的数据库 {possible_db_names}，将使用默认数据库: {target_db}")
            
            logging.info(f"使用数据库: {target_db}")
            
            spark.sql(f"USE {target_db}")
            
            # 检查可能的表名
            tables = spark.sql("SHOW TABLES").collect()
            table_names = [table[1] for table in tables]
            logging.info(f"{target_db}中的表: {table_names}")
            
            possible_table_names = ['dwd_orders', 'orders']
            target_table = None
            for table_name in possible_table_names:
                if table_name in table_names:
                    target_table = table_name
                    break
            
            if target_table is None:
                logging.error(f"❌ 在 {target_db} 数据库中未找到预期的表 {possible_table_names}")
                raise Exception(f"DWD Orders表不存在，请先运行 dwd_orders_pipeline")
            
            logging.info(f"✅ 使用 {target_db}.{target_table} 表")
            full_table_name = f"{target_db}.{target_table}"
        except Exception as e:
            logging.error(f"❌ DWD Orders表不存在: {e}")
            raise Exception("DWD Orders表不存在，请先运行 dwd_orders_pipeline")
        
        batch_date = context['ds']
        
        # 计算最近30天的客户分析数据
        start_date = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logging.info(f"分析时间范围: {start_date} 到 {batch_date}")
        
        # 读取最近30天的订单数据
        orders_df = spark.sql(f"""
            SELECT *
            FROM {full_table_name}
            WHERE OrderDate >= '{start_date}' AND OrderDate <= '{batch_date}'
        """)
        
        record_count = orders_df.count()
        logging.info(f"读取到最近30天 {record_count} 条订单记录")
        
        if record_count == 0:
            logging.warning("没有找到最近30天的订单数据")
            customer_analytics = spark.createDataFrame([], schema="customer_id string, customer_name string")
        else:
            # 按客户聚合分析
            customer_analytics = orders_df.groupBy(
                col("CustomerID").alias("customer_id"),
                col("CustomerName").alias("customer_name"),
                col("CustomerType").alias("customer_type")
            ).agg(
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
            ).withColumn("completion_rate",
                        (col("completed_orders") / col("total_orders") * 100)
            ).withColumn("cancellation_rate",
                        (col("cancelled_orders") / col("total_orders") * 100)
            ).withColumn("delay_rate",
                        (col("delayed_orders") / col("total_orders") * 100)
            ).withColumn("days_since_last_order",
                        datediff(lit(batch_date), col("last_order_date"))
            ).withColumn("customer_lifetime_days",
                        datediff(col("last_order_date"), col("first_order_date")) + 1
            ).withColumn("order_frequency",
                        col("total_orders") / (col("customer_lifetime_days") / 30.0)  # 每月订单数
            ).withColumn("customer_segment",
                        when(col("total_spent") >= 50000, "Platinum")
                        .when(col("total_spent") >= 20000, "Gold")
                        .when(col("total_spent") >= 5000, "Silver")
                        .otherwise("Bronze")
            ).withColumn("customer_status",
                        when(col("days_since_last_order") <= 7, "Active")
                        .when(col("days_since_last_order") <= 30, "Recent")
                        .when(col("days_since_last_order") <= 90, "Inactive")
                        .otherwise("Dormant")
            ).withColumn("etl_created_date", lit(datetime.now())) \
             .withColumn("etl_batch_id", lit(context['ds_nodash'])) \
             .withColumn("analysis_period_start", lit(start_date)) \
             .withColumn("analysis_period_end", lit(batch_date)) \
             .withColumn("dt", lit(batch_date))
        
        # 缓存并统计
        customer_analytics.cache()
        analytics_count = customer_analytics.count()
        logging.info(f"生成 {analytics_count} 条客户分析记录")
        
        # 显示客户分段统计
        if analytics_count > 0:
            segment_stats = customer_analytics.groupBy("customer_segment").count().collect()
            logging.info("客户分段分布:")
            for row in segment_stats:
                logging.info(f"  {row['customer_segment']}: {row['count']} 位客户")
            
            status_stats = customer_analytics.groupBy("customer_status").count().collect()
            logging.info("客户状态分布:")
            for row in status_stats:
                logging.info(f"  {row['customer_status']}: {row['count']} 位客户")
        
        # 保存到临时文件
        temp_dir = tempfile.mkdtemp(prefix='customer_analytics_')
        temp_file = os.path.join(temp_dir, f'customer_analytics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
        pandas_df = customer_analytics.toPandas()
        customer_analytics.unpersist()
        
        pandas_df.to_parquet(temp_file, index=False)
        logging.info(f"客户分析数据已保存到: {temp_file}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='customer_analytics_file', value=temp_file)
        context['task_instance'].xcom_push(key='customer_temp_dir', value=temp_dir)
        context['task_instance'].xcom_push(key='customer_record_count', value=analytics_count)
        
        return temp_file
        
    except Exception as e:
        logging.error(f"客户分析聚合失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

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

def create_analytics_views(**context):
    """创建DWS分析视图"""
    from pyspark.sql import SparkSession
    import logging
    
    spark = None
    try:
        # 获取创建的表信息
        tables_created = context['task_instance'].xcom_pull(task_ids='load_analytics_to_hdfs', key='tables_created')
        
        # 创建Spark会话 - 最小内存配置
        spark = SparkSession.builder \
            .appName("Create DWS Analytics Views") \
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
                FROM dws_orders_daily_summary
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
                FROM dws_orders_daily_summary
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
                FROM dws_orders_monthly_summary
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
                FROM dws_orders_monthly_summary
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
                FROM dws_customer_analytics
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
                FROM dws_customer_analytics
                GROUP BY customer_status
                """,
                
                # 高价值客户视图
                """
                CREATE OR REPLACE VIEW dws_high_value_customers AS
                SELECT 
                    customer_id,
                    customer_name,
                    customer_type,
                    customer_segment,
                    customer_status,
                    total_spent,
                    total_orders,
                    avg_order_value,
                    completion_rate,
                    days_since_last_order,
                    order_frequency
                FROM dws_customer_analytics
                WHERE customer_segment IN ('Platinum', 'Gold')
                   OR total_spent >= 20000
                ORDER BY total_spent DESC
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
            dws_views = [row[1] for row in views if row[1].startswith('dws_')]
            logging.info(f"创建的DWS视图: {dws_views}")
        except Exception as e:
            logging.warning(f"无法列出视图: {e}")
        
        logging.info(f"✅ DWS分析视图创建完成，成功创建 {views_created} 个视图")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='views_created_count', value=views_created)
        context['task_instance'].xcom_push(key='available_tables', value=tables_created)
        
        return views_created
        
    except Exception as e:
        logging.error(f"DWS视图创建失败: {e}")
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

def validate_analytics_data(**context):
    """验证DWS分析数据质量"""
    import logging
    
    try:
        # 获取相关信息
        load_summary = context['task_instance'].xcom_pull(task_ids='load_analytics_to_hdfs', key='load_summary')
        views_created = context['task_instance'].xcom_pull(task_ids='create_analytics_views', key='views_created_count')
        
        # 获取各阶段的记录数
        daily_count = context['task_instance'].xcom_pull(task_ids='aggregate_daily_orders', key='daily_record_count')
        monthly_count = context['task_instance'].xcom_pull(task_ids='aggregate_monthly_orders', key='monthly_record_count')
        customer_count = context['task_instance'].xcom_pull(task_ids='aggregate_customer_analytics', key='customer_record_count')
        
        # 数据质量验证结果
        validation_results = {
            'tables_created': load_summary['total_tables'],
            'views_created': views_created,
            'data_summary': {
                'daily_records': daily_count or 0,
                'monthly_records': monthly_count or 0,
                'customer_records': customer_count or 0
            },
            'batch_date': load_summary['batch_date']
        }
        
        # 验证结果评估
        quality_score = 100
        issues = []
        
        # 检查表创建情况
        expected_tables = 3  # 日汇总、月汇总、客户分析
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
        total_records = (daily_count or 0) + (monthly_count or 0) + (customer_count or 0)
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
        logging.info("=== DWS Orders Analytics 数据质量验证结果 ===")
        logging.info(f"总体质量分数: {quality_score}/100")
        logging.info(f"验证状态: {validation_results['validation_status']}")
        logging.info(f"创建表数: {load_summary['total_tables']}")
        logging.info(f"创建视图数: {views_created}")
        logging.info(f"数据记录统计:")
        logging.info(f"  - 日汇总: {daily_count or 0} 条")
        logging.info(f"  - 月汇总: {monthly_count or 0} 条")
        logging.info(f"  - 客户分析: {customer_count or 0} 条")
        
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
    'dws_orders_analytics',
    default_args=default_args,
    schedule_interval='0 4 * * *',  # 每日凌晨4点执行（在dwd_orders_pipeline的3点之后）
    catchup=False,
    max_active_runs=1,
    tags=['dws', 'analytics', 'orders', 'aggregation', 'downstream'],
    description='DWS层Orders分析表ETL流程 - 基于DWD层数据进行多维度聚合分析',
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
            
            # 检查DWD Orders表是否存在且有当天数据
            batch_date = context['ds']
            
            try:
                # 首先检查数据库是否存在（兼容不同的数据库命名）
                databases = spark.sql("SHOW DATABASES").collect()
                db_names = [db[0] for db in databases]
                logging.info(f"可用数据库: {db_names}")
                
                # 检查可能的数据库名称（优先使用 dwd_db，与 dwd_orders_pipeline 保持一致）
                possible_db_names = ['dwd_db', 'wudeli_analytics']
                target_db = None
                
                # 优先使用 dwd_db 数据库
                if 'dwd_db' in db_names:
                    target_db = 'dwd_db'
                else:
                    # 如果 dwd_db 不存在，再尝试其他数据库
                    for db_name in possible_db_names[1:]:  # 跳过 dwd_db，从第二个开始
                        if db_name in db_names:
                            target_db = db_name
                            break
                
                # 如果没有找到合适的数据库，则使用默认的dwd_db
                if target_db is None:
                    target_db = 'dwd_db'
                    logging.warning(f"未找到预期的数据库 {possible_db_names}，将使用默认数据库: {target_db}")
                
                logging.info(f"使用数据库: {target_db}")
                
                # 切换到目标数据库
                spark.sql(f"USE {target_db}")
                
                # 检查dwd_orders表是否存在
                tables = spark.sql("SHOW TABLES").collect()
                table_names = [table[1] for table in tables]
                logging.info(f"{target_db}中的表: {table_names}")
                
                # 检查可能的表名
                possible_table_names = ['dwd_orders', 'orders']
                target_table = None
                for table_name in possible_table_names:
                    if table_name in table_names:
                        target_table = table_name
                        break
                
                if target_table is None:
                    logging.error(f"❌ 在 {target_db} 数据库中未找到预期的表 {possible_table_names}")
                    raise Exception(f"在 {target_db} 数据库中未找到预期的表 {possible_table_names}，请先运行 dwd_orders_pipeline 创建DWD层数据")
                
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
                logging.error(f"❌ DWD Orders表检查失败: {e}")
                logging.error("请按以下步骤解决:")
                logging.error("1. 确保 mysql_to_hive_sync_dag 已成功运行")
                logging.error("2. 运行 dwd_orders_pipeline 创建DWD层数据")
                logging.error("3. 检查HDFS和Hive服务是否正常")
                raise Exception("DWD Orders表不可用，请先运行 dwd_orders_pipeline")
            
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
        doc_md="检查DWD层dwd_orders_pipeline是否已成功运行并产生所需数据"
    )

    # 开始任务
    start_task = DummyOperator(
        task_id='start_dws_analytics_pipeline',
        doc_md="""
        ## DWS Orders Analytics Pipeline 开始
        
        这个流程专门处理Orders的DWS层分析ETL:
        1. 检查DWD层依赖（dwd_orders_pipeline的输出）
        2. 聚合日汇总数据
        3. 聚合月汇总数据  
        4. 生成客户分析数据
        5. 加载所有分析结果到HDFS
        6. 创建分析视图
        7. 执行数据质量验证
        """
    )

    # 1. 日汇总聚合
    daily_agg_task = PythonOperator(
        task_id='aggregate_daily_orders',
        python_callable=aggregate_daily_orders,
        doc_md="按日期聚合订单数据，生成日汇总指标"
    )

    # 2. 月汇总聚合
    monthly_agg_task = PythonOperator(
        task_id='aggregate_monthly_orders',
        python_callable=aggregate_monthly_orders,
        doc_md="按月份聚合订单数据，生成月汇总指标"
    )

    # 3. 客户分析聚合
    customer_agg_task = PythonOperator(
        task_id='aggregate_customer_analytics',
        python_callable=aggregate_customer_analytics,
        doc_md="按客户聚合分析数据，生成客户价值和行为分析"
    )

    # 4. 加载到HDFS
    load_task = PythonOperator(
        task_id='load_analytics_to_hdfs',
        python_callable=load_analytics_to_hdfs,
        doc_md="将所有分析结果加载到HDFS的DWS层表中"
    )

    # 5. 创建分析视图
    create_views_task = PythonOperator(
        task_id='create_analytics_views',
        python_callable=create_analytics_views,
        doc_md="创建DWS层分析视图，便于业务查询和报表"
    )

    # 6. 数据验证
    validate_task = PythonOperator(
        task_id='validate_analytics_data',
        python_callable=validate_analytics_data,
        doc_md="执行DWS层数据质量验证和完整性检查"
    )

    # 7. HDFS验证
    verify_hdfs_task = BashOperator(
        task_id='verify_hdfs_analytics',
        bash_command='''
        echo "=== 验证DWS分析表HDFS结构 ==="
        echo "检查DWS数据库目录:"
        curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dws_db.db?op=LISTSTATUS" | python -m json.tool || echo "DWS目录检查失败"
        
        echo "检查分析表:"
        for table in dws_orders_daily_summary dws_orders_monthly_summary dws_customer_analytics; do
            echo "检查表: $table"
            curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dws_db.db/$table?op=LISTSTATUS" | python -m json.tool || echo "$table 检查失败"
        done
        
        echo "DWS HDFS验证完成"
        ''',
        doc_md="验证HDFS中的DWS分析表结构和数据文件"
    )

    # 结束任务
    end_task = DummyOperator(
        task_id='end_dws_analytics_pipeline',
        doc_md="DWS Orders Analytics Pipeline 完成"
    )

    # 定义任务依赖关系
    start_task >> check_dependencies_task
    
    # 三个聚合任务可以并行执行
    check_dependencies_task >> [daily_agg_task, monthly_agg_task, customer_agg_task]
    
    # 等待所有聚合完成后再加载
    [daily_agg_task, monthly_agg_task, customer_agg_task] >> load_task
    
    # 后续任务串行执行
    load_task >> create_views_task >> validate_task >> verify_hdfs_task >> end_task