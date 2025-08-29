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
    from pyspark.sql.functions import col, sum, count, avg, max, min, when, lit, date_format, year, month, datediff, desc, to_date, from_unixtime, isnan, isnull, input_file_name, regexp_extract
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

        # 检查源表是否存在并验证schema
        try:
            table_desc = spark.sql("DESCRIBE TABLE dwd_db.dwd_orders").collect()
            logging.info("✅ Source table dwd_db.dwd_orders exists")
            
            # Log the schema for debugging
            logging.info("📋 DWD table schema:")
            for row in table_desc:
                if row['col_name'] and not row['col_name'].startswith('#'):
                    logging.info(f"  - {row['col_name']}: {row['data_type']}")
                    
        except Exception as e:
            logging.error(f"❌ Source table dwd_db.dwd_orders not found: {e}")
            context['task_instance'].xcom_push(key='status', value='SKIPPED_NO_SOURCE_TABLE')
            return
        
        # Load DWD data directly from Parquet files to avoid schema conflicts
        logging.info("🔄 Loading DWD data directly from Parquet files to bypass schema issues...")
        
        # Configure Spark for better Parquet handling
        spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
        spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.mergeSchema", "true")
        
        try:
            # Skip Hive table approach due to schema conflicts, go directly to Parquet files
            logging.info("🔄 Using direct HDFS path reading to avoid Hive schema conflicts...")
            
            # Get existing partitions from Hive metastore for reference
            try:
                existing_partitions_df = spark.sql("SHOW PARTITIONS dwd_db.dwd_orders")
                existing_partitions = [row['partition'].split('=')[1] for row in existing_partitions_df.collect()]
                logging.info(f"📋 Found {len(existing_partitions)} existing partitions in Hive metastore")
                
                # Filter partitions within our date range
                start_date_obj = datetime.strptime(start_date_30d, '%Y-%m-%d')
                batch_date_obj = datetime.strptime(batch_date, '%Y-%m-%d')
                
                valid_partitions = []
                for partition_date_str in existing_partitions:
                    try:
                        partition_date_obj = datetime.strptime(partition_date_str, '%Y-%m-%d')
                        if start_date_obj <= partition_date_obj <= batch_date_obj:
                            valid_partitions.append(partition_date_str)
                    except ValueError:
                        continue
                
                logging.info(f"📅 Valid partitions in date range: {valid_partitions}")
                
                if not valid_partitions:
                    logging.warning("No valid partitions found in the specified date range")
                    context['task_instance'].xcom_push(key='status', value='SKIPPED_NO_PARTITIONS')
                    return
                
                # Read directly from HDFS paths for valid partitions
                hdfs_path = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders"
                existing_partition_paths = []
                
                for partition_date in valid_partitions:
                    partition_path = f"{hdfs_path}/dt={partition_date}"
                    
                    # Check if partition path exists and has data
                    try:
                        test_df = spark.read.option("recursiveFileLookup", "true").parquet(partition_path).limit(1)
                        test_df.count()  # Force evaluation
                        existing_partition_paths.append(partition_path)
                        logging.info(f"✅ Found partition: dt={partition_date}")
                    except Exception:
                        logging.info(f"⚠️  Partition not accessible: dt={partition_date}")
                        continue
                
                if not existing_partition_paths:
                    logging.error("❌ No accessible partition paths found on HDFS")
                    context['task_instance'].xcom_push(key='status', value='SKIPPED_NO_PARTITIONS')
                    return
                
                logging.info(f"📂 Reading from {len(existing_partition_paths)} accessible partitions")
                
                # Read parquet files directly with schema merging
                dwd_df_raw = spark.read.option("mergeSchema", "true").option("recursiveFileLookup", "true").parquet(*existing_partition_paths)
                
            except Exception as partition_error:
                logging.warning(f"⚠️  Could not get partitions from Hive: {partition_error}")
                logging.info("🔄 Trying to scan all possible dates in range...")
                
                # Fallback: Check all dates in range
                hdfs_path = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders"
                existing_partition_paths = []
                
                for i in range((datetime.strptime(batch_date, '%Y-%m-%d') - datetime.strptime(start_date_30d, '%Y-%m-%d')).days + 1):
                    partition_date = (datetime.strptime(start_date_30d, '%Y-%m-%d') + timedelta(days=i)).strftime('%Y-%m-%d')
                    partition_path = f"{hdfs_path}/dt={partition_date}"
                    
                    # Check if partition path exists
                    try:
                        test_df = spark.read.option("recursiveFileLookup", "true").parquet(partition_path).limit(1)
                        test_df.count()  # Force evaluation
                        existing_partition_paths.append(partition_path)
                        logging.info(f"✅ Found partition: dt={partition_date}")
                    except Exception:
                        logging.info(f"⚠️  Partition not found: dt={partition_date}")
                        continue
                
                if not existing_partition_paths:
                    logging.error("❌ No valid partition paths found on HDFS")
                    context['task_instance'].xcom_push(key='status', value='SKIPPED_NO_PARTITIONS')
                    return
                
                logging.info(f"📂 Reading from {len(existing_partition_paths)} existing partitions")
                
                # Read parquet files directly with schema merging
                dwd_df_raw = spark.read.option("mergeSchema", "true").option("recursiveFileLookup", "true").parquet(*existing_partition_paths)
            
            # Add dt column if missing (extract from file path)
            if "dt" not in dwd_df_raw.columns:
                dwd_df_raw = dwd_df_raw.withColumn("dt", 
                    regexp_extract(input_file_name(), r"dt=(\d{4}-\d{2}-\d{2})", 1)
                )
            
            # Filter out empty partitions if the column exists
            if "is_empty_partition" in dwd_df_raw.columns:
                dwd_df_raw = dwd_df_raw.filter(
                    (col("is_empty_partition").isNull()) | (col("is_empty_partition") == False)
                )
            
            # Check the actual Parquet schema and handle OrderDate conversion
            logging.info("📋 Checking actual Parquet schema...")
            dwd_schema = dwd_df_raw.schema
            orderdate_field_type = None
            
            for field in dwd_schema.fields:
                if field.name == "OrderDate":
                    orderdate_field_type = str(field.dataType)
                    logging.info(f"OrderDate field type in Parquet: {orderdate_field_type}")
            
            # Check if OrderDate needs conversion based on actual data type
            logging.info("🔄 Checking OrderDate data type and converting if needed")
            
            # Get the actual data type of OrderDate column
            orderdate_field = None
            for field in dwd_df_raw.schema.fields:
                if field.name == "OrderDate":
                    orderdate_field = field
                    break
            
            if orderdate_field:
                orderdate_type = str(orderdate_field.dataType).lower()
                logging.info(f"OrderDate actual type: {orderdate_type}")
                
                if "long" in orderdate_type or "int" in orderdate_type or "bigint" in orderdate_type:
                    # OrderDate is numeric, convert to date
                    logging.info("🔄 Converting OrderDate from numeric to DATE format")
                    dwd_df_30d = dwd_df_raw.withColumn(
                        "OrderDate",
                        when(col("OrderDate").isNull(), None)
                        .when(col("OrderDate") > 1000000000000, to_date(from_unixtime(col("OrderDate") / 1000)))  # milliseconds
                        .when(col("OrderDate") > 1000000000, to_date(from_unixtime(col("OrderDate"))))  # seconds  
                        .when(col("OrderDate") > 19000000, to_date(col("OrderDate").cast("string"), "yyyyMMdd"))  # YYYYMMDD format
                        .otherwise(to_date(col("OrderDate").cast("string")))
                    )
                elif "date" in orderdate_type:
                    # OrderDate is already a date, use as-is
                    logging.info("✅ OrderDate is already in DATE format")
                    dwd_df_30d = dwd_df_raw
                else:
                    # OrderDate is string or other type, try to convert
                    logging.info("🔄 Converting OrderDate from string/other to DATE format")
                    dwd_df_30d = dwd_df_raw.withColumn(
                        "OrderDate",
                        when(col("OrderDate").isNull(), None)
                        .when(col("OrderDate").rlike("^\\d{4}-\\d{2}-\\d{2}$"), to_date(col("OrderDate"), "yyyy-MM-dd"))
                        .when(col("OrderDate").rlike("^\\d{8}$"), to_date(col("OrderDate"), "yyyyMMdd"))
                        .otherwise(to_date(col("OrderDate")))
                    )
            else:
                logging.warning("⚠️  OrderDate column not found, using raw data")
                dwd_df_30d = dwd_df_raw
            
            # Ensure other numeric fields are properly typed with null handling
            dwd_df_30d = dwd_df_30d.withColumn("TotalAmount", 
                when(col("TotalAmount").isNull() | isnan(col("TotalAmount")), 0.0)
                .otherwise(col("TotalAmount").cast("decimal(20,2)"))
            ).withColumn("NetAmount",
                when(col("NetAmount").isNull() | isnan(col("NetAmount")), 0.0)
                .otherwise(col("NetAmount").cast("decimal(20,2)"))
            ).withColumn("ProcessingDays",
                when(col("ProcessingDays").isNull() | isnan(col("ProcessingDays")), 0)
                .otherwise(col("ProcessingDays").cast("int"))
            )
            
            dwd_df_30d.cache()
            logging.info("✅ Successfully loaded and converted DWD data from Parquet files")
            
        except Exception as load_error:
            logging.error(f"❌ Failed to load DWD data from Parquet files: {load_error}")
            context['task_instance'].xcom_push(key='status', value='FAILED_DATA_LOADING')
            raise Exception(f"Could not load DWD data from Parquet files: {load_error}")
        
        # 记录数据范围和统计信息
        total_rows = dwd_df_30d.count()
        logging.info(f"📊 DWD data statistics:")
        logging.info(f"  - Date range: {start_date_30d} to {batch_date}")
        logging.info(f"  - Total rows in 30-day window: {total_rows}")
        
        # Validate OrderDate conversion
        try:
            date_sample = dwd_df_30d.select("OrderDate").filter(col("OrderDate").isNotNull()).limit(5).collect()
            logging.info(f"📅 Sample OrderDate values after conversion: {[row['OrderDate'] for row in date_sample]}")
        except Exception as date_check_e:
            logging.warning(f"Could not sample OrderDate values: {date_check_e}")
        
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
                # 写入数据到表
                daily_summary_with_dt = daily_summary.withColumn("dt", lit(batch_date))
                daily_summary_with_dt.write.mode("overwrite").partitionBy("dt").format("parquet") \
                    .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_daily_summary") \
                    .saveAsTable("dws_orders_daily_summary")
                
                # 执行表修复和刷新
                try:
                    spark.sql("MSCK REPAIR TABLE dws_db.dws_orders_daily_summary")
                    logging.info("🔧 Executed MSCK REPAIR for dws_orders_daily_summary")
                except Exception as repair_e:
                    logging.warning(f"⚠️  MSCK REPAIR failed for dws_orders_daily_summary: {repair_e}")
                
                spark.sql("REFRESH TABLE dws_db.dws_orders_daily_summary")
                
                # 验证表创建成功
                try:
                    row_count = spark.sql("SELECT COUNT(*) as cnt FROM dws_db.dws_orders_daily_summary").collect()[0]['cnt']
                    tables_created_list.append("dws_orders_daily_summary")
                    logging.info(f"✅ Daily aggregation complete and loaded. Row count: {row_count}")
                except Exception as count_e:
                    logging.error(f"❌ Failed to count rows in dws_orders_daily_summary: {count_e}")
                    # 仍然添加到列表，因为表可能存在但为空
                    tables_created_list.append("dws_orders_daily_summary")
                    logging.warning("⚠️  Added table to created list despite count failure")
                    
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
            
            try:
                zero_summary_df.write.mode("overwrite").partitionBy("dt").format("parquet") \
                    .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_daily_summary") \
                    .saveAsTable("dws_orders_daily_summary")
                
                # 执行表修复和刷新
                try:
                    spark.sql("MSCK REPAIR TABLE dws_db.dws_orders_daily_summary")
                    logging.info("🔧 Executed MSCK REPAIR for zero-value dws_orders_daily_summary")
                except Exception as repair_e:
                    logging.warning(f"⚠️  MSCK REPAIR failed for zero-value dws_orders_daily_summary: {repair_e}")
                
                spark.sql("REFRESH TABLE dws_db.dws_orders_daily_summary")
                tables_created_list.append("dws_orders_daily_summary")
                logging.info("✅ Zero-value daily aggregation created for empty partition.")
            except Exception as e:
                logging.error(f"❌ Failed to create zero-value daily summary table: {e}")
                raise
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
                monthly_summary_with_dt = monthly_summary.withColumn("dt", lit(batch_date))
                monthly_summary_with_dt.write.mode("overwrite").partitionBy("dt").format("parquet") \
                    .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_orders_monthly_summary") \
                    .saveAsTable("dws_orders_monthly_summary")
                
                # 执行表修复和刷新
                try:
                    spark.sql("MSCK REPAIR TABLE dws_db.dws_orders_monthly_summary")
                    logging.info("🔧 Executed MSCK REPAIR for dws_orders_monthly_summary")
                except Exception as repair_e:
                    logging.warning(f"⚠️  MSCK REPAIR failed for dws_orders_monthly_summary: {repair_e}")
                
                spark.sql("REFRESH TABLE dws_db.dws_orders_monthly_summary")
                
                # 验证表创建成功
                try:
                    row_count = spark.sql("SELECT COUNT(*) as cnt FROM dws_db.dws_orders_monthly_summary").collect()[0]['cnt']
                    tables_created_list.append("dws_orders_monthly_summary")
                    logging.info(f"✅ Monthly aggregation complete and loaded. Row count: {row_count}")
                except Exception as count_e:
                    logging.error(f"❌ Failed to count rows in dws_orders_monthly_summary: {count_e}")
                    tables_created_list.append("dws_orders_monthly_summary")
                    logging.warning("⚠️  Added table to created list despite count failure")
                    
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
            
            customer_analytics_with_dt = customer_analytics.withColumn("dt", lit(batch_date))
            customer_analytics_with_dt.write.mode("overwrite").partitionBy("dt").format("parquet") \
                .option("path", "hdfs://namenode:9000/user/hive/warehouse/dws_db.db/dws_customer_analytics") \
                .saveAsTable("dws_customer_analytics")
            
            # 执行表修复和刷新
            try:
                spark.sql("MSCK REPAIR TABLE dws_db.dws_customer_analytics")
                logging.info("🔧 Executed MSCK REPAIR for dws_customer_analytics")
            except Exception as repair_e:
                logging.warning(f"⚠️  MSCK REPAIR failed for dws_customer_analytics: {repair_e}")
            
            spark.sql("REFRESH TABLE dws_db.dws_customer_analytics")
            
            # 验证表创建成功
            try:
                row_count = spark.sql("SELECT COUNT(*) as cnt FROM dws_db.dws_customer_analytics").collect()[0]['cnt']
                tables_created_list.append("dws_customer_analytics")
                logging.info(f"✅ Customer analytics complete and loaded. Row count: {row_count}")
            except Exception as count_e:
                logging.error(f"❌ Failed to count rows in dws_customer_analytics: {count_e}")
                tables_created_list.append("dws_customer_analytics")
                logging.warning("⚠️  Added table to created list despite count failure")
            
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
    
    # 检查ETL任务的状态
    etl_status = context['task_instance'].xcom_pull(task_ids='run_dws_analytics_etl_task', key='status')
    if etl_status == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping view creation as no data was processed.")
        return
    elif etl_status == 'SKIPPED_NO_SOURCE_TABLE':
        logging.warning("Skipping view creation as source tables are not available.")
        return
    elif etl_status != 'SUCCESS' and etl_status is not None:
        logging.warning(f"ETL task status is {etl_status}, but proceeding with view creation attempt.")
    
    logging.info(f"ETL task status: {etl_status}, proceeding with view creation.")

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
        
        def check_table_exists(table_name):
            """检查表是否存在并可访问"""
            try:
                spark.sql(f"DESCRIBE TABLE dws_db.{table_name}")
                try:
                    # 尝试简单查询以确保表可访问
                    spark.sql(f"SELECT 1 FROM dws_db.{table_name} LIMIT 1").collect()
                    return True, "accessible"
                except Exception:
                    return True, "exists_but_empty_or_inaccessible"
            except Exception:
                return False, "not_found"
        
        # 验证表是否真正存在
        verified_tables = []
        for table_name in tables_created_from_etl:
            exists, status = check_table_exists(table_name)
            
            if exists:
                verified_tables.append(table_name)
                if status == "accessible":
                    logging.info(f"✅ Verified table exists and is accessible: {table_name}")
                else:
                    logging.warning(f"⚠️  Table {table_name} exists but may be empty or inaccessible")
            else:
                logging.warning(f"❌ Table {table_name} not found, attempting recovery...")
                
                # 尝试重新创建表的元数据
                try:
                    spark.sql(f"MSCK REPAIR TABLE dws_db.{table_name}")
                    spark.sql(f"REFRESH TABLE dws_db.{table_name}")
                    # 再次检查
                    exists_after_repair, status_after_repair = check_table_exists(table_name)
                    if exists_after_repair:
                        verified_tables.append(table_name)
                        logging.info(f"✅ Table {table_name} recovered after metadata repair")
                    else:
                        logging.error(f"❌ Failed to recover table {table_name}")
                except Exception as repair_e:
                    logging.error(f"❌ Failed to repair table {table_name}: {repair_e}")
        
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
                # Extract view name and dependencies before execution
                view_name = view_sql.split('VIEW')[1].split('AS')[0].strip()
                
                # Check if the view depends on tables that exist
                view_sql_upper = view_sql.upper()
                dependencies_exist = True
                missing_deps = []
                
                # Check for table dependencies in the SQL
                if 'DWS_DB.DWS_ORDERS_DAILY_SUMMARY' in view_sql_upper:
                    if 'dws_orders_daily_summary' not in verified_tables:
                        dependencies_exist = False
                        missing_deps.append('dws_orders_daily_summary')
                        
                if 'DWS_DB.DWS_ORDERS_MONTHLY_SUMMARY' in view_sql_upper:
                    if 'dws_orders_monthly_summary' not in verified_tables:
                        dependencies_exist = False
                        missing_deps.append('dws_orders_monthly_summary')
                        
                if 'DWS_DB.DWS_CUSTOMER_ANALYTICS' in view_sql_upper:
                    if 'dws_customer_analytics' not in verified_tables:
                        dependencies_exist = False
                        missing_deps.append('dws_customer_analytics')
                
                if not dependencies_exist:
                    logging.warning(f"⚠️  Skipping view {view_name} due to missing dependencies: {missing_deps}")
                    continue
                
                # Execute the view creation
                spark.sql(view_sql.strip())
                
                # Refresh the view to ensure metadata is synchronized
                try:
                    spark.sql(f"REFRESH TABLE dws_db.{view_name}")
                    logging.info(f"🔄 Refreshed view metadata: {view_name}")
                except Exception as refresh_e:
                    logging.warning(f"⚠️  Could not refresh view {view_name}: {refresh_e}")
                
                created_views.append(view_name)
                logging.info(f"✅ Successfully created view: {view_name}")
            except Exception as e:
                view_name_for_error = "unknown"
                try:
                    view_name_for_error = view_sql.split('VIEW')[1].split('AS')[0].strip()
                except:
                    pass
                logging.error(f"❌ Failed to create view {view_name_for_error} (#{i+1}): {e}")
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
