from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import os
import yaml
from pyspark.sql.types import DecimalType

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
        logging.warning(f"配置文件 {file_path} 不存在，使用默认配置。")
        return default_config


def get_table_partition_strategy(spark, table_name, target_date, config):
    """根据配置获取表的最佳分区日期，能处理非分区表。"""
    try:
        table_config = config.get('tables', {}).get(table_name, {})
        partition_strategy = table_config.get('partition_strategy')

        # 如果策略是 aatest_only，我们知道它没有分区，直接返回None
        if partition_strategy == 'latest_only':
            logging.info(f"表 {table_name} 策略为 'latest_only'，不使用分区进行JOIN。")
            return None

        # 对于其他策略，我们假设它是分区的并继续
        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        available_dates = [p[0].split('=')[1] for p in partitions]
        
        if not available_dates:
            logging.warning(f"{table_name} 是分区表但未找到任何分区，使用目标日期: {target_date}")
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
                logging.warning(f"{table_name} 目标分区 dt={target_date} 不存在，回退到最新分区: dt={latest_date}")
                return latest_date
        
        return target_date
            
    except Exception as e:
        # 异常处理作为备用，以防配置和实际情况不符
        if "PARTITION_SCHEMA_IS_EMPTY" in str(e) or "is not partitioned" in str(e):
             logging.warning(f"表 {table_name} 配置为分区表，但实际上未找到分区。将不使用分区进行JOIN。")
             return None
        else:
            logging.error(f"检查 {table_name} 分区失败: {e}")
            return target_date # 回退


# =============================================================================
# MAIN SPARK ETL TASK
# =============================================================================
def run_dwd_orderdetails_etl(**context):
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.functions import (
        col, when, year, month, dayofmonth, dayofweek, quarter, coalesce, lit, 
        min, max, current_timestamp, length, avg, sum as spark_sum, count as spark_count,
        create_map
    )

    spark = None
    try:
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName("DWD_OrderDetails_ETL_Pipeline") \
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
            .enableHiveSupport() \
            .getOrCreate()
        logging.info("✅ Spark session created successfully.")

        batch_date = context['ds']
        logging.info(f"Starting ETL for batch date: {batch_date}")

        partition_config = load_config('/opt/airflow/config/table_partition_strategy.yaml')
        
        # 首先检查主表 ods.OrderDetails 的分区策略
        orderdetails_partition = get_table_partition_strategy(spark, 'ods.OrderDetails', batch_date, partition_config)
        logging.info(f"ods.OrderDetails partition strategy result: {orderdetails_partition}")
        
        # 包含所有需要检查的表，包括主表
        all_tables_to_check = ['ods.OrderDetails', 'ods.Orders', 'ods.Customers', 'ods.Products', 'ods.Warehouses', 'ods.Factories']
        table_partition_info = {t: get_table_partition_strategy(spark, t, batch_date, partition_config) for t in all_tables_to_check}
        
        # 维度表用于JOIN
        dim_tables_to_join = ['ods.Orders', 'ods.Customers', 'ods.Products', 'ods.Warehouses', 'ods.Factories']
        
        # 调试信息：检查所有表的分区策略和数据
        for table, partition_date in table_partition_info.items():
            try:
                if partition_date:
                    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE dt='{partition_date}'").collect()[0]['cnt']
                    logging.info(f"{table} using partition dt='{partition_date}' with {count} records")
                else:
                    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
                    logging.info(f"{table} using no partition filter with {count} total records")
            except Exception as e:
                logging.warning(f"Failed to check {table}: {e}")
        
        # 使用统一的分区信息
        orderdetails_partition = table_partition_info.get('ods.OrderDetails')
        
        # 动态构建JOIN子句
        join_clauses = []
        dim_tables_meta = {
            'ods.Orders': ('o', 'od.OrderID = o.OrderID'),
            'ods.Customers': ('c', 'o.CustomerID = c.CustomerID'),
            'ods.Products': ('p', 'od.ProductID = p.ProductID'),
            'ods.Warehouses': ('w', 'od.WarehouseID = w.WarehouseID'),
            'ods.Factories': ('f', 'w.FactoryID = f.FactoryID')
        }

        for table, (alias, join_cond) in dim_tables_meta.items():
            partition_date = table_partition_info.get(table)
            partition_filter = f" AND {alias}.dt = '{partition_date}'" if partition_date else ""
            join_clauses.append(f"LEFT JOIN {table} {alias} ON {join_cond}{partition_filter}")

        join_sql = "\n".join(join_clauses)

        # 主表数据检查已经在上面的循环中完成了，这里只需要确认
        if orderdetails_partition != batch_date and orderdetails_partition:
            logging.warning(f"ods.OrderDetails: Using fallback partition {orderdetails_partition} instead of target date {batch_date}")
        elif not orderdetails_partition:
            logging.info("ods.OrderDetails: Using non-partitioned table or no partition filter")

        # 使用窗口函数确保维度表数据唯一性，避免JOIN产生重复记录
        orderdetails_filter = f"od.dt='{orderdetails_partition}'" if orderdetails_partition else "1=1"
        
        # 构建去重的维度表CTE
        orders_partition = table_partition_info.get('ods.Orders')
        customers_partition = table_partition_info.get('ods.Customers')
        products_partition = table_partition_info.get('ods.Products')
        warehouses_partition = table_partition_info.get('ods.Warehouses')
        factories_partition = table_partition_info.get('ods.Factories')
        
        query = f"""
        WITH unique_orders AS (
            SELECT 
                OrderID, CustomerID, OrderDate, Status, PaymentMethod, PaymentStatus,
                ROW_NUMBER() OVER (
                    PARTITION BY OrderID 
                    ORDER BY UpdatedDate DESC, CreatedDate DESC
                ) as rn
            FROM ods.Orders o
            WHERE {f"o.dt = '{orders_partition}'" if orders_partition else "1=1"}
        ),
        unique_customers AS (
            SELECT 
                CustomerID, CustomerName, CustomerType,
                ROW_NUMBER() OVER (
                    PARTITION BY CustomerID 
                    ORDER BY UpdatedDate DESC, CreatedDate DESC
                ) as rn
            FROM ods.Customers c
            WHERE {f"c.dt = '{customers_partition}'" if customers_partition else "1=1"}
        ),
        unique_products AS (
            SELECT 
                ProductID, ProductName, Category, Specification,
                ROW_NUMBER() OVER (
                    PARTITION BY ProductID 
                    ORDER BY UpdatedDate DESC, CreatedDate DESC
                ) as rn
            FROM ods.Products p
            WHERE {f"p.dt = '{products_partition}'" if products_partition else "1=1"}
        ),
        unique_warehouses AS (
            SELECT 
                WarehouseID, WarehouseName, Manager, FactoryID,
                ROW_NUMBER() OVER (
                    PARTITION BY WarehouseID 
                    ORDER BY UpdatedDate DESC, CreatedDate DESC
                ) as rn
            FROM ods.Warehouses w
            WHERE {f"w.dt = '{warehouses_partition}'" if warehouses_partition else "1=1"}
        ),
        unique_factories AS (
            SELECT 
                FactoryID, FactoryName, Location,
                ROW_NUMBER() OVER (
                    PARTITION BY FactoryID 
                    ORDER BY UpdatedDate DESC, CreatedDate DESC
                ) as rn
            FROM ods.Factories f
            WHERE {f"f.dt = '{factories_partition}'" if factories_partition else "1=1"}
        )
        SELECT 
            od.*,
            uo.CustomerID,
            uc.CustomerName,
            uc.CustomerType,
            up.ProductName,
            up.Category as ProductCategory,
            up.Specification as ProductSpecification,
            uw.WarehouseName,
            uw.Manager as WarehouseManager,
            uf.FactoryName,
            uf.Location as FactoryLocation,
            uo.OrderDate,
            uo.Status as OrderStatus,
            uo.PaymentMethod,
            uo.PaymentStatus
        FROM ods.OrderDetails od
        LEFT JOIN unique_orders uo ON od.OrderID = uo.OrderID AND uo.rn = 1
        LEFT JOIN unique_customers uc ON uo.CustomerID = uc.CustomerID AND uc.rn = 1
        LEFT JOIN unique_products up ON od.ProductID = up.ProductID AND up.rn = 1
        LEFT JOIN unique_warehouses uw ON od.WarehouseID = uw.WarehouseID AND uw.rn = 1
        LEFT JOIN unique_factories uf ON uw.FactoryID = uf.FactoryID AND uf.rn = 1
        WHERE {orderdetails_filter}
        """
        
        logging.info(f"Executing dynamically generated query:\n{query}")
        
        # 分步调试：先检查基础查询和去重效果
        try:
            base_query = f"SELECT COUNT(*) as cnt FROM ods.OrderDetails od WHERE {orderdetails_filter}"
            base_count = spark.sql(base_query).collect()[0]['cnt']
            logging.info(f"Base OrderDetails count with filter '{orderdetails_filter}': {base_count}")
            
            if base_count > 0:
                # 检查维度表去重效果
                for table, partition in [
                    ('ods.Orders', orders_partition),
                    ('ods.Customers', customers_partition), 
                    ('ods.Products', products_partition),
                    ('ods.Warehouses', warehouses_partition),
                    ('ods.Factories', factories_partition)
                ]:
                    try:
                        where_clause = f"WHERE dt = '{partition}'" if partition else ""
                        total_query = f"SELECT COUNT(*) as total FROM {table} {where_clause}"
                        table_name = table.split('.')[1]
                        if table_name == 'Factories':
                            column_name = 'FactoryID'
                        elif table_name == 'Warehouses':
                            column_name = 'WarehouseID'
                        else:
                            column_name = f"{table_name[:-1]}ID"
                        unique_query = f"SELECT COUNT(DISTINCT {column_name}) as unique FROM {table} {where_clause}"
                        
                        total = spark.sql(total_query).collect()[0]['total']
                        unique = spark.sql(unique_query).collect()[0]['unique']
                        
                        if total != unique:
                            logging.warning(f"⚠️ {table}: {total} total records, {unique} unique IDs (potential duplicates)")
                        else:
                            logging.info(f"✅ {table}: {total} records, all unique")
                    except Exception as dim_e:
                        logging.warning(f"Could not check {table}: {dim_e}")
                        
        except Exception as e:
            logging.warning(f"Debug query failed: {e}")
        
        df = spark.sql(query)
        logging.info('Executed query successfully.')
        
        # Use limit(1).count() instead of rdd.isEmpty() for better performance
        record_count = df.limit(1).count()
        if record_count == 0:
            logging.warning("No records found for this batch.")
            table_name = "dwd_db.dwd_orderdetails"
            location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orderdetails"
            
            # Create an empty DataFrame with the full transformed schema
            from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType, TimestampType, BooleanType, IntegerType, DoubleType
            
            transformed_schema = StructType([
                StructField("OrderDetailID", IntegerType(), True),
                StructField("OrderID", IntegerType(), True),
                StructField("ProductID", IntegerType(), True),
                StructField("Quantity", IntegerType(), True),
                StructField("UnitPrice", DecimalType(10,2), True),
                StructField("Discount", DecimalType(5,2), True),
                StructField("Amount", DecimalType(10,2), True),
                StructField("WarehouseID", IntegerType(), True),
                StructField("Status", StringType(), True),
                StructField("CreatedDate", TimestampType(), True),
                StructField("UpdatedDate", TimestampType(), True),
                StructField("CustomerID", IntegerType(), True),
                StructField("CustomerName", StringType(), True),
                StructField("CustomerType", StringType(), True),
                StructField("ProductName", StringType(), True),
                StructField("ProductCategory", StringType(), True),
                StructField("ProductSpecification", StringType(), True),
                StructField("WarehouseName", StringType(), True),
                StructField("WarehouseManager", StringType(), True),
                StructField("FactoryName", StringType(), True),
                StructField("FactoryLocation", StringType(), True),
                StructField("OrderDate", DateType(), True),
                StructField("OrderStatus", StringType(), True),
                StructField("PaymentMethod", StringType(), True),
                StructField("PaymentStatus", StringType(), True),
                # Transformed columns
                StructField("OrderDetailStatus", StringType(), True),
                StructField("LineTotal", DecimalType(10,2), True),
                StructField("DiscountAmount", DecimalType(10,2), True),
                StructField("NetAmount", DecimalType(10,2), True),
                StructField("PriceCategory", StringType(), True),
                StructField("IsHighValue", BooleanType(), True),
                StructField("IsDiscounted", BooleanType(), True),
                StructField("WarehouseEfficiency", DoubleType(), True),
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
        maps = load_config('/opt/airflow/dags/config/orderdetail_status_mapping.yaml', {
            'orderdetail_status_mapping': {
                '1': 'Pending',
                '2': 'Processing',
                '3': 'Shipped',
                '4': 'Delivered',
                '5': 'Cancelled'
            },
            'product_category_mapping': {}
        })
        status_map = create_map([lit(x) for c in maps['orderdetail_status_mapping'].items() for x in c])
        cat_map = create_map([lit(x) for c in maps['product_category_mapping'].items() for x in c])

        df = df.fillna({'ProductName':'Unknown Product','ProductCategory':'Unknown Category','WarehouseName':'Unknown Warehouse'}).withColumn("OrderDetailStatus", coalesce(status_map[col("Status")], col("Status"))).withColumn("ProductCategory", coalesce(cat_map[col("ProductCategory")], col("ProductCategory")))
        df = df.withColumn("LineTotal", (col("Quantity")*col("UnitPrice")).cast(DecimalType(10,2))).withColumn("DiscountAmount", (col("LineTotal")*col("Discount")/100).cast(DecimalType(10,2))).withColumn("NetAmount", (col("LineTotal")-col("DiscountAmount")).cast(DecimalType(10,2)))
        df = df.withColumn("PriceCategory", when(col("UnitPrice")>=1000,"Premium").when(col("UnitPrice")>=500,"High").otherwise("Medium")).withColumn("IsHighValue", when(col("NetAmount")>=10000,True).otherwise(False)).withColumn("IsDiscounted", when(col("Discount") > 0, True).otherwise(False))
        win = Window.partitionBy("WarehouseName")
        df = df.withColumn("WarehouseEfficiency", (spark_sum(when(col("OrderDetailStatus")=='Delivered',1).otherwise(0)).over(win)/spark_count(lit(1)).over(win))*100)
        df = df.withColumn("DataQualityScore", lit(100)-when(col('ProductName')=='Unknown Product',15).otherwise(0)-when(col('ProductCategory')=='Unknown Category',10).otherwise(0))
        df = df.withColumn("DataQualityLevel", when(col("DataQualityScore")<=70,"Poor").when(col("DataQualityScore")<=85,"Fair").otherwise("Good"))
        df = df.withColumn('etl_created_date',current_timestamp()).withColumn('etl_batch_id',lit(context['ds_nodash'])).withColumn('is_empty_partition', lit(False))
        logging.info("✅ Transformation complete.")

        # 添加数据去重检查，确保OrderDetailID唯一性
        logging.info("Performing data deduplication check...")
        initial_count = df.count()
        df = df.dropDuplicates(['OrderDetailID'])
        final_count = df.count()
        
        if initial_count != final_count:
            duplicate_count = initial_count - final_count
            logging.warning(f"⚠️ Found and removed {duplicate_count} duplicate records based on OrderDetailID")
            logging.warning(f"Record count: {initial_count} → {final_count}")
        else:
            logging.info("✅ No duplicate records found.")
            
        record_count = final_count  # 更新记录数

        logging.info("Calculating statistics...")
        stats = df.agg(avg("UnitPrice").alias("avg_price"), spark_sum(when(col("IsHighValue"),1).otherwise(0)).alias("high_value_items")).collect()[0]
        qual_dist = {r['DataQualityLevel']:r['count'] for r in df.groupBy('DataQualityLevel').count().collect()}
        
        # 添加重复数据检查统计信息
        dedup_stats = {
            'initial_records': initial_count,
            'final_records': final_count,
            'duplicates_removed': initial_count - final_count,
            'deduplication_rate': (initial_count - final_count) / initial_count if initial_count > 0 else 0
        }
        
        transform_stats = {
            'total_records': record_count, 
            'avg_unit_price': stats['avg_price'], 
            'high_value_items': stats['high_value_items'], 
            'quality_distribution': qual_dist,
            'dedup_stats': dedup_stats
        }
        context['task_instance'].xcom_push(key='transform_stats', value=transform_stats)
        context['task_instance'].xcom_push(key='dedup_stats', value=dedup_stats)

        logging.info("Loading data to DWD layer...")
        table_name = "dwd_db.dwd_orderdetails"
        location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orderdetails"
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd_db")
        
        # 强化的元数据同步和缓存清理
        def cleanup_metadata_and_cache_orderdetails(table_name, batch_date, location):
            """清理 OrderDetails 表的元数据和缓存，确保数据一致性"""
            try:
                # 1. 清除所有相关缓存
                spark.catalog.uncacheTable(table_name)
                logging.info(f"清除表缓存: {table_name}")
            except:
                pass
            
            try:
                # 2. 检查表是否存在以及 schema 是否正确
                existing_table = spark.table(table_name)
                existing_columns = [field.name for field in existing_table.schema.fields]
                has_empty_partition_column = 'is_empty_partition' in existing_columns
                
                if not has_empty_partition_column:
                    logging.warning(f"表 {table_name} 缺少 is_empty_partition 列，需要重新创建")
                    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                    logging.info(f"✅ 删除表 {table_name} 以更新 schema")
                    return  # 表已删除，无需进一步清理分区
                    
            except Exception as e:
                logging.info(f"表 {table_name} 不存在或无法访问: {e}")
                return  # 表不存在，无需清理
            
            try:
                # 3. 刷新表元数据
                spark.sql(f"REFRESH TABLE {table_name}")
                logging.info(f"刷新表元数据: {table_name}")
            except Exception as e:
                logging.warning(f"刷新表元数据失败: {e}")
            
            try:
                # 4. 检查并清理无效分区
                existing_partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
                partition_to_check = f"dt={batch_date}"
                
                # 检查分区是否存在于元数据中
                partition_exists_in_metadata = any(partition_to_check in str(p) for p in existing_partitions)
                
                if partition_exists_in_metadata:
                    logging.info(f"发现分区 {partition_to_check} 在元数据中，准备清理")
                    
                    # 删除分区元数据
                    spark.sql(f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION (dt='{batch_date}')")
                    logging.info(f"删除分区元数据: dt='{batch_date}'")
                    
                    # 删除 HDFS 目录
                    partition_path = f"{location}/dt={batch_date}"
                    try:
                        # 使用 Hadoop 文件系统 API 删除
                        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(partition_path)
                        if fs.exists(path):
                            fs.delete(path, True)  # True 表示递归删除
                            logging.info(f"删除 HDFS 分区目录: {partition_path}")
                        else:
                            logging.info(f"HDFS 分区目录不存在: {partition_path}")
                    except Exception as hdfs_e:
                        logging.warning(f"删除 HDFS 分区目录失败: {hdfs_e}")
                        # 备用方法：使用 Spark SQL 删除
                        try:
                            spark.sql(f"dfs -rm -r -f {partition_path}")
                            logging.info(f"使用 Spark SQL 删除 HDFS 目录: {partition_path}")
                        except Exception as sql_e:
                            logging.warning(f"Spark SQL 删除也失败: {sql_e}")
                
            except Exception as e:
                logging.warning(f"分区清理过程出错: {e}")
        
        # 执行清理
        cleanup_metadata_and_cache_orderdetails(table_name, batch_date, location)
        
        # Use a more robust write approach
        df_with_partition = df.withColumn('dt', lit(batch_date))
        
        # Write the data using dynamic partition overwrite.
        # With 'partitionOverwriteMode' set to 'dynamic', this will only overwrite the specific partition
        # for the given batch_date, which is safer and more efficient.
        df_with_partition.write \
          .mode("overwrite") \
          .partitionBy("dt") \
          .format("parquet") \
          .option("path", location) \
          .saveAsTable(table_name)

        # 强化的元数据刷新和同步
        def refresh_orderdetails_metadata(table_name, batch_date):
            """执行全面的 OrderDetails 表元数据刷新"""
            try:
                # 1. MSCK REPAIR TABLE - 重新同步分区
                spark.sql(f"MSCK REPAIR TABLE {table_name}")
                logging.info(f"✅ MSCK REPAIR 成功: {table_name}")
            except Exception as e:
                logging.warning(f"⚠️ MSCK REPAIR 失败: {e}")
            
            try:
                # 2. REFRESH TABLE - 刷新表元数据
                spark.sql(f"REFRESH TABLE {table_name}")
                logging.info(f"✅ 表刷新成功: {table_name}")
            except Exception as e:
                logging.warning(f"⚠️ 表刷新失败: {e}")
            
            try:
                # 3. 清除全局缓存
                spark.catalog.clearCache()
                logging.info("✅ 全局缓存清除成功")
            except Exception as e:
                logging.warning(f"⚠️ 全局缓存清除失败: {e}")
            
            # 4. 验证分区是否正确创建
            try:
                partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
                current_partitions = [p['partition'] for p in partitions]
                expected_partition = f"dt={batch_date}"
                
                if any(expected_partition in partition for partition in current_partitions):
                    logging.info(f"✅ 分区验证成功: {expected_partition}")
                    
                    # 验证数据可访问性
                    test_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name} WHERE dt='{batch_date}'").collect()[0]['cnt']
                    logging.info(f"✅ 数据验证成功，分区 dt={batch_date} 记录数: {test_count}")
                    
                    # 额外验证：检查关键字段
                    sample_data = spark.sql(f"SELECT OrderDetailID, ProductID, OrderID FROM {table_name} WHERE dt='{batch_date}' LIMIT 3").collect()
                    logging.info(f"✅ 数据样本验证: {len(sample_data)} 条记录")
                    
                else:
                    logging.warning(f"⚠️ 预期分区 {expected_partition} 未找到")
                    logging.info(f"当前分区列表: {current_partitions}")
                    
            except Exception as e:
                logging.warning(f"⚠️ 分区验证失败: {e}")
        
        # 执行元数据刷新
        refresh_orderdetails_metadata(table_name, batch_date)

        df.unpersist()
        logging.info("✅ Data loaded successfully.")

        summary = {'total_records':record_count, 'partitions':[{'dt':batch_date, 'path':location}]}
        context['task_instance'].xcom_push(key='hdfs_load_summary', value=summary)
        context['task_instance'].xcom_push(key='table_name', value=table_name)
        context['task_instance'].xcom_push(key='status', value='SUCCESS')

    except Exception as e:
        logging.error(f"ETL failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()


def create_orderdetails_hive_views(**context):
    status = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='status')
    if status in ['SKIPPED_EMPTY_DATA', 'SUCCESS_EMPTY_PARTITION']:
        logging.warning(f"Skipping view creation as status is {status}.")
        return

    from pyspark.sql import SparkSession
    spark = None
    try:
        spark = SparkSession.builder.appName("CreateDWDViews") \
            .config("spark.sql.catalogImplementation","hive") \
            .config("spark.hadoop.hive.metastore.uris","thrift://hive-metastore:9083") \
            .config("spark.sql.parquet.cacheMetadata", "false") \
            .config("spark.sql.hive.metastorePartitionPruning", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .enableHiveSupport().getOrCreate()
        spark.sql("USE dwd_db")
        
        # Refresh table metadata before checking schema
        try:
            spark.sql("REFRESH TABLE dwd_orderdetails")
            spark.catalog.clearCache()
        except Exception as e:
            logging.warning(f"Failed to refresh table metadata: {e}")
        
        # Check if the table has transformed columns and empty partition flag
        try:
            table_columns = [col.name for col in spark.table("dwd_orderdetails").schema.fields]
            has_transformed_columns = all(col in table_columns for col in ['IsHighValue', 'DataQualityLevel', 'IsDiscounted', 'OrderDetailStatus'])
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
        
        views = [
            # 1. 高价值订单明细视图
            f"""
            CREATE OR REPLACE VIEW dwd_orderdetails_high_value AS
            SELECT *
            FROM dwd_orderdetails
            WHERE IsHighValue = true
              AND DataQualityLevel IN ('Good', 'Excellent'){empty_partition_filter}
            """,
            
            # 2. 折扣商品视图
            f"""
            CREATE OR REPLACE VIEW dwd_orderdetails_discounted AS
            SELECT *
            FROM dwd_orderdetails
            WHERE IsDiscounted = true
              AND Discount > 0{empty_partition_filter}
            """,
            
            # 3. 产品销售统计视图
            f"""
            CREATE OR REPLACE VIEW dwd_orderdetails_product_summary AS
            SELECT 
                ProductID,
                ProductName,
                ProductCategory,
                COUNT(*) as order_count,
                SUM(Quantity) as total_quantity,
                SUM(NetAmount) as total_amount,
                AVG(UnitPrice) as avg_unit_price,
                AVG(Discount) as avg_discount
            FROM dwd_orderdetails
            WHERE 1=1{empty_partition_filter}
            GROUP BY ProductID, ProductName, ProductCategory
            """,
            
            # 4. 仓库效率视图
            f"""
            CREATE OR REPLACE VIEW dwd_orderdetails_warehouse_performance AS
            SELECT 
                WarehouseID,
                WarehouseName,
                FactoryName,
                COUNT(*) as total_items,
                SUM(CASE WHEN OrderDetailStatus = 'Delivered' THEN 1 ELSE 0 END) as delivered_items,
                AVG(WarehouseEfficiency) as efficiency_score
            FROM dwd_orderdetails
            WHERE 1=1{empty_partition_filter}
            GROUP BY WarehouseID, WarehouseName, FactoryName
            """
        ]
        for v in views: spark.sql(v)
        logging.info(f"✅ Successfully created {len(views)} views.")
    except Exception as e:
        logging.error(f"View creation failed: {e}", exc_info=True)
        raise
    finally:
        if spark: spark.stop()


def validate_orderdetails_dwd(**context):
    """Validates the DWD OrderDetails data quality including duplicate checks."""
    status = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='status')
    if status == 'SKIPPED_EMPTY_DATA':
        logging.warning("Skipping validation as no data was processed.")
        return
    elif status == 'SUCCESS_EMPTY_PARTITION':
        logging.info("✅ Empty partition validation passed.")
        return

    stats = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='transform_stats')
    summary = context['task_instance'].xcom_pull(task_ids='run_dwd_orderdetails_etl_task', key='hdfs_load_summary')
    dedup_stats = stats.get('dedup_stats', {}) if stats else {}
    
    issues = []
    warnings = []
    
    # 检查重复数据统计
    if dedup_stats:
        duplicates_removed = dedup_stats.get('duplicates_removed', 0)
        dedup_rate = dedup_stats.get('deduplication_rate', 0)
        
        if duplicates_removed > 0:
            warnings.append(f"Removed {duplicates_removed} duplicate OrderDetail records (dedup rate: {dedup_rate:.2%})")
            logging.info(f"📊 OrderDetails deduplication stats: {dedup_stats}")
    
    # 原有的验证逻辑
    if stats and summary:
        if stats['total_records'] != summary['total_records']:
            issues.append("Record count mismatch between transform and load stages.")
                
        if stats['total_records'] > 0:
            poor_quality_ratio = stats['quality_distribution'].get('Poor', 0) / stats['total_records']
            if poor_quality_ratio > 0.1:
                issues.append(f"High ratio of poor quality data: {poor_quality_ratio:.2%}")

    if issues:
        logging.warning(f"❌ Validation issues found: {issues}")
    if warnings:
        logging.warning(f"⚠️ Validation warnings: {warnings}")
    if not issues and not warnings:
        logging.info("✅ Data validation passed.")
    elif not issues:
        logging.info("✅ Data validation passed with warnings.")


with DAG(
    'dwd_orderdetails_pipeline',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['dwd', 'orderdetails', 'refactored'],
    description='Refactored DWD OrderDetails ETL pipeline using a single Spark job.',
) as dag:

    start = DummyOperator(task_id='start')

    etl_task = PythonOperator(
        task_id='run_dwd_orderdetails_etl_task',
        python_callable=run_dwd_orderdetails_etl
    )

    create_views = PythonOperator(
        task_id='create_hive_views',
        python_callable=create_orderdetails_hive_views
    )

    validate_dwd = PythonOperator(
        task_id='validate_dwd_data',
        python_callable=validate_orderdetails_dwd
    )

    end = DummyOperator(task_id='end')

    start >> etl_task >> create_views >> validate_dwd >> end