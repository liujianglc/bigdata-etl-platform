import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, max as spark_max, lit, when
from pyspark.sql.types import *
import logging
from datetime import datetime, timedelta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True, help="MySQL JDBC URL")
    parser.add_argument("--username", required=True, help="MySQL username")
    parser.add_argument("--password", required=True, help="MySQL password")
    parser.add_argument("--source-table", required=True, help="Source MySQL table")
    parser.add_argument("--hdfs-path", required=True, help="HDFS storage path")
    parser.add_argument("--hive-db", required=True, help="Hive database name")
    parser.add_argument("--hive-table", required=True, help="Hive table name")
    parser.add_argument("--incremental-column", default="updated_at", help="Column for incremental load")
    parser.add_argument("--full-load", action="store_true", help="Force full load instead of incremental")
    parser.add_argument("--lookback-days", type=int, default=1, help="Days to look back for incremental load")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    spark = SparkSession.builder \
        .appName("IncrementalMySQLToHiveETL") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        logger.info(f"Starting {'FULL' if args.full_load else 'INCREMENTAL'} ETL for table: {args.source_table}")
        
        # 创建 Hive 数据库
        logger.info(f"Creating Hive database: {args.hive_db}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")
        
        # 获取增量读取的时间戳
        last_update_time = None
        if not args.full_load:
            try:
                # 检查目标表是否存在
                existing_tables = spark.sql(f"SHOW TABLES IN {args.hive_db}").collect()
                table_exists = any(row['tableName'] == args.hive_table for row in existing_tables)
                
                if table_exists:
                    # 获取最后更新时间
                    max_time_df = spark.sql(f"""
                        SELECT MAX({args.incremental_column}) as max_time 
                        FROM {args.hive_db}.{args.hive_table}
                    """)
                    max_time_row = max_time_df.collect()[0]
                    last_update_time = max_time_row['max_time']
                    
                    if last_update_time:
                        # 减去回溯天数以避免丢失数据
                        if isinstance(last_update_time, datetime):
                            last_update_time = last_update_time - timedelta(days=args.lookback_days)
                        logger.info(f"Last update time: {last_update_time}")
                    else:
                        logger.info("No previous data found, performing full load")
                        args.full_load = True
                else:
                    logger.info("Target table doesn't exist, performing full load")
                    args.full_load = True
            
            except Exception as e:
                logger.warning(f"Failed to get last update time: {e}, performing full load")
                args.full_load = True

        # 构建 MySQL 查询
        if args.full_load or last_update_time is None:
            mysql_query = f"(SELECT * FROM {args.source_table}) as t"
            logger.info("Performing FULL load")
        else:
            mysql_query = f"""(
                SELECT * FROM {args.source_table} 
                WHERE {args.incremental_column} > '{last_update_time}'
            ) as t"""
            logger.info(f"Performing INCREMENTAL load since {last_update_time}")

        # 从 MySQL 读取数据
        logger.info("Reading data from MySQL...")
        df = spark.read.format("jdbc") \
            .option("url", args.jdbc_url) \
            .option("dbtable", mysql_query) \
            .option("user", args.username) \
            .option("password", args.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("fetchsize", "10000") \
            .option("numPartitions", "4") \
            .load()

        record_count = df.count()
        logger.info(f"Read {record_count} rows from MySQL")

        if record_count == 0:
            logger.info("No new data to process")
            return

        # 数据处理和质量检查
        logger.info("Processing and validating data...")
        processed_df = df.withColumn("etl_processing_date", current_date()) \
                        .withColumn("etl_processing_timestamp", lit(datetime.now()))

        # 数据质量检查
        null_count = processed_df.filter(col("id").isNull()).count()
        if null_count > 0:
            logger.warning(f"Found {null_count} records with null IDs")

        # 写入 HDFS（分区存储）
        hdfs_partition_path = f"{args.hdfs_path}/year={datetime.now().year}/month={datetime.now().month:02d}/day={datetime.now().day:02d}"
        logger.info(f"Writing data to HDFS: {hdfs_partition_path}")
        
        processed_df.write \
            .mode("append" if not args.full_load else "overwrite") \
            .option("compression", "snappy") \
            .partitionBy("etl_processing_date") \
            .parquet(args.hdfs_path)

        # 更新 Hive 表
        logger.info(f"Updating Hive table: {args.hive_db}.{args.hive_table}")
        
        if args.full_load:
            # 全量加载：重建表
            processed_df.write \
                .mode("overwrite") \
                .option("path", args.hdfs_path) \
                .saveAsTable(f"{args.hive_db}.{args.hive_table}")
        else:
            # 增量加载：追加数据
            processed_df.write \
                .mode("append") \
                .insertInto(f"{args.hive_db}.{args.hive_table}")

        # 刷新表统计信息
        spark.sql(f"ANALYZE TABLE {args.hive_db}.{args.hive_table} COMPUTE STATISTICS")

        logger.info(f"ETL completed successfully. Processed {record_count} records.")

        # 返回处理统计信息
        stats = {
            "records_processed": record_count,
            "load_type": "FULL" if args.full_load else "INCREMENTAL",
            "processing_timestamp": datetime.now().isoformat(),
            "hdfs_path": args.hdfs_path,
            "hive_table": f"{args.hive_db}.{args.hive_table}"
        }
        
        logger.info(f"ETL Statistics: {stats}")

    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()