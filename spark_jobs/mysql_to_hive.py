import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col
import logging

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--hdfs-path", required=True)
    parser.add_argument("--hive-db", required=True)
    parser.add_argument("--hive-table", required=True)
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    spark = SparkSession.builder \
        .appName("MySQLToHiveETL") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        logger.info(f"Starting ETL for table: {args.table}")
        
        # 1. 从 MySQL 读取数据
        logger.info("Reading data from MySQL...")
        df = spark.read.format("jdbc") \
            .option("url", args.jdbc_url) \
            .option("dbtable", args.table) \
            .option("user", args.username) \
            .option("password", args.password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        logger.info(f"Read {df.count()} rows from MySQL")

        # 2. 数据处理（示例：添加处理日期）
        logger.info("Processing data...")
        processed_df = df.withColumn("processing_date", current_date())

        # 3. 写入 HDFS（Parquet 格式）
        logger.info(f"Writing data to HDFS: {args.hdfs_path}")
        processed_df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(args.hdfs_path)

        # 4. 创建 Hive 数据库和表
        logger.info(f"Creating Hive database: {args.hive_db}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")
        
        logger.info(f"Writing to Hive table: {args.hive_db}.{args.hive_table}")
        processed_df.write \
            .mode("overwrite") \
            .option("path", args.hdfs_path) \
            .saveAsTable(f"{args.hive_db}.{args.hive_table}")

        logger.info("ETL completed successfully")

    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()