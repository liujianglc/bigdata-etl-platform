#!/bin/bash

echo "=== Data Verification Tool ==="

echo ""
echo "1. Check MySQL source data:"
docker-compose exec mysql mysql -u etl_user -petl_pass -e "SELECT COUNT(*) as mysql_count FROM source_db.orders;"

echo ""
echo "2. Check HDFS directories:"
echo "Sales DB directory:"
docker-compose exec namenode hdfs dfs -ls /user/hive/warehouse/sales_db/ || echo "Directory not found"

echo ""
echo "Orders table directory:"
docker-compose exec namenode hdfs dfs -ls /user/hive/warehouse/sales_db/orders/ || echo "Orders directory not found"

echo ""
echo "3. Check if we can access Hive Metastore:"
docker-compose exec spark-master python -c "
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName('Test').config('spark.hadoop.hive.metastore.uris', 'thrift://hive-metastore:9083').enableHiveSupport().getOrCreate()
    print('✅ Hive Metastore connection successful')
    spark.sql('SHOW DATABASES').show()
    spark.stop()
except Exception as e:
    print('❌ Hive Metastore connection failed: ' + str(e))
"

echo ""
echo "4. Web UI Status:"
echo "- HDFS NameNode: http://localhost:9870"
echo "- Spark Master: http://localhost:8081"
echo "- Airflow: http://localhost:8080"

echo ""
echo "=== Verification completed ==="