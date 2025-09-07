#!/bin/bash

# 等待HDFS服务可用
sleep 10

# 创建HDFS目录并设置权限
hdfs dfsadmin -safemode wait

# 创建基本目录结构
hdfs dfs -mkdir -p /tmp
hdfs dfs -chmod -R 1777 /tmp

hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod -R 1777 /user/hive/warehouse

# 创建所有可能的数据库目录并设置宽松权限
hdfs dfs -mkdir -p /user/hive/warehouse/dws_db.db
hdfs dfs -mkdir -p /user/hive/warehouse/dwd_db.db
hdfs dfs -mkdir -p /user/hive/warehouse/ods_db.db

# 设置宽松的权限（开发环境）
hdfs dfs -chmod -R 777 /user/hive/warehouse/dws_db.db
hdfs dfs -chmod -R 777 /user/hive/warehouse/dwd_db.db
hdfs dfs -chmod -R 777 /user/hive/warehouse/ods_db.db

# 设置正确的所有者和组
hdfs dfs -chown -R airflow:supergroup /user/hive/warehouse

echo "HDFS目录初始化完成"