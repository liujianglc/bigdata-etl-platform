from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging

CONFIG_PATH = "/opt/airflow/config/mysql_to_hive_config.yaml"  # 放在挂载目录中
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),  # 30分钟超时
    'sla': timedelta(minutes=45),  # SLA 45分钟
}

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def check_environment():
    """检查环境是否准备就绪"""
    import socket
    import time
    
    services_to_check = [
        ("namenode", 9000, "HDFS Namenode"),
        ("hive-metastore", 9083, "Hive Metastore"),
        ("spark-master", 7077, "Spark Master")
    ]
    
    for host, port, service_name in services_to_check:
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                result = sock.connect_ex((host, port))
                sock.close()
                
                if result == 0:
                    logging.info(f"✅ {service_name} ({host}:{port}) 连接成功")
                    break
                else:
                    if attempt < max_attempts - 1:
                        logging.warning(f"⚠️ {service_name} ({host}:{port}) 连接失败，等待重试...")
                        time.sleep(5)
                    else:
                        logging.error(f"❌ {service_name} ({host}:{port}) 连接失败")
                        raise Exception(f"无法连接到 {service_name}")
                        
            except Exception as e:
                if attempt < max_attempts - 1:
                    logging.warning(f"⚠️ 检查 {service_name} 时出错: {e}，等待重试...")
                    time.sleep(5)
                else:
                    logging.error(f"❌ 检查 {service_name} 时出错: {e}")
                    raise

def create_spark():
    import os
    import subprocess
    
    # 设置环境变量
    os.environ['JAVA_HOME'] = '/usr/lib/jvm/default-java'
    os.environ['SPARK_HOME'] = '/home/airflow/.local/lib/python3.8/site-packages/pyspark'
    
    # 检查Java是否可用
    try:
        java_version = subprocess.check_output(['java', '-version'], stderr=subprocess.STDOUT, text=True)
        logging.info(f"Java版本检查通过: {java_version.split()[0] if java_version else 'Unknown'}")
    except Exception as e:
        logging.error(f"Java版本检查失败: {e}")
        raise Exception("Java环境不可用")
    
    # 检查是否在容器环境中运行
    is_container = os.path.exists('/.dockerenv')
    
    # 设置JVM参数以避免内存问题
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 512m --executor-memory 512m pyspark-shell'
    
    builder = SparkSession.builder.appName("MySQL to Hive Sync")
    
    if is_container:
        # 容器环境：使用本地模式但连接到远程服务
        logging.info("检测到容器环境，使用本地Spark模式连接远程服务")
        builder = builder.master("local[2]")  # 限制为2个核心避免资源问题
    else:
        # 本地环境：使用本地模式
        logging.info("检测到本地环境，使用本地Spark模式")
        builder = builder.master("local[2]")
    
    # 通用配置 - 简化配置避免复杂性
    try:
        spark = builder \
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.driver.memory", "512m") \
            .config("spark.driver.maxResultSize", "256m") \
            .config("spark.network.timeout", "300s") \
            .config("spark.rpc.askTimeout", "300s") \
            .config("spark.rpc.lookupTimeout", "300s") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logging.info("✅ Spark会话创建成功")
        return spark
        
    except Exception as e:
        logging.error(f"创建Spark会话时出错: {e}")
        # 尝试更简单的配置
        logging.info("尝试使用最小配置创建Spark会话...")
        try:
            spark = SparkSession.builder \
                .appName("MySQL to Hive Sync - Minimal") \
                .master("local[1]") \
                .config("spark.driver.memory", "256m") \
                .config("spark.driver.maxResultSize", "128m") \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
                .enableHiveSupport() \
                .getOrCreate()
            
            logging.info("✅ 使用最小配置创建Spark会话成功")
            return spark
            
        except Exception as e2:
            logging.error(f"最小配置也失败: {e2}")
            raise

def create_hive_databases():
    """预创建所有需要的Hive数据库"""
    spark = None
    max_retries = 3
    retry_delay = 30  # 30秒
    
    # 首先检查环境
    try:
        logging.info("检查环境连接状态...")
        check_environment()
        logging.info("✅ 环境检查通过")
    except Exception as e:
        logging.error(f"❌ 环境检查失败: {e}")
        raise
    
    for attempt in range(max_retries):
        try:
            logging.info(f"尝试创建Spark会话 (第 {attempt + 1}/{max_retries} 次)")
            spark = create_spark()
            logging.info("✅ Spark会话创建成功")
            
            config = load_config()
            
            # 收集所有需要的数据库名称
            databases = set()
            for table_conf in config['tables']:
                if table_conf.get('enabled', True):
                    hive_table = table_conf['hive_table']
                    if '.' in hive_table:
                        db_name = hive_table.split('.')[0]
                        databases.add(db_name)
            
            logging.info(f"需要创建的数据库: {databases}")
            
            # 创建所有需要的数据库
            for db_name in databases:
                logging.info(f"正在创建Hive数据库: {db_name}")
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                logging.info(f"✅ 数据库 {db_name} 创建成功")
            
            # 验证数据库创建结果
            result = spark.sql("SHOW DATABASES").collect()
            existing_databases = [row[0] for row in result]
            logging.info(f"当前所有数据库: {existing_databases}")
            
            for db_name in databases:
                if db_name in existing_databases:
                    logging.info(f"✅ 验证通过: 数据库 {db_name} 存在")
                else:
                    logging.error(f"❌ 验证失败: 数据库 {db_name} 不存在")
                    raise Exception(f"数据库 {db_name} 创建失败")
            
            # 如果成功，跳出重试循环
            break
            
        except Exception as e:
            logging.error(f"❌ 第 {attempt + 1} 次尝试失败: {e}")
            
            if spark:
                try:
                    spark.stop()
                    logging.info("已关闭失败的Spark会话")
                except Exception as stop_e:
                    logging.warning(f"关闭Spark会话时出现警告: {stop_e}")
                spark = None
            
            if attempt < max_retries - 1:
                logging.info(f"等待 {retry_delay} 秒后重试...")
                import time
                time.sleep(retry_delay)
            else:
                logging.error("所有重试都失败了，抛出异常")
                raise
    
    # 如果所有重试都成功，确保清理资源
    if spark:
        try:
            spark.stop()
            logging.info("Spark会话已关闭")
        except Exception as e:
            logging.warning(f"关闭Spark会话时出现警告: {e}")

def sync_table(table_conf, exec_date):
    spark = None
    try:
        spark = create_spark()
        config = load_config()
        mysql = config['mysql']
        mysql_url = mysql['url']
        properties = {
            "user": mysql['user'],
            "password": mysql['password'],
            "driver": mysql['driver'],
            "connectTimeout": "60000",  # 60 seconds
            "socketTimeout": "300000",  # 5 minutes
            "autoReconnect": "true",
            "useSSL": "false",
            "allowPublicKeyRetrieval": "true",
            "fetchsize": "10000"  # 移到properties中
        }

        mysql_table = table_conf['mysql_table']
        hive_table = table_conf['hive_table']
        inc_col = table_conf.get('incremental_column', 'update_time')
        part_col = table_conf.get('partition_column', 'dt')
        sync_mode = table_conf.get('sync_mode', 'incremental')
        
        logging.info(f"开始同步表: {mysql_table} -> {hive_table}")
        logging.info(f"同步模式: {sync_mode}, 分区列: {part_col}")
        
        # 确保Hive数据库存在
        hive_db = hive_table.split('.')[0] if '.' in hive_table else 'default'
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
        spark.sql(f"USE {hive_db}")
        logging.info(f"Hive数据库 {hive_db} 已准备就绪")

        # 检查表是否存在
        table_exists = False
        try:
            spark.sql(f"DESCRIBE {hive_table}")
            table_exists = True
            logging.info(f"Hive表 {hive_table} 已存在")
        except Exception:
            table_exists = False
            logging.info(f"Hive表 {hive_table} 不存在，将创建新表")
        
        # 构建增量查询条件
        where_clause = ""
        if sync_mode == 'incremental' and table_exists:
            try:
                result = spark.sql(f"SELECT MAX({inc_col}) FROM {hive_table}").collect()
                if result and result[0][0] is not None:
                    last_ts = result[0][0]
                    where_clause = f" WHERE {inc_col} > '{last_ts}'"
                    logging.info(f"增量同步：从 {last_ts} 之后开始")
                else:
                    logging.info("增量同步：表为空，执行全量同步")
            except Exception as e:
                logging.warning(f"无法获取增量时间戳，执行全量同步: {e}")
                where_clause = ""
        else:
            logging.info("执行全量同步")

        # 从MySQL读取数据 - 添加分页和超时控制
        query = f"(SELECT * FROM {mysql_table}{where_clause}) AS tmp"
        logging.info(f"执行MySQL查询: SELECT * FROM {mysql_table}{where_clause}")
        
        # 添加分区读取配置以提高性能和避免超时
        df = spark.read.jdbc(
            mysql_url, 
            query, 
            properties=properties,
            numPartitions=4  # 分4个分区并行读取
        )
        
        # 使用缓存避免重复计算
        df.cache()
        record_count = df.count()
        logging.info(f"从MySQL读取到 {record_count} 条记录")
        
        if record_count == 0:
            logging.info("没有数据需要同步")
            # 清理缓存
            df.unpersist()
            return

        # 添加分区列
        if part_col:
            df = df.withColumn(part_col, lit(exec_date))
            logging.info(f"添加分区列 {part_col} = {exec_date}")

        # HDFS 表位置
        table_location = f"hdfs://namenode:9000/user/hive/warehouse/{hive_db}.db/{hive_table.split('.')[-1]}"
        logging.info(f"表将存储在 HDFS 位置: {table_location}")

        # 写入Hive表 - 优化写入性能
        write_options = {
            "path": table_location,
            "compression": "snappy"  # 使用snappy压缩提高性能
        }
        
        if table_exists and sync_mode == 'incremental':
            # 增量写入已存在的表
            logging.info("增量写入已存在的表...")
            if part_col:
                # 对于分区表，先删除当天分区再写入，避免重复数据
                partition_spec = f"{part_col}='{exec_date}'"
                try:
                    spark.sql(f"ALTER TABLE {hive_table} DROP IF EXISTS PARTITION ({partition_spec})")
                    logging.info(f"删除已存在的分区: {partition_spec}")
                except Exception as e:
                    logging.warning(f"删除分区时出现警告: {e}")
                
                # 使用指定路径写入，添加重分区优化
                df.coalesce(2).write.mode("append").partitionBy(part_col).options(**write_options).saveAsTable(hive_table)
            else:
                # 对于非分区表，使用saveAsTable append模式
                df.coalesce(2).write.mode("append").options(**write_options).saveAsTable(hive_table)
        else:
            # 全量写入或创建新表
            logging.info("创建新表或全量覆盖...")
            if part_col:
                df.coalesce(2).write.mode("overwrite").partitionBy(part_col).options(**write_options).saveAsTable(hive_table)
            else:
                df.coalesce(2).write.mode("overwrite").options(**write_options).saveAsTable(hive_table)
        
        # 清理缓存
        df.unpersist()
        
        # 刷新表元数据以确保文件可见
        spark.sql(f"REFRESH TABLE {hive_table}")
        logging.info(f"已刷新表元数据: {hive_table}")
        
        # 验证写入结果
        try:
            final_count = spark.sql(f"SELECT COUNT(*) FROM {hive_table}").collect()[0][0]
            logging.info(f"✅ 写入成功！表 {hive_table} 中共有 {final_count} 条记录")
            
            # 如果是分区表，显示分区信息
            if part_col:
                try:
                    partitions = spark.sql(f"SHOW PARTITIONS {hive_table}").collect()
                    logging.info(f"表分区信息: {[row[0] for row in partitions[-3:]]}")  # 显示最后3个分区
                except Exception as part_e:
                    logging.warning(f"无法获取分区信息: {part_e}")
                    
        except Exception as count_e:
            logging.error(f"❌ 无法验证写入结果: {count_e}")
            # 尝试简单的表存在性检查
            try:
                spark.sql(f"DESCRIBE {hive_table}")
                logging.info("✅ 表结构验证通过 - 表已创建")
            except Exception as desc_e:
                logging.error(f"❌ 表创建可能失败: {desc_e}")
                raise
        
        # 显示表结构（仅对新表）
        if not table_exists:
            logging.info(f"表 {hive_table} 的结构:")
            schema_info = spark.sql(f"DESCRIBE {hive_table}").collect()
            for row in schema_info:
                logging.info(f"  {row[0]}: {row[1]}")
        
        # 显示表的详细信息包括位置
        try:
            table_info = spark.sql(f"DESCRIBE EXTENDED {hive_table}").collect()
            for row in table_info:
                if 'Location' in str(row[0]) or 'location' in str(row[0]):
                    logging.info(f"表存储位置: {row[1]}")
                    break
        except Exception as e:
            logging.warning(f"无法获取表位置信息: {e}")
            
        # 验证数据是否可以正常读取
        try:
            sample_data = spark.sql(f"SELECT * FROM {hive_table} LIMIT 1").collect()
            if sample_data:
                logging.info("✅ 数据验证通过 - 表可以正常读取")
            else:
                logging.warning("⚠️ 表存在但无数据")
        except Exception as e:
            logging.error(f"❌ 数据验证失败: {e}")
            # 不抛出异常，因为表可能已经创建成功，只是验证有问题
            logging.warning("继续执行，但建议检查表状态")
            
        # 验证 HDFS 中的文件（使用 Spark 的文件系统 API）
        try:
            from pyspark import SparkContext
            sc = spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            
            hdfs_path = sc._jvm.org.apache.hadoop.fs.Path(table_location)
            if fs.exists(hdfs_path):
                # 列出目录内容
                file_status = fs.listStatus(hdfs_path)
                parquet_files = []
                for status in file_status:
                    path_str = str(status.getPath())
                    if '.parquet' in path_str or status.isDirectory():
                        parquet_files.append(path_str)
                
                if parquet_files:
                    logging.info(f"✅ 在 HDFS 中找到数据文件/目录:")
                    for file_path in parquet_files[:5]:  # 只显示前5个
                        logging.info(f"  {file_path}")
                    if len(parquet_files) > 5:
                        logging.info(f"  ... 还有 {len(parquet_files) - 5} 个文件/目录")
                else:
                    logging.warning(f"⚠️ 在 HDFS {table_location} 中未找到数据文件")
            else:
                logging.warning(f"⚠️ HDFS 路径不存在: {table_location}")
        except Exception as e:
            logging.warning(f"无法验证 HDFS 中的文件: {e}")
            # 作为备选，尝试使用 Spark SQL 验证表位置
            try:
                table_info = spark.sql(f"DESCRIBE EXTENDED {hive_table}").collect()
                for row in table_info:
                    if 'Location' in str(row[0]):
                        actual_location = row[1]
                        logging.info(f"表实际存储位置: {actual_location}")
                        break
            except Exception as desc_e:
                logging.warning(f"也无法获取表位置信息: {desc_e}")
                
    except Exception as e:
        logging.error(f"❌ 同步失败: {e}")
        import traceback
        logging.error(traceback.format_exc())
        raise
    finally:
        # 确保Spark会话被正确关闭
        if spark:
            try:
                spark.stop()
                logging.info("Spark会话已关闭")
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def generate_tasks(dag, config):
    tasks = []
    exec_date = "{{ ds }}"  # execution_date 格式为 YYYY-MM-DD
    for table_conf in config['tables']:
        if not table_conf.get('enabled', True):
            continue

        task = PythonOperator(
            task_id=f"sync_{table_conf['mysql_table']}",
            python_callable=sync_table,
            op_kwargs={'table_conf': table_conf, 'exec_date': exec_date},
            dag=dag,
        )
        tasks.append(task)
    return tasks

# DAG 声明
with DAG(
    dag_id="sync_mysql_to_hive_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # 每天凌晨 2 点执行
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,  # 限制并发任务数
    tags=["sync", "mysql", "hive"],
) as dag:
    
    # 创建数据库任务
    create_databases_task = PythonOperator(
        task_id="create_hive_databases",
        python_callable=create_hive_databases,
        dag=dag,
    )
    
    # 加载配置并生成同步任务
    config = load_config()
    sync_tasks = generate_tasks(dag, config)
    
    # 设置任务依赖：先创建数据库，再执行同步任务
    if sync_tasks:
        create_databases_task >> sync_tasks
