from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import logging
import pendulum

# 设置时区
local_tz = pendulum.timezone("Asia/Shanghai")

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

def get_execution_date(**context):
    """
    获取执行日期的辅助函数
    
    使用 execution_date 是 Airflow 的最佳实践：
    - 保证数据分区与逻辑日期一致
    - 支持历史数据回填
    - 便于数据治理和监控
    """
    # 使用 execution_date（Airflow 标准做法和最佳实践）
    return context['execution_date'].strftime('%Y-%m-%d')

def check_environment():
    """检查环境是否准备就绪"""
    import socket
    import time
    
    services_to_check = [
        ("namenode", 9000, "HDFS Namenode"),
        ("datanode", 9864, "HDFS Datanode"),  # 添加datanode检查
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

def check_hdfs_health():
    """检查HDFS集群健康状态"""
    try:
        import subprocess
        import json
        
        # 检查HDFS状态
        result = subprocess.run([
            'hdfs', 'dfsadmin', '-report'
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            output = result.stdout
            logging.info("HDFS集群状态报告:")
            
            # 解析关键信息
            lines = output.split('\n')
            for line in lines:
                if 'Live datanodes' in line or 'Dead datanodes' in line or 'DFS Used%' in line:
                    logging.info(f"  {line.strip()}")
            
            # 检查是否有死节点
            if 'Dead datanodes (0)' not in output:
                logging.warning("⚠️ 检测到死的datanode，这可能导致数据读取问题")
                return False
            else:
                logging.info("✅ 所有datanode都正常运行")
                return True
        else:
            logging.error(f"HDFS状态检查失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error("HDFS状态检查超时")
        return False
    except Exception as e:
        logging.warning(f"无法执行HDFS状态检查: {e}")
        return False

def create_spark():
    import os
    import subprocess
    import shutil
    
    # 容器环境的Java路径（按优先级排序）
    container_java_paths = [
        '/usr/local/bin/java',  # Dockerfile中创建的符号链接
        '/usr/lib/jvm/default-java/bin/java',  # 默认安装路径
        '/usr/bin/java',  # 系统路径
    ]
    
    java_executable = None
    
    # 首先尝试容器环境的已知路径
    for java_path in container_java_paths:
        if os.path.exists(java_path) and os.access(java_path, os.X_OK):
            java_executable = java_path
            logging.info(f"在容器环境中找到Java: {java_path}")
            break
    
    # 如果还没找到，使用shutil.which作为备选
    if not java_executable:
        java_executable = shutil.which('java')
        if java_executable:
            logging.info(f"通过which命令找到Java: {java_executable}")
    
    if java_executable:
        try:
            # 使用绝对路径执行java命令
            java_version = subprocess.check_output([java_executable, '-version'], 
                                                 stderr=subprocess.STDOUT, 
                                                 text=True, 
                                                 timeout=10)
            version_line = java_version.split('\n')[0] if java_version else 'Unknown'
            logging.info(f"Java版本检查通过: {version_line}")
            logging.info(f"使用Java路径: {java_executable}")
            
            # 设置JAVA_HOME环境变量（容器环境已设置，但确保正确）
            if 'JAVA_HOME' not in os.environ or not os.environ['JAVA_HOME']:
                if java_executable == '/usr/local/bin/java':
                    # 符号链接，使用实际的JAVA_HOME
                    os.environ['JAVA_HOME'] = '/usr/lib/jvm/default-java'
                else:
                    # 计算JAVA_HOME
                    java_home = os.path.dirname(os.path.dirname(java_executable))
                    os.environ['JAVA_HOME'] = java_home
                logging.info(f"设置JAVA_HOME: {os.environ['JAVA_HOME']}")
            else:
                logging.info(f"使用现有JAVA_HOME: {os.environ['JAVA_HOME']}")
            
            # 确保PATH包含Java路径
            java_bin_dir = os.path.dirname(java_executable)
            current_path = os.environ.get('PATH', '')
            if java_bin_dir not in current_path:
                os.environ['PATH'] = f"{java_bin_dir}:{current_path}"
                logging.info(f"更新PATH包含Java路径: {java_bin_dir}")
                
        except subprocess.TimeoutExpired:
            logging.error("Java版本检查超时")
            raise Exception("Java环境检查超时")
        except subprocess.CalledProcessError as e:
            logging.error(f"Java版本检查失败: {e}")
            # 在容器环境中，即使版本检查失败，Java可能仍然可用
            logging.warning("Java版本检查失败，但在容器环境中继续尝试...")
        except Exception as e:
            logging.error(f"Java版本检查失败: {e}")
            logging.warning("Java版本检查失败，但在容器环境中继续尝试...")
    else:
        logging.error("找不到Java可执行文件")
        # 在容器环境中列出详细的调试信息
        logging.info("调试信息 - 检查容器中的Java安装:")
        debug_paths = ['/usr/lib/jvm/', '/usr/bin/', '/usr/local/bin/']
        for location in debug_paths:
            if os.path.exists(location):
                try:
                    contents = os.listdir(location)
                    java_related = [item for item in contents if 'java' in item.lower()]
                    if java_related:
                        logging.info(f"在 {location} 找到Java相关文件: {java_related}")
                        # 检查权限
                        for item in java_related:
                            full_path = os.path.join(location, item)
                            if os.path.isfile(full_path):
                                permissions = oct(os.stat(full_path).st_mode)[-3:]
                                logging.info(f"  {full_path} 权限: {permissions}")
                except Exception as e:
                    logging.warning(f"无法列出 {location}: {e}")
        
        # 检查环境变量
        logging.info(f"JAVA_HOME环境变量: {os.environ.get('JAVA_HOME', '未设置')}")
        logging.info(f"PATH环境变量: {os.environ.get('PATH', '未设置')}")
        
        raise Exception("Java环境不可用")
    
    # 设置Spark环境变量（容器环境已在Dockerfile中设置，但确保正确）
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = '/home/airflow/.local/lib/python3.8/site-packages/pyspark'
    logging.info(f"SPARK_HOME: {os.environ['SPARK_HOME']}")
    
    # 确认容器环境
    is_container = os.path.exists('/.dockerenv')
    logging.info(f"容器环境检测: {'是' if is_container else '否'}")
    
    # 容器环境的Spark配置
    if is_container:
        # 设置容器环境特定的JVM参数
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 512m --executor-memory 512m pyspark-shell'
        
        # 设置Spark临时目录（使用airflow用户可写的目录）
        spark_temp_dirs = ['/tmp/spark', '/tmp/spark-worker', '/opt/airflow/spark-temp']
        os.environ['SPARK_LOCAL_DIRS'] = ':'.join(spark_temp_dirs)
        os.environ['SPARK_WORKER_DIR'] = '/tmp/spark-worker'
        
        # 确保临时目录存在并设置正确权限
        for temp_dir in spark_temp_dirs:
            try:
                os.makedirs(temp_dir, exist_ok=True)
                # 在容器中使用更宽松的权限
                os.chmod(temp_dir, 0o755)
                logging.info(f"创建Spark临时目录: {temp_dir}")
            except Exception as e:
                logging.warning(f"无法创建临时目录 {temp_dir}: {e}")
        
        # 设置Python环境变量
        os.environ['PYSPARK_PYTHON'] = 'python3'
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
        
        logging.info("容器环境Spark配置完成")
    
    builder = SparkSession.builder.appName("MySQL to Hive Sync - Container")
    
    # 容器环境使用本地模式，但配置更保守
    logging.info("容器环境使用本地Spark模式")
    builder = builder.master("local[1]")  # 容器环境使用单核避免资源竞争
    
    # 容器环境优化配置
    try:
        spark = builder \
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.cores", "1") \
            .config("spark.network.timeout", "300s") \
            .config("spark.rpc.askTimeout", "300s") \
            .config("spark.rpc.lookupTimeout", "300s") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.local.dir", "/tmp/spark") \
            .config("spark.worker.dir", "/tmp/spark-worker") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .config("spark.sql.adaptive.skewJoin.enabled", "false") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.host", "localhost") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "2") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logging.info("✅ Spark会话创建成功")
        
        # 验证Spark会话
        spark_version = spark.version
        logging.info(f"Spark版本: {spark_version}")
        
        # 测试基本功能
        test_df = spark.range(1).select("id")
        test_count = test_df.count()
        logging.info(f"Spark基本功能测试通过，测试数据条数: {test_count}")
        
        return spark
        
    except Exception as e:
        logging.error(f"创建Spark会话时出错: {e}")
        import traceback
        logging.error(f"详细错误信息: {traceback.format_exc()}")
        
        # 尝试容器环境的最小配置
        logging.info("尝试使用容器环境最小配置创建Spark会话...")
        try:
            spark = SparkSession.builder \
                .appName("MySQL to Hive Sync - Container Minimal") \
                .master("local[1]") \
                .config("spark.driver.memory", "256m") \
                .config("spark.driver.maxResultSize", "128m") \
                .config("spark.executor.memory", "256m") \
                .config("spark.local.dir", "/tmp/spark") \
                .config("spark.worker.dir", "/tmp/spark-worker") \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .config("spark.driver.host", "localhost") \
                .config("spark.ui.enabled", "false") \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
                .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
                .enableHiveSupport() \
                .getOrCreate()
            
            logging.info("✅ 使用容器环境最小配置创建Spark会话成功")
            return spark
            
        except Exception as e2:
            logging.error(f"容器环境最小配置也失败: {e2}")
            logging.error(f"最小配置详细错误: {traceback.format_exc()}")
            
            # 最后尝试：不使用Hive支持的纯Spark会话
            logging.info("最后尝试：创建不带Hive支持的纯Spark会话...")
            try:
                spark = SparkSession.builder \
                    .appName("MySQL to Hive Sync - Pure Spark") \
                    .master("local[1]") \
                    .config("spark.driver.memory", "256m") \
                    .config("spark.executor.memory", "256m") \
                    .config("spark.local.dir", "/tmp/spark") \
                    .config("spark.ui.enabled", "false") \
                    .getOrCreate()
                
                logging.warning("⚠️ 创建了不带Hive支持的Spark会话，功能可能受限")
                return spark
                
            except Exception as e3:
                logging.error(f"所有配置都失败: {e3}")
                logging.error(f"纯Spark配置详细错误: {traceback.format_exc()}")
                raise Exception("无法创建任何Spark会话配置")

def cleanup_old_partitions(spark, table_name, partition_column, retention_days=30):
    """
    清理旧分区，保留指定天数的数据
    """
    try:
        from datetime import datetime, timedelta
        
        # 计算保留的最早日期
        cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
        
        # 获取所有分区
        partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
        
        deleted_count = 0
        for partition in partitions:
            partition_str = partition[0]  # 格式如 "dt=2025-08-01"
            
            if partition_column in partition_str:
                # 提取日期
                date_part = partition_str.split('=')[1]
                if date_part < cutoff_date:
                    try:
                        spark.sql(f"ALTER TABLE {table_name} DROP PARTITION ({partition_str})")
                        logging.info(f"删除旧分区: {table_name} {partition_str}")
                        deleted_count += 1
                    except Exception as e:
                        logging.warning(f"删除分区失败 {table_name} {partition_str}: {e}")
        
        if deleted_count > 0:
            logging.info(f"表 {table_name} 清理了 {deleted_count} 个旧分区")
        else:
            logging.info(f"表 {table_name} 没有需要清理的旧分区")
            
    except Exception as e:
        logging.warning(f"清理旧分区时出错 {table_name}: {e}")

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
        
        # 检查HDFS健康状态
        logging.info("检查HDFS集群健康状态...")
        hdfs_healthy = check_hdfs_health()
        if not hdfs_healthy:
            logging.warning("⚠️ HDFS集群可能存在问题，但继续执行")
        else:
            logging.info("✅ HDFS集群健康检查通过")
            
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

def sync_table(table_conf, **context):
    """
    同步单个表的函数
    现在使用 context 来动态获取执行日期
    """
    spark = None
    try:
        # 动态获取执行日期
        exec_date = get_execution_date(**context)
        logging.info(f"动态计算的执行日期: {exec_date}")
        
        # 也可以记录其他相关日期用于调试
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        current_date = datetime.now().strftime('%Y-%m-%d')
        logging.info(f"Airflow execution_date: {execution_date}")
        logging.info(f"当前实际日期: {current_date}")
        logging.info(f"使用的分区日期: {exec_date}")
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

        # 写入Hive表 - 根据同步模式和表配置优化写入策略
        write_options = {
            "path": table_location,
            "compression": "snappy",  # 使用snappy压缩提高性能
            "maxRecordsPerFile": "50000",  # 限制每个文件的记录数，避免大文件
            "parquet.block.size": "134217728"  # 128MB块大小，适合HDFS
        }
        
        # 获取全量备份策略配置
        full_backup_strategy = table_conf.get('full_backup_strategy', 'keep_history')  # 默认保留历史
        # 可选值: 'keep_history' (保留历史分区), 'latest_only' (只保留最新)
        
        if sync_mode == 'incremental':
            # 增量同步逻辑
            logging.info("执行增量同步...")
            if part_col:
                # 对于分区表，先删除当天分区再写入，避免重复数据
                partition_spec = f"{part_col}='{exec_date}'"
                try:
                    spark.sql(f"ALTER TABLE {hive_table} DROP IF EXISTS PARTITION ({partition_spec})")
                    logging.info(f"删除已存在的分区: {partition_spec}")
                except Exception as e:
                    logging.warning(f"删除分区时出现警告: {e}")
                
                # 增量写入新分区 - 使用更安全的写入策略
                df.repartition(2).write.mode("append").partitionBy(part_col).options(**write_options).saveAsTable(hive_table)
            else:
                # 对于非分区表，直接追加
                df.repartition(2).write.mode("append").options(**write_options).saveAsTable(hive_table)
                
        elif sync_mode == 'full':
            # 全量同步逻辑
            if full_backup_strategy == 'latest_only':
                # 策略1: 只保留最新数据（覆盖整个表）
                logging.info("全量同步 - 只保留最新数据（覆盖模式）...")
                if part_col:
                    # 即使有分区列，也覆盖整个表
                    df.repartition(2).write.mode("overwrite").options(**write_options).saveAsTable(hive_table)
                else:
                    df.repartition(2).write.mode("overwrite").options(**write_options).saveAsTable(hive_table)
                    
            else:  # 'keep_history' 或其他值
                # 策略2: 保留历史数据（按日期分区）
                logging.info("全量同步 - 保留历史数据（分区模式）...")
                if part_col:
                    # 删除当天分区，然后写入新的全量数据到当天分区
                    partition_spec = f"{part_col}='{exec_date}'"
                    try:
                        spark.sql(f"ALTER TABLE {hive_table} DROP IF EXISTS PARTITION ({partition_spec})")
                        logging.info(f"删除当天分区: {partition_spec}")
                    except Exception as e:
                        logging.warning(f"删除分区时出现警告: {e}")
                    
                    # 写入到当天分区
                    df.repartition(2).write.mode("append").partitionBy(part_col).options(**write_options).saveAsTable(hive_table)
                else:
                    # 如果没有分区列但要保留历史，建议添加分区列
                    logging.warning(f"表 {hive_table} 配置为保留历史但没有分区列，建议添加分区列")
                    df.repartition(2).write.mode("overwrite").options(**write_options).saveAsTable(hive_table)
        else:
            # 创建新表的情况
            logging.info("创建新表...")
            if part_col:
                df.repartition(2).write.mode("overwrite").partitionBy(part_col).options(**write_options).saveAsTable(hive_table)
            else:
                df.repartition(2).write.mode("overwrite").options(**write_options).saveAsTable(hive_table)
        
        # 清理缓存
        df.unpersist()
        
        # 刷新表元数据以确保文件可见
        spark.sql(f"REFRESH TABLE {hive_table}")
        logging.info(f"已刷新表元数据: {hive_table}")
        
        # 验证写入结果 - 改进错误处理
        verification_success = False
        try:
            # 等待一段时间让HDFS完成文件写入
            import time
            time.sleep(5)
            
            # 刷新表统计信息
            spark.sql(f"ANALYZE TABLE {hive_table} COMPUTE STATISTICS")
            
            final_count = spark.sql(f"SELECT COUNT(*) FROM {hive_table}").collect()[0][0]
            logging.info(f"✅ 写入成功！表 {hive_table} 中共有 {final_count} 条记录")
            verification_success = True
            
            # 如果是分区表，显示分区信息
            if part_col:
                try:
                    partitions = spark.sql(f"SHOW PARTITIONS {hive_table}").collect()
                    logging.info(f"表分区信息: {[row[0] for row in partitions[-3:]]}")  # 显示最后3个分区
                except Exception as part_e:
                    logging.warning(f"无法获取分区信息: {part_e}")
                    
        except Exception as count_e:
            logging.error(f"❌ 无法验证写入结果: {count_e}")
            
            # 检查是否是HDFS块丢失问题
            if "BlockMissingException" in str(count_e) or "Could not obtain block" in str(count_e):
                logging.error("检测到HDFS块丢失问题，可能是datanode故障")
                
                # 检查HDFS健康状态
                hdfs_healthy = check_hdfs_health()
                if not hdfs_healthy:
                    logging.error("HDFS集群不健康，建议检查datanode状态")
                
                # 尝试修复HDFS
                try:
                    logging.info("尝试运行HDFS fsck修复...")
                    import subprocess
                    fsck_result = subprocess.run([
                        'hdfs', 'fsck', table_location, '-delete-corrupted'
                    ], capture_output=True, text=True, timeout=60)
                    
                    if fsck_result.returncode == 0:
                        logging.info("HDFS fsck执行完成")
                    else:
                        logging.warning(f"HDFS fsck警告: {fsck_result.stderr}")
                        
                except Exception as fsck_e:
                    logging.warning(f"无法执行HDFS fsck: {fsck_e}")
            
            # 尝试简单的表存在性检查
            try:
                spark.sql(f"DESCRIBE {hive_table}")
                logging.info("✅ 表结构验证通过 - 表已创建")
                
                # 尝试使用不同的验证方法
                try:
                    # 使用HDFS命令直接检查文件
                    import subprocess
                    hdfs_result = subprocess.run([
                        'hdfs', 'dfs', '-ls', table_location
                    ], capture_output=True, text=True, timeout=30)
                    
                    if hdfs_result.returncode == 0:
                        logging.info("✅ HDFS文件验证通过")
                        logging.info(f"HDFS文件列表:\n{hdfs_result.stdout}")
                        verification_success = True
                    else:
                        logging.warning(f"HDFS文件检查警告: {hdfs_result.stderr}")
                        
                except Exception as hdfs_e:
                    logging.warning(f"无法执行HDFS文件检查: {hdfs_e}")
                    
            except Exception as desc_e:
                logging.error(f"❌ 表创建可能失败: {desc_e}")
                raise
        
        # 显示表结构（仅对新表）
        if not table_exists:
            try:
                logging.info(f"表 {hive_table} 的结构:")
                schema_info = spark.sql(f"DESCRIBE {hive_table}").collect()
                for row in schema_info:
                    logging.info(f"  {row[0]}: {row[1]}")
            except Exception as e:
                logging.warning(f"无法获取表结构: {e}")
        
        # 显示表的详细信息包括位置
        try:
            table_info = spark.sql(f"DESCRIBE EXTENDED {hive_table}").collect()
            for row in table_info:
                if 'Location' in str(row[0]) or 'location' in str(row[0]):
                    logging.info(f"表存储位置: {row[1]}")
                    break
        except Exception as e:
            logging.warning(f"无法获取表位置信息: {e}")
            
        # 验证数据是否可以正常读取 - 只在验证成功时进行
        if verification_success:
            try:
                sample_data = spark.sql(f"SELECT * FROM {hive_table} LIMIT 1").collect()
                if sample_data:
                    logging.info("✅ 数据验证通过 - 表可以正常读取")
                else:
                    logging.warning("⚠️ 表存在但无数据")
            except Exception as e:
                logging.error(f"❌ 数据验证失败: {e}")
                # 如果是HDFS问题，记录但不抛出异常
                if "BlockMissingException" in str(e):
                    logging.warning("数据验证失败可能是由于HDFS块问题，但数据写入可能已成功")
                else:
                    logging.warning("继续执行，但建议检查表状态")
        else:
            logging.warning("跳过数据验证，因为之前的验证失败")
            
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
        
        # 分区清理逻辑（仅对保留历史的全量同步表）
        if (sync_mode == 'full' and 
            full_backup_strategy == 'keep_history' and 
            part_col and 
            table_conf.get('partition_retention_days')):
            
            retention_days = table_conf.get('partition_retention_days', 30)
            logging.info(f"开始清理表 {hive_table} 超过 {retention_days} 天的旧分区...")
            cleanup_old_partitions(spark, hive_table, part_col, retention_days)
                
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

def check_hdfs_before_sync(**context):
    """同步前的HDFS健康检查"""
    logging.info("执行同步前的HDFS健康检查...")
    
    try:
        # 检查基本连接
        check_environment()
        
        # 检查HDFS健康状态
        hdfs_healthy = check_hdfs_health()
        
        if not hdfs_healthy:
            logging.error("HDFS集群不健康，建议先修复再执行同步")
            # 可以选择抛出异常来阻止后续任务
            # raise Exception("HDFS集群不健康")
            logging.warning("HDFS集群可能存在问题，但继续执行同步任务")
        
        logging.info("✅ HDFS健康检查完成")
        
    except Exception as e:
        logging.error(f"HDFS健康检查失败: {e}")
        raise

def generate_tasks(dag, config):
    """
    生成同步任务
    现在使用动态日期计算，不再需要在这里硬编码日期
    """
    tasks = []
    
    for table_conf in config['tables']:
        if not table_conf.get('enabled', True):
            continue

        task = PythonOperator(
            task_id=f"sync_{table_conf['mysql_table']}",
            python_callable=sync_table,
            op_kwargs={'table_conf': table_conf},
            provide_context=True,  # 提供 Airflow context
            dag=dag,
        )
        tasks.append(task)
    return tasks

# DAG 声明
with DAG(
    dag_id="sync_mysql_to_hive_dag",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # 每天凌晨 2 点执行（中国时间）
    start_date=datetime(2025, 8, 10, tzinfo=local_tz),  # 使用中国时区
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,  # 限制并发任务数
    tags=["sync", "mysql", "hive"],
) as dag:
    
    # HDFS健康检查任务
    hdfs_check_task = PythonOperator(
        task_id="check_hdfs_health",
        python_callable=check_hdfs_before_sync,
        dag=dag,
    )
    
    # 创建数据库任务
    create_databases_task = PythonOperator(
        task_id="create_hive_databases",
        python_callable=create_hive_databases,
        dag=dag,
    )
    
    # 加载配置并生成同步任务
    config = load_config()
    sync_tasks = generate_tasks(dag, config)
    
    # 设置任务依赖：HDFS检查 -> 创建数据库 -> 同步任务
    if sync_tasks:
        hdfs_check_task >> create_databases_task >> sync_tasks
