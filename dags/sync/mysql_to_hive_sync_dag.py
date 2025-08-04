from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

CONFIG_PATH = "/opt/airflow/config/mysql_to_hive_config.yaml"  # 放在挂载目录中
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def create_spark():
    return SparkSession.builder \
        .appName("MySQL to Hive Sync") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .enableHiveSupport() \
        .getOrCreate()

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
        }

        mysql_table = table_conf['mysql_table']
        hive_table = table_conf['hive_table']
        inc_col = table_conf.get('incremental_column', 'update_time')
        part_col = table_conf.get('partition_column', 'dt')
        sync_mode = table_conf.get('sync_mode', 'incremental')
        
        print(f"开始同步表: {mysql_table} -> {hive_table}")
        print(f"同步模式: {sync_mode}, 分区列: {part_col}")
        
        # 确保Hive数据库存在
        hive_db = hive_table.split('.')[0] if '.' in hive_table else 'default'
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
        spark.sql(f"USE {hive_db}")
        print(f"Hive数据库 {hive_db} 已准备就绪")

        # 检查表是否存在
        table_exists = False
        try:
            spark.sql(f"DESCRIBE {hive_table}")
            table_exists = True
            print(f"Hive表 {hive_table} 已存在")
        except:
            table_exists = False
            print(f"Hive表 {hive_table} 不存在，将创建新表")
        
        # 构建增量查询条件
        where_clause = ""
        if sync_mode == 'incremental' and table_exists:
            try:
                last_ts = spark.sql(f"SELECT MAX({inc_col}) FROM {hive_table}").collect()[0][0]
                if last_ts:
                    where_clause = f" WHERE {inc_col} > '{last_ts}'"
                    print(f"增量同步：从 {last_ts} 之后开始")
                else:
                    print("增量同步：表为空，执行全量同步")
            except Exception as e:
                print(f"无法获取增量时间戳，执行全量同步: {e}")
                where_clause = ""
        else:
            print("执行全量同步")

        # 从MySQL读取数据
        query = f"(SELECT * FROM {mysql_table}{where_clause}) AS tmp"
        print(f"执行MySQL查询: SELECT * FROM {mysql_table}{where_clause}")
        
        df = spark.read.jdbc(mysql_url, query, properties=properties)
        record_count = df.count()
        print(f"从MySQL读取到 {record_count} 条记录")
        
        if record_count == 0:
            print("没有数据需要同步")
            return

        # 添加分区列
        if part_col:
            df = df.withColumn(part_col, lit(exec_date))
            print(f"添加分区列 {part_col} = {exec_date}")

        # 写入Hive表
        if table_exists and sync_mode == 'incremental':
            # 增量写入已存在的表
            print("增量写入已存在的表...")
            if part_col:
                # 对于分区表，使用insertInto
                df.write.mode("append").insertInto(hive_table)
            else:
                # 对于非分区表，使用saveAsTable append模式
                df.write.mode("append").saveAsTable(hive_table)
        else:
            # 全量写入或创建新表
            print("创建新表或全量覆盖...")
            if part_col:
                df.write.mode("overwrite").partitionBy(part_col).saveAsTable(hive_table)
            else:
                df.write.mode("overwrite").saveAsTable(hive_table)
        
        # 验证写入结果
        final_count = spark.sql(f"SELECT COUNT(*) FROM {hive_table}").collect()[0][0]
        print(f"✅ 写入成功！表 {hive_table} 中共有 {final_count} 条记录")
        
        # 显示表结构（仅对新表）
        if not table_exists:
            print(f"表 {hive_table} 的结构:")
            spark.sql(f"DESCRIBE {hive_table}").show()
            
        # 验证数据文件是否真的存在
        try:
            spark.sql(f"SELECT * FROM {hive_table} LIMIT 1").show()
            print("✅ 数据验证通过")
        except Exception as e:
            print(f"⚠️ 数据验证失败: {e}")
            raise
                
    except Exception as e:
        print(f"❌ 同步失败: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # 确保Spark会话被正确关闭
        if spark:
            try:
                spark.stop()
                print("Spark会话已关闭")
            except:
                pass

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
    concurrency=5,
    tags=["sync", "mysql", "hive"],
) as dag:
    config = load_config()
    sync_tasks = generate_tasks(dag, config)
