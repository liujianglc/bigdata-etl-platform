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
        .enableHiveSupport() \
        .getOrCreate()

def sync_table(table_conf, exec_date):
    spark = create_spark()
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

    # 增量条件构造
    if sync_mode == 'incremental':
        try:
            last_ts = spark.sql(f"SELECT MAX({inc_col}) FROM {hive_table}").collect()[0][0]
            where_clause = f" WHERE {inc_col} > '{last_ts}'" if last_ts else ""
        except:
            where_clause = ""
    else:
        where_clause = ""

    query = f"(SELECT * FROM {mysql_table}{where_clause}) AS tmp"
    df = spark.read.jdbc(mysql_url, query, properties=properties)

    if part_col:
        df = df.withColumn(part_col, lit(exec_date))

    # insert into hive with partition
    df.write.mode("append").partitionBy(part_col).insertInto(hive_table)

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
