from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def test_python_task(**context):
    """简单的Python测试任务"""
    import logging
    logging.info("Python任务执行成功！")
    print("这是一个测试任务")
    return "success"

def check_connections(**context):
    """检查基本连接"""
    import logging
    import os
    
    logging.info("检查环境变量...")
    logging.info(f"MYSQL_HOST: {os.getenv('MYSQL_HOST', 'mysql')}")
    logging.info(f"SPARK_MASTER_HOST: {os.getenv('SPARK_MASTER_HOST', 'spark-master')}")
    logging.info(f"HDFS_NAMENODE_HOST: {os.getenv('HDFS_NAMENODE_HOST', 'namenode')}")
    
    return "connections_ok"

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_test_dag',
    default_args=default_args,
    description='简单测试DAG',
    schedule_interval=None,  # 只手动触发
    max_active_runs=1,
    catchup=False,
    tags=['test', 'simple']
)

# 任务定义
start = DummyOperator(
    task_id='start',
    dag=dag
)

test_bash = BashOperator(
    task_id='test_bash',
    bash_command='echo "Bash任务执行成功！" && date && whoami',
    dag=dag
)

test_python = PythonOperator(
    task_id='test_python',
    python_callable=test_python_task,
    dag=dag
)

check_env = PythonOperator(
    task_id='check_environment',
    python_callable=check_connections,
    dag=dag
)

test_hdfs = BashOperator(
    task_id='test_hdfs',
    bash_command='hdfs dfs -ls / || echo "HDFS连接失败，但继续执行"',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# 任务依赖
start >> [test_bash, test_python, check_env] >> test_hdfs >> end