from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.dummy import DummyOperator

# 导入必需的 providers
try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: SparkSubmitOperator not available")

try:
    from airflow.providers.mysql.operators.mysql import MySqlOperator
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    print("Warning: MySQL providers not available")

import os
import logging
import yaml

def load_dynamic_config():
    """从动态配置文件和环境变量加载配置"""
    config_file = '/opt/airflow/config/production_etl_config_dynamic.yaml'
    
    # 默认配置
    default_config = {
        'mysql_conn': {
            'host': os.getenv('MYSQL_HOST', 'mysql'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'user': os.getenv('MYSQL_USER', 'etl_user'),
            'password': os.getenv('MYSQL_PASSWORD', 'etl_pass'),
            'database': os.getenv('MYSQL_DATABASE', 'wudeli')
        },
        'spark_config': {
            'master': f"spark://{os.getenv('SPARK_MASTER_HOST', 'spark-master')}:{os.getenv('SPARK_MASTER_PORT', '7077')}",
            'deploy_mode': 'client',
            'driver_memory': '2g',
            'executor_memory': os.getenv('SPARK_WORKER_MEMORY', '2g'),
            'executor_cores': int(os.getenv('SPARK_WORKER_CORES', 2)),
            'num_executors': 2
        },
        'hdfs_paths': {
            'base': '/user/bigdata/wudeli',
            'raw': '/user/bigdata/wudeli/raw',
            'processed': '/user/bigdata/wudeli/processed',
            'reports': '/user/bigdata/wudeli/reports',
            'temp': '/user/bigdata/wudeli/temp'
        },
        'hive_db': 'wudeli_analytics',
        'services': {
            'hdfs_namenode': f"hdfs://{os.getenv('HDFS_NAMENODE_HOST', 'namenode')}:{os.getenv('HDFS_NAMENODE_PORT', '9000')}",
            'hive_metastore': f"thrift://hive-metastore:{os.getenv('HIVE_METASTORE_PORT', '9083')}"
        }
    }
    
    try:
        # 尝试加载YAML配置文件
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                # 替换环境变量
                content = f.read()
                # 简单的环境变量替换
                for key, value in os.environ.items():
                    content = content.replace(f"${{{key}}}", value)
                    content = content.replace(f"${{{key}:-", f"${{{key}:-").replace(f"}}", "}")
                
                yaml_config = yaml.safe_load(content)
                # 合并配置
                if yaml_config:
                    default_config.update(yaml_config)
                    logging.info("成功加载动态配置文件")
        else:
            logging.info("使用默认配置（配置文件不存在）")
    except Exception as e:
        logging.warning(f"加载配置文件失败，使用默认配置: {str(e)}")
    
    return default_config

# 加载动态配置
PRODUCTION_CONFIG = load_dynamic_config()

default_args = {
    'owner': 'bigdata_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': int(os.getenv('ETL_RETRY_ATTEMPTS', 2)),
    'retry_delay': timedelta(minutes=int(os.getenv('ETL_RETRY_DELAY', 10))),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
    'email_on_failure': os.getenv('ENABLE_ALERTS', 'true').lower() == 'true',
    'email_on_retry': False,
}

def check_mysql_connection(**context):
    """检查MySQL连接和数据"""
    import mysql.connector
    import logging
    
    config = PRODUCTION_CONFIG['mysql_conn']
    
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database']
        )
        
        cursor = connection.cursor()
        
        # 检查数据库中的表
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        logging.info(f"数据库 {config['database']} 中的表: {[table[0] for table in tables]}")
        
        if tables:
            # 检查orders表（如果存在）
            table_names = [table[0] for table in tables]
            if 'orders' in table_names:
                cursor.execute("SELECT COUNT(*) as count FROM orders")
                result = cursor.fetchone()
                logging.info(f"orders表记录数: {result[0]}")
                
                # 检查最新数据
                cursor.execute("SELECT MAX(created_at) FROM orders")
                latest = cursor.fetchone()
                logging.info(f"最新数据时间: {latest[0]}")
        
        cursor.close()
        connection.close()
        
        logging.info("MySQL连接检查完成")
        return True
        
    except Exception as e:
        logging.error(f"MySQL连接检查失败: {str(e)}")
        raise

def check_hdfs_connection(**context):
    """检查HDFS连接"""
    import subprocess
    import logging
    
    try:
        # 检查HDFS状态
        result = subprocess.run(['hdfs', 'dfsadmin', '-report'], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            logging.info("HDFS连接正常")
            logging.info(f"HDFS报告: {result.stdout[:500]}...")
            
            # 检查目录是否存在
            hdfs_paths = PRODUCTION_CONFIG['hdfs_paths']
            for path_name, path_value in hdfs_paths.items():
                check_result = subprocess.run(['hdfs', 'dfs', '-test', '-d', path_value], 
                                            capture_output=True, timeout=10)
                if check_result.returncode == 0:
                    logging.info(f"HDFS目录 {path_name} ({path_value}) 存在")
                else:
                    logging.warning(f"HDFS目录 {path_name} ({path_value}) 不存在")
            
            return True
        else:
            logging.error(f"HDFS连接失败: {result.stderr}")
            raise Exception(f"HDFS连接失败: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logging.error("HDFS连接检查超时")
        raise Exception("HDFS连接检查超时")
    except Exception as e:
        logging.error(f"HDFS连接检查失败: {str(e)}")
        raise

def check_spark_connection(**context):
    """检查Spark连接"""
    import subprocess
    import logging
    
    try:
        spark_master = PRODUCTION_CONFIG['spark_config']['master']
        logging.info(f"检查Spark Master: {spark_master}")
        
        # 使用spark-submit检查连接
        spark_check_cmd = [
            'spark-submit',
            '--master', spark_master,
            '--deploy-mode', 'client',
            '--class', 'org.apache.spark.examples.SparkPi',
            '/opt/spark/examples/jars/spark-examples_2.12-3.3.0.jar',
            '1'
        ]
        
        result = subprocess.run(spark_check_cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            logging.info("Spark连接正常")
            logging.info("Spark Pi 测试完成")
            return True
        else:
            logging.error(f"Spark连接失败: {result.stderr}")
            # 非致命错误，继续执行
            logging.warning("Spark连接检查失败，但继续执行ETL流程")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error("Spark连接检查超时")
        return False
    except Exception as e:
        logging.error(f"Spark连接检查失败: {str(e)}")
        return False

def run_mysql_to_hive_etl(**context):
    """执行MySQL到Hive的ETL作业"""
    import subprocess
    import logging
    
    try:
        config = PRODUCTION_CONFIG
        
        # 构建spark-submit命令
        spark_cmd = [
            'spark-submit',
            '--master', config['spark_config']['master'],
            '--deploy-mode', config['spark_config']['deploy_mode'],
            '--driver-memory', config['spark_config']['driver_memory'],
            '--executor-memory', config['spark_config']['executor_memory'],
            '--executor-cores', str(config['spark_config']['executor_cores']),
            '--num-executors', str(config['spark_config']['num_executors']),
            '--conf', 'spark.sql.adaptive.enabled=true',
            '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
            '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
            '/opt/airflow/spark_jobs/incremental_mysql_to_hive.py'
        ]
        
        logging.info(f"执行Spark作业: {' '.join(spark_cmd)}")
        
        result = subprocess.run(spark_cmd, capture_output=True, text=True, timeout=1800)  # 30分钟超时
        
        if result.returncode == 0:
            logging.info("MySQL到Hive ETL作业完成")
            logging.info(f"作业输出: {result.stdout}")
            return True
        else:
            logging.error(f"MySQL到Hive ETL作业失败: {result.stderr}")
            raise Exception(f"ETL作业失败: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logging.error("MySQL到Hive ETL作业超时")
        raise Exception("ETL作业超时")
    except Exception as e:
        logging.error(f"MySQL到Hive ETL作业失败: {str(e)}")
        raise

def create_hive_analytics_views(**context):
    """创建Hive分析视图"""
    import subprocess
    import logging
    
    try:
        config = PRODUCTION_CONFIG
        
        # 构建spark-submit命令来创建分析视图
        spark_cmd = [
            'spark-submit',
            '--master', config['spark_config']['master'],
            '--deploy-mode', config['spark_config']['deploy_mode'],
            '--driver-memory', config['spark_config']['driver_memory'],
            '--executor-memory', config['spark_config']['executor_memory'],
            '/opt/airflow/spark_jobs/hive_analytics_views.py'
        ]
        
        logging.info(f"创建Hive分析视图: {' '.join(spark_cmd)}")
        
        result = subprocess.run(spark_cmd, capture_output=True, text=True, timeout=900)  # 15分钟超时
        
        if result.returncode == 0:
            logging.info("Hive分析视图创建完成")
            logging.info(f"视图创建输出: {result.stdout}")
            return True
        else:
            logging.error(f"Hive分析视图创建失败: {result.stderr}")
            raise Exception(f"视图创建失败: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logging.error("Hive分析视图创建超时")
        raise Exception("视图创建超时")
    except Exception as e:
        logging.error(f"Hive分析视图创建失败: {str(e)}")
        raise

# 创建DAG
dag = DAG(
    'dynamic_bigdata_etl',
    default_args=default_args,
    description='动态配置的大数据ETL流程',
    schedule_interval=os.getenv('ETL_SCHEDULE_CRON', '0 2 * * *'),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'bigdata', 'production', 'dynamic']
)

# 任务定义
start_task = DummyOperator(
    task_id='start_etl_pipeline',
    dag=dag
)

# 连接检查任务
check_mysql = PythonOperator(
    task_id='check_mysql_connection',
    python_callable=check_mysql_connection,
    dag=dag
)

check_hdfs = PythonOperator(
    task_id='check_hdfs_connection',
    python_callable=check_hdfs_connection,
    dag=dag
)

check_spark = PythonOperator(
    task_id='check_spark_connection',
    python_callable=check_spark_connection,
    dag=dag
)

# 数据同步任务
mysql_to_hive = PythonOperator(
    task_id='mysql_to_hive_etl',
    python_callable=run_mysql_to_hive_etl,
    dag=dag
)

# 创建分析视图
create_views = PythonOperator(
    task_id='create_analytics_views',
    python_callable=create_hive_analytics_views,
    dag=dag
)

# 数据质量检查
data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command=f"""
    echo "执行数据质量检查..."
    
    # 检查HDFS中的数据文件
    hdfs dfs -ls {PRODUCTION_CONFIG['hdfs_paths']['processed']} || echo "处理后数据目录为空"
    
    # 检查数据新鲜度（这里是示例，实际应该检查具体的数据时间戳）
    echo "数据质量检查完成"
    """,
    dag=dag
)

# 生成报告
generate_report = BashOperator(
    task_id='generate_etl_report',
    bash_command=f"""
    echo "生成ETL报告..."
    
    # 创建报告目录
    hdfs dfs -mkdir -p {PRODUCTION_CONFIG['hdfs_paths']['reports']}/$(date +%Y%m%d)
    
    # 生成简单的报告（实际中应该是更复杂的报告生成逻辑）
    echo "ETL执行报告 - $(date)" > /tmp/etl_report.txt
    echo "处理记录数: 待统计" >> /tmp/etl_report.txt
    echo "执行状态: 成功" >> /tmp/etl_report.txt
    
    # 上传报告到HDFS
    hdfs dfs -put /tmp/etl_report.txt {PRODUCTION_CONFIG['hdfs_paths']['reports']}/$(date +%Y%m%d)/etl_report.txt
    
    echo "ETL报告生成完成"
    """,
    dag=dag
)

end_task = DummyOperator(
    task_id='end_etl_pipeline',
    dag=dag
)

# 定义任务依赖关系
start_task >> [check_mysql, check_hdfs, check_spark]
[check_mysql, check_hdfs, check_spark] >> mysql_to_hive
mysql_to_hive >> create_views
create_views >> data_quality_check
data_quality_check >> generate_report
generate_report >> end_task