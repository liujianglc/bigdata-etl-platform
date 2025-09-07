from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
import logging
import os

# =============================================================================
# DEFAULT ARGS
# =============================================================================
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_cleanup_needed(**context):
    """检查是否需要执行清理"""
    from pyspark.sql import SparkSession
    
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("Check_Duplicate_Cleanup_Needed") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # 检查表是否存在重复数据
        tables_to_check = [
            ('dwd_db.dwd_orders', 'OrderID'),
            ('dwd_db.dwd_orderdetails', 'OrderDetailID')
        ]
        
        cleanup_needed = False
        duplicate_stats = {}
        
        for table_name, primary_key in tables_to_check:
            try:
                # 检查最近3天的数据
                recent_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                
                # 统计重复数据
                duplicate_query = f"""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(DISTINCT {primary_key}) as unique_count
                FROM {table_name}
                WHERE dt >= '{(datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')}'
                """
                
                result = spark.sql(duplicate_query).collect()[0]
                total_count = result['total_count']
                unique_count = result['unique_count']
                duplicate_count = total_count - unique_count
                
                duplicate_stats[table_name] = {
                    'total_count': total_count,
                    'unique_count': unique_count,
                    'duplicate_count': duplicate_count,
                    'duplicate_rate': duplicate_count / total_count if total_count > 0 else 0
                }
                
                if duplicate_count > 0:
                    cleanup_needed = True
                    logging.warning(f"发现重复数据 - {table_name}: {duplicate_count} 条重复记录")
                else:
                    logging.info(f"无重复数据 - {table_name}")
                    
            except Exception as e:
                logging.warning(f"检查表 {table_name} 失败: {e}")
        
        # 推送结果到 XCom
        context['task_instance'].xcom_push(key='cleanup_needed', value=cleanup_needed)
        context['task_instance'].xcom_push(key='duplicate_stats', value=duplicate_stats)
        
        if cleanup_needed:
            logging.info("🧹 检测到重复数据，需要执行清理")
        else:
            logging.info("✅ 未检测到重复数据，无需清理")
            
        return cleanup_needed
        
    except Exception as e:
        logging.error(f"检查重复数据失败: {e}")
        raise
    finally:
        if spark:
            spark.stop()

def execute_cleanup(**context):
    """执行清理操作"""
    cleanup_needed = context['task_instance'].xcom_pull(task_ids='check_cleanup_needed', key='cleanup_needed')
    
    if not cleanup_needed:
        logging.info("无需清理，跳过执行")
        return "SKIPPED"
    
    logging.info("开始执行重复数据清理...")
    return "EXECUTED"

def send_cleanup_report(**context):
    """发送清理报告"""
    cleanup_needed = context['task_instance'].xcom_pull(task_ids='check_cleanup_needed', key='cleanup_needed')
    duplicate_stats = context['task_instance'].xcom_pull(task_ids='check_cleanup_needed', key='duplicate_stats')
    
    if cleanup_needed:
        logging.info("📊 重复数据清理报告:")
        for table_name, stats in duplicate_stats.items():
            logging.info(f"  {table_name}:")
            logging.info(f"    总记录数: {stats['total_count']:,}")
            logging.info(f"    唯一记录数: {stats['unique_count']:,}")
            logging.info(f"    重复记录数: {stats['duplicate_count']:,}")
            logging.info(f"    重复率: {stats['duplicate_rate']:.2%}")
    else:
        logging.info("✅ 数据质量良好，无重复数据")

# DAG 定义
with DAG(
    'cleanup_duplicate_data',
    default_args=default_args,
    description='定期清理DWD层重复数据',
    schedule_interval='0 4 * * 0',  # 每周日凌晨4点执行
    catchup=False,
    max_active_runs=1,
    tags=['cleanup', 'data_quality', 'maintenance']
) as dag:

    start = DummyOperator(task_id='start')

    # 检查是否需要清理
    check_task = PythonOperator(
        task_id='check_cleanup_needed',
        python_callable=check_cleanup_needed,
        provide_context=True
    )

    # 执行清理脚本（仅分析模式）
    analyze_task = BashOperator(
        task_id='analyze_duplicates',
        bash_command="""
        cd /opt/airflow/scripts && \
        export CLEANUP_DAYS=7 && \
        export ANALYZE_ONLY=true && \
        python3 cleanup_duplicate_data.py
        """,
        trigger_rule='none_failed'
    )

    # 条件执行清理（如果发现重复数据）
    cleanup_task = PythonOperator(
        task_id='execute_cleanup',
        python_callable=execute_cleanup,
        provide_context=True,
        trigger_rule='none_failed'
    )

    # 发送报告
    report_task = PythonOperator(
        task_id='send_cleanup_report',
        python_callable=send_cleanup_report,
        provide_context=True,
        trigger_rule='none_failed_or_skipped'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_or_skipped'
    )

    # 任务依赖
    start >> check_task >> analyze_task >> cleanup_task >> report_task >> end

