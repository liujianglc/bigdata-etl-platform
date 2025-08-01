from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_mysql_connection(**context):
    """检查MySQL连接"""
    import mysql.connector
    import logging
    
    try:
        # 直接连接MySQL
        connection = mysql.connector.connect(
            host='mysql',
            port=3306,
            user='etl_user',
            password='etl_pass',
            database='source_db'
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM orders")
        result = cursor.fetchone()
        
        logging.info(f"MySQL connection successful. Orders count: {result[0]}")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        logging.error(f"MySQL connection failed: {e}")
        raise

def simple_data_processing(**context):
    """简单的数据处理示例"""
    import mysql.connector
    import logging
    import json
    
    logging.info("执行简单的数据处理逻辑...")
    
    try:
        # 连接MySQL并读取数据
        connection = mysql.connector.connect(
            host='mysql',
            port=3306,
            user='etl_user',
            password='etl_pass',
            database='source_db'
        )
        
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM orders LIMIT 10")
        orders = cursor.fetchall()
        
        # 简单的数据处理：计算总金额
        total_amount = sum(float(order['amount']) for order in orders)
        
        logging.info(f"处理了 {len(orders)} 条订单记录")
        logging.info(f"总金额: {total_amount}")
        
        # 将处理结果保存到XCom
        context['task_instance'].xcom_push(key='processed_orders', value=len(orders))
        context['task_instance'].xcom_push(key='total_amount', value=total_amount)
        
        cursor.close()
        connection.close()
        
        logging.info("数据处理完成")
        return f"处理了 {len(orders)} 条记录，总金额: {total_amount}"
        
    except Exception as e:
        logging.error(f"数据处理失败: {e}")
        raise

with DAG(
    'simple_mysql_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['mysql', 'etl', 'simple'],
    description='简化版MySQL ETL流程',
) as dag:

    # 1. 检查MySQL连接
    check_mysql = PythonOperator(
        task_id='check_mysql_connection',
        python_callable=check_mysql_connection,
    )

    # 2. 查看MySQL数据
    def view_mysql_data(**context):
        """查看MySQL数据"""
        import mysql.connector
        import logging
        
        try:
            connection = mysql.connector.connect(
                host='mysql',
                port=3306,
                user='etl_user',
                password='etl_pass',
                database='source_db'
            )
            
            cursor = connection.cursor(dictionary=True)
            
            # 查看前5条数据
            cursor.execute("SELECT * FROM orders LIMIT 5")
            sample_data = cursor.fetchall()
            logging.info("Orders Table Sample Data:")
            for row in sample_data:
                logging.info(f"  {row}")
            
            # 查看总数
            cursor.execute("SELECT COUNT(*) as total FROM orders")
            total = cursor.fetchone()
            logging.info(f"Total Orders: {total['total']}")
            
            cursor.close()
            connection.close()
            
            return f"查看了 {len(sample_data)} 条样本数据，总计 {total['total']} 条记录"
            
        except Exception as e:
            logging.error(f"查看数据失败: {e}")
            raise

    view_mysql_data_task = PythonOperator(
        task_id='view_mysql_data',
        python_callable=view_mysql_data,
    )

    # 3. 简单数据处理
    process_data = PythonOperator(
        task_id='process_data',
        python_callable=simple_data_processing,
    )

    # 4. 导出数据到文件
    def export_data_to_file(**context):
        """导出数据到文件"""
        import mysql.connector
        import csv
        import logging
        from datetime import datetime
        import os
        
        try:
            # 创建输出目录
            output_dir = '/tmp/etl_output'
            os.makedirs(output_dir, exist_ok=True)
            
            # 生成文件名
            today = datetime.now().strftime('%Y%m%d')
            filename = f'{output_dir}/orders_{today}.csv'
            
            # 连接MySQL
            connection = mysql.connector.connect(
                host='mysql',
                port=3306,
                user='etl_user',
                password='etl_pass',
                database='source_db'
            )
            
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM orders")
            
            # 写入CSV文件
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                # 获取列名
                column_names = [desc[0] for desc in cursor.description]
                writer = csv.writer(csvfile)
                writer.writerow(column_names)
                
                # 写入数据
                rows_written = 0
                for row in cursor:
                    writer.writerow(row)
                    rows_written += 1
            
            cursor.close()
            connection.close()
            
            # 获取文件大小
            file_size = os.path.getsize(filename)
            
            logging.info(f"数据已导出到 {filename}")
            logging.info(f"导出行数: {rows_written}")
            logging.info(f"文件大小: {file_size} bytes")
            
            # 保存到XCom
            context['task_instance'].xcom_push(key='export_file', value=filename)
            context['task_instance'].xcom_push(key='rows_exported', value=rows_written)
            
            return f"导出 {rows_written} 行数据到 {filename}"
            
        except Exception as e:
            logging.error(f"导出数据失败: {e}")
            raise

    export_data_task = PythonOperator(
        task_id='export_data_to_file',
        python_callable=export_data_to_file,
    )

    # 5. 数据质量检查
    def validate_export(**context):
        """验证导出文件"""
        import logging
        import os
        
        try:
            # 从XCom获取导出文件信息
            export_file = context['task_instance'].xcom_pull(task_ids='export_data_to_file', key='export_file')
            rows_exported = context['task_instance'].xcom_pull(task_ids='export_data_to_file', key='rows_exported')
            
            if not export_file or not os.path.exists(export_file):
                raise Exception("导出文件不存在！")
            
            # 检查文件大小
            file_size = os.path.getsize(export_file)
            
            # 验证行数（简单检查）
            with open(export_file, 'r') as f:
                actual_lines = sum(1 for line in f) - 1  # 减去标题行
            
            logging.info(f"文件验证结果:")
            logging.info(f"  文件路径: {export_file}")
            logging.info(f"  文件大小: {file_size} bytes")
            logging.info(f"  预期行数: {rows_exported}")
            logging.info(f"  实际行数: {actual_lines}")
            
            if actual_lines != rows_exported:
                raise Exception(f"行数不匹配！预期: {rows_exported}, 实际: {actual_lines}")
            
            logging.info("数据质量检查通过！")
            return f"验证通过: {actual_lines} 行数据，文件大小 {file_size} bytes"
            
        except Exception as e:
            logging.error(f"数据质量检查失败: {e}")
            raise

    validate_export_task = PythonOperator(
        task_id='validate_export',
        python_callable=validate_export,
    )

    # 定义任务依赖关系
    check_mysql >> view_mysql_data_task >> process_data >> export_data_task >> validate_export_task