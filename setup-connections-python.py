#!/usr/bin/env python3
"""
使用Python API设置Airflow连接
"""

from airflow.models import Connection
from airflow import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_connection(conn_id, conn_type, host, port=None, login=None, password=None, schema=None, extra=None):
    """创建Airflow连接"""
    session = settings.Session()
    
    # 检查连接是否已存在
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        logger.info(f"连接 {conn_id} 已存在，删除旧连接")
        session.delete(existing_conn)
    
    # 创建新连接
    new_conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        port=port,
        login=login,
        password=password,
        schema=schema,
        extra=extra
    )
    
    session.add(new_conn)
    session.commit()
    session.close()
    
    logger.info(f"成功创建连接: {conn_id}")

def main():
    """设置所有连接"""
    logger.info("开始设置Airflow连接...")
    
    # MySQL连接
    create_connection(
        conn_id='mysql_default',
        conn_type='mysql',
        host='mysql',
        port=3306,
        login='etl_user',
        password='etl_pass',
        schema='source_db'
    )
    
    # Spark连接
    create_connection(
        conn_id='spark_default',
        conn_type='spark',
        host='spark-master',
        port=7077
    )
    
    # Hive连接
    create_connection(
        conn_id='hive_default',
        conn_type='hive_cli',
        host='hive-metastore',
        port=9083
    )
    
    logger.info("所有连接设置完成！")

if __name__ == "__main__":
    main()