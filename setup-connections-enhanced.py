#!/usr/bin/env python3
"""
增强版Airflow连接设置脚本
根据环境变量动态配置连接信息
"""

import os
from airflow.models import Connection
from airflow import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_connection(conn_id, conn_type, host, port=None, login=None, password=None, schema=None, extra=None):
    """创建或更新Airflow连接"""
    session = settings.Session()
    
    try:
        # 检查连接是否已存在
        existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if existing_conn:
            logger.info(f"连接 {conn_id} 已存在，删除旧连接")
            session.delete(existing_conn)
            session.commit()  # 先提交删除操作
        
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
        logger.info(f"成功创建连接: {conn_id}")
        
    except Exception as e:
        logger.error(f"创建连接 {conn_id} 失败: {str(e)}")
        session.rollback()
        # 如果失败，尝试跳过这个连接继续处理其他连接
        logger.warning(f"跳过连接 {conn_id}，继续处理其他连接")
    finally:
        session.close()

def main():
    """设置所有连接"""
    logger.info("开始设置Airflow连接...")
    
    # 从环境变量读取外部MySQL配置
    mysql_host = os.getenv('EXTERNAL_MYSQL_HOST', '10.0.19.6')
    mysql_port = int(os.getenv('EXTERNAL_MYSQL_PORT', 3306))
    mysql_user = os.getenv('EXTERNAL_MYSQL_USER', 'root')
    mysql_password = os.getenv('EXTERNAL_MYSQL_PASSWORD', 'Sp1derman123@')
    mysql_database = os.getenv('EXTERNAL_MYSQL_DATABASE', 'wudeli')
    
    spark_host = os.getenv('SPARK_MASTER_HOST', 'spark-master')
    spark_port = int(os.getenv('SPARK_MASTER_PORT', 7077))
    
    hive_host = 'hive-metastore'
    hive_port = int(os.getenv('HIVE_METASTORE_PORT', 9083))
    
    hdfs_host = os.getenv('HDFS_NAMENODE_HOST', 'namenode')
    hdfs_port = int(os.getenv('HDFS_NAMENODE_PORT', 9000))
    
    # MySQL连接 - 主要数据源连接
    create_connection(
        conn_id='mysql_default',
        conn_type='mysql',
        host=mysql_host,
        port=mysql_port,
        login=mysql_user,
        password=mysql_password,
        schema=mysql_database
    )
    
    # Spark连接
    create_connection(
        conn_id='spark_default',
        conn_type='spark',
        host=spark_host,
        port=spark_port,
        extra='{"deploy-mode": "client", "spark.executor.memory": "2g", "spark.driver.memory": "2g", "spark.executor.cores": "2"}'
    )
    
    # Hive连接
    create_connection(
        conn_id='hive_default',
        conn_type='hive_cli',
        host=hive_host,
        port=hive_port,
        schema='default'
    )
    
    # HDFS连接
    create_connection(
        conn_id='hdfs_default',
        conn_type='hdfs',
        host=hdfs_host,
        port=hdfs_port,
        extra='{"namenode_principal": ""}'
    )
    
    # Hive Metastore Thrift连接
    create_connection(
        conn_id='hive_metastore_default',
        conn_type='hive_metastore',
        host=hive_host,
        port=hive_port
    )
    
    # 外部MySQL连接（生产环境，与 mysql_default 相同配置）
    create_connection(
        conn_id='mysql_wudeli',
        conn_type='mysql',
        host=mysql_host,
        port=mysql_port,
        login=mysql_user,
        password=mysql_password,
        schema=mysql_database
    )
    
    logger.info("所有连接设置完成！")
    
    # 验证连接
    logger.info("验证连接配置...")
    session = settings.Session()
    try:
        connections = session.query(Connection).all()
        logger.info(f"当前共有 {len(connections)} 个连接:")
        for conn in connections:
            logger.info(f"  - {conn.conn_id} ({conn.conn_type}): {conn.host}:{conn.port}")
    except Exception as e:
        logger.error(f"验证连接失败: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    main()