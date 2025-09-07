"""
重复数据防护工具函数
用于DWD层ETL流程中的重复数据检测和处理
"""

import logging
import yaml
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, desc, asc
from pyspark.sql.window import Window


def load_duplicate_prevention_config():
    """加载重复数据防护配置"""
    config_path = '/opt/airflow/config/duplicate_prevention_config.yaml'
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        else:
            logging.warning(f"Duplicate prevention config not found: {config_path}")
            return _get_default_config()
    except Exception as e:
        logging.error(f"Failed to load duplicate prevention config: {e}")
        return _get_default_config()


def _get_default_config():
    """默认重复数据防护配置"""
    return {
        'duplicate_prevention': {
            'global_strategy': {
                'enable_deduplication': True,
                'dedup_columns': ['OrderID'],
                'dedup_method': 'drop_duplicates'
            },
            'dimension_tables': {
                'ods.Customers': {
                    'enable_unique_join': True,
                    'unique_key': 'CustomerID',
                    'sort_columns': ['UpdatedDate DESC', 'CreatedDate DESC']
                },
                'ods.Employees': {
                    'enable_unique_join': True,
                    'unique_key': 'EmployeeID', 
                    'sort_columns': ['UpdatedDate DESC', 'CreatedDate DESC']
                }
            }
        },
        'data_quality_checks': {
            'duplicate_tolerance': {
                'max_duplicate_rate': 0.05,
                'warning_duplicate_rate': 0.01
            }
        }
    }


def generate_unique_dimension_cte(table_name, config):
    """
    生成维度表去重的CTE SQL
    
    Args:
        table_name: 表名，如 'ods.Customers'
        config: 重复数据防护配置
        
    Returns:
        str: CTE SQL语句
    """
    dim_config = config.get('duplicate_prevention', {}).get('dimension_tables', {}).get(table_name, {})
    
    if not dim_config.get('enable_unique_join', False):
        return None
        
    unique_key = dim_config.get('unique_key')
    sort_columns = dim_config.get('sort_columns', ['UpdatedDate DESC', 'CreatedDate DESC'])
    
    if not unique_key:
        logging.warning(f"No unique key defined for {table_name}, skipping unique CTE generation")
        return None
    
    # 构建排序字段
    sort_clause = ", ".join(sort_columns)
    
    # 根据表名生成CTE名称
    cte_name = table_name.replace('ods.', 'unique_').lower()
    
    cte_sql = f"""
    {cte_name} AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {unique_key}
                ORDER BY {sort_clause}
            ) as rn
        FROM {table_name}
        WHERE {{partition_filter}}
    )"""
    
    return cte_name, cte_sql


def check_duplicate_rate(df: DataFrame, key_columns: list, context=None):
    """
    检查DataFrame中的重复率
    
    Args:
        df: Spark DataFrame
        key_columns: 用于检查重复的关键字段列表
        context: Airflow context (可选)
        
    Returns:
        dict: 包含重复统计信息的字典
    """
    try:
        total_count = df.count()
        if total_count == 0:
            return {
                'total_records': 0,
                'unique_records': 0,
                'duplicate_records': 0,
                'duplicate_rate': 0.0
            }
        
        unique_count = df.dropDuplicates(key_columns).count()
        duplicate_count = total_count - unique_count
        duplicate_rate = duplicate_count / total_count if total_count > 0 else 0.0
        
        stats = {
            'total_records': total_count,
            'unique_records': unique_count,
            'duplicate_records': duplicate_count,
            'duplicate_rate': duplicate_rate
        }
        
        # 记录统计信息
        logging.info(f"📊 Duplicate check results: {stats}")
        
        # 推送到XCom（如果提供了context）
        if context:
            context['task_instance'].xcom_push(key='duplicate_check_stats', value=stats)
            
        return stats
        
    except Exception as e:
        logging.error(f"Failed to check duplicate rate: {e}")
        return {
            'total_records': 0,
            'unique_records': 0,
            'duplicate_records': 0,
            'duplicate_rate': 0.0,
            'error': str(e)
        }


def deduplicate_dataframe(df: DataFrame, key_columns: list, method='drop_duplicates', sort_columns=None):
    """
    对DataFrame进行去重
    
    Args:
        df: Spark DataFrame
        key_columns: 去重的关键字段
        method: 去重方法 ('drop_duplicates' 或 'window_function')
        sort_columns: 排序字段（用于window_function方法）
        
    Returns:
        DataFrame: 去重后的DataFrame
    """
    try:
        if method == 'drop_duplicates':
            return df.dropDuplicates(key_columns)
        elif method == 'window_function':
            if not sort_columns:
                sort_columns = key_columns
                
            # 构建窗口函数
            window_spec = Window.partitionBy(*key_columns)
            for sort_col in sort_columns:
                if sort_col.upper().endswith(' DESC'):
                    col_name = sort_col.replace(' DESC', '').strip()
                    window_spec = window_spec.orderBy(desc(col_name))
                else:
                    col_name = sort_col.replace(' ASC', '').strip()
                    window_spec = window_spec.orderBy(asc(col_name))
            
            # 添加行号并过滤
            df_with_rn = df.withColumn("rn", row_number().over(window_spec))
            return df_with_rn.filter(col("rn") == 1).drop("rn")
        else:
            logging.warning(f"Unknown deduplication method: {method}, using drop_duplicates")
            return df.dropDuplicates(key_columns)
            
    except Exception as e:
        logging.error(f"Failed to deduplicate DataFrame: {e}")
        return df


def validate_duplicate_tolerance(duplicate_stats, config):
    """
    验证重复数据是否在容忍范围内
    
    Args:
        duplicate_stats: 重复数据统计信息
        config: 重复数据防护配置
        
    Returns:
        dict: 验证结果
    """
    tolerance_config = config.get('data_quality_checks', {}).get('duplicate_tolerance', {})
    max_rate = tolerance_config.get('max_duplicate_rate', 0.05)
    warning_rate = tolerance_config.get('warning_duplicate_rate', 0.01)
    
    duplicate_rate = duplicate_stats.get('duplicate_rate', 0.0)
    
    result = {
        'is_valid': True,
        'severity': 'INFO',
        'message': 'Duplicate rate within tolerance',
        'duplicate_rate': duplicate_rate,
        'max_allowed_rate': max_rate
    }
    
    if duplicate_rate > max_rate:
        result.update({
            'is_valid': False,
            'severity': 'ERROR',
            'message': f'Duplicate rate {duplicate_rate:.2%} exceeds maximum allowed {max_rate:.2%}'
        })
    elif duplicate_rate > warning_rate:
        result.update({
            'severity': 'WARNING',
            'message': f'Duplicate rate {duplicate_rate:.2%} exceeds warning threshold {warning_rate:.2%}'
        })
    
    return result


def log_duplicate_analysis(duplicate_stats, validation_result):
    """
    记录重复数据分析结果
    
    Args:
        duplicate_stats: 重复数据统计信息
        validation_result: 验证结果
    """
    severity = validation_result.get('severity', 'INFO')
    message = validation_result.get('message', '')
    
    log_message = f"""
    📊 重复数据分析报告:
    - 总记录数: {duplicate_stats.get('total_records', 0):,}
    - 唯一记录数: {duplicate_stats.get('unique_records', 0):,}
    - 重复记录数: {duplicate_stats.get('duplicate_records', 0):,}
    - 重复率: {duplicate_stats.get('duplicate_rate', 0.0):.4%}
    - 验证结果: {message}
    """
    
    if severity == 'ERROR':
        logging.error(log_message)
    elif severity == 'WARNING':
        logging.warning(log_message)
    else:
        logging.info(log_message)


