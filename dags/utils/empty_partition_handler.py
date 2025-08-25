"""
Empty Partition Handler Utility
Handles creation of empty partitions when no data is available for a given date.
"""

import logging
import yaml
import os
from datetime import datetime, timedelta
from pyspark.sql.functions import lit, current_timestamp

def load_empty_partition_config():
    """Load empty partition strategy configuration."""
    config_path = '/opt/airflow/config/empty_partition_strategy.yaml'
    default_config = {
        'empty_partition_strategy': {
            'default_strategy': 'create_empty',
            'tables': {}
        },
        'empty_partition_metadata': {
            'include_etl_timestamp': True,
            'include_batch_id': True,
            'add_empty_flag': True
        }
    }
    
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    else:
        logging.warning(f"Empty partition config {config_path} not found, using defaults.")
        return default_config

def handle_empty_partition(spark, empty_df, table_name, batch_date, context, location):
    """
    Handle empty partition creation based on configuration.
    
    Args:
        spark: SparkSession
        df: Original DataFrame (empty)
        table_name: Target table name
        batch_date: Batch date string
        context: Airflow context
        location: HDFS location for the table
    
    Returns:
        str: Status of the operation
    """
    config = load_empty_partition_config()
    strategy_config = config.get('empty_partition_strategy', {})
    metadata_config = config.get('empty_partition_metadata', {})
    
    # Get strategy for this table
    table_strategy = strategy_config.get('tables', {}).get(table_name, {})
    strategy = table_strategy.get('strategy', strategy_config.get('default_strategy', 'create_empty'))
    
    if strategy == 'skip':
        logging.info(f"Strategy is 'skip' for {table_name}. Skipping partition creation.")
        return 'SKIPPED_EMPTY_DATA'
    
    elif strategy == 'use_previous_day':
        return _handle_previous_day_strategy(spark, table_name, batch_date, table_strategy, context)
    
    elif strategy == 'create_empty':
        return _create_empty_partition(spark, empty_df, table_name, batch_date, context, location, metadata_config)

    else:
        logging.warning(f"Unknown strategy '{strategy}', defaulting to create_empty")
        return _create_empty_partition(spark, empty_df, table_name, batch_date, context, location, metadata_config)


def _create_empty_partition(spark, empty_df, table_name, batch_date, context, location, metadata_config):
    """Create an empty partition with the correct schema."""
    logging.info(f"Creating empty partition for {table_name} on {batch_date}")

    # Clear any existing cache for this table to avoid file conflicts
    try:
        spark.catalog.uncacheTable(table_name)
    except:
        pass  # Table might not be cached

    # Add partition column
    empty_df = empty_df.withColumn('dt', lit(batch_date))

    # Add metadata columns based on configuration
    if metadata_config.get('include_etl_timestamp', True):
        empty_df = empty_df.withColumn('etl_created_date', current_timestamp())

    if metadata_config.get('include_batch_id', True):
        empty_df = empty_df.withColumn('etl_batch_id', lit(context['ds_nodash']))

    if metadata_config.get('add_empty_flag', True):
        empty_df = empty_df.withColumn('is_empty_partition', lit(True))

    # Create database if not exists
    database_name = table_name.split('.')[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    # Write empty partition
    empty_df.write.mode("overwrite") \
           .partitionBy("dt") \
           .format("parquet") \
           .option("path", location) \
           .saveAsTable(table_name)

    # Comprehensive metadata refresh after writing
    try:
        spark.sql(f"MSCK REPAIR TABLE {table_name}")
    except Exception as e:
        logging.warning(f"MSCK REPAIR failed for {table_name}: {e}")

    try:
        spark.sql(f"REFRESH TABLE {table_name}")
    except Exception as e:
        logging.warning(f"REFRESH TABLE failed for {table_name}: {e}")

    # Clear catalog cache to ensure fresh metadata
    try:
        spark.catalog.clearCache()
    except Exception as e:
        logging.warning(f"Clear cache failed: {e}")

    logging.info(f"✅ Empty partition created for {table_name}")
    return 'SUCCESS_EMPTY_PARTITION'


def _handle_previous_day_strategy(spark, table_name, batch_date, table_strategy, context):
    """Try to use data from previous days as fallback."""
    fallback_days = table_strategy.get('fallback_days', 3)
    batch_datetime = datetime.strptime(batch_date, '%Y-%m-%d')
    
    # Clear any existing cache for this table
    try:
        spark.catalog.uncacheTable(table_name)
    except:
        pass
    
    for i in range(1, fallback_days + 1):
        fallback_date = (batch_datetime - timedelta(days=i)).strftime('%Y-%m-%d')
        try:
            # Refresh table metadata before checking partitions
            try:
                spark.sql(f"REFRESH TABLE {table_name}")
            except:
                pass
            
            # Check if partition exists for fallback date
            partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
            available_dates = [p[0].split('=')[1] for p in partitions]
            
            if fallback_date in available_dates:
                logging.info(f"Using data from {fallback_date} for {batch_date}")
                
                # Copy data from previous day with updated partition
                fallback_df = spark.sql(f"SELECT * FROM {table_name} WHERE dt = '{fallback_date}'")
                fallback_df = fallback_df.withColumn('dt', lit(batch_date)) \
                                       .withColumn('etl_created_date', current_timestamp()) \
                                       .withColumn('etl_batch_id', lit(context['ds_nodash'])) \
                                       .withColumn('is_fallback_data', lit(True))
                
                # Write with new partition date
                fallback_df.write.mode("overwrite") \
                          .partitionBy("dt") \
                          .format("parquet") \
                          .saveAsTable(table_name)
                
                # Refresh metadata after writing
                try:
                    spark.sql(f"MSCK REPAIR TABLE {table_name}")
                except Exception as e:
                    logging.warning(f"MSCK REPAIR failed for {table_name}: {e}")
                
                try:
                    spark.sql(f"REFRESH TABLE {table_name}")
                except Exception as e:
                    logging.warning(f"REFRESH TABLE failed for {table_name}: {e}")
                
                try:
                    spark.catalog.clearCache()
                except Exception as e:
                    logging.warning(f"Clear cache failed: {e}")
                
                return 'SUCCESS_FALLBACK_DATA'
                
        except Exception as e:
            logging.warning(f"Failed to check fallback date {fallback_date}: {e}")
            continue
    
    # If no fallback data found, create empty partition
    logging.warning(f"No fallback data found for {table_name}, creating empty partition")
    return 'SUCCESS_EMPTY_PARTITION'

def check_consecutive_empty_days(spark, table_name, batch_date, max_consecutive=3):
    """Check if there have been too many consecutive empty days."""
    try:
        batch_datetime = datetime.strptime(batch_date, '%Y-%m-%d')
        consecutive_count = 0
        
        for i in range(1, max_consecutive + 1):
            check_date = (batch_datetime - timedelta(days=i)).strftime('%Y-%m-%d')
            
            # Check if partition exists and is empty
            try:
                count = spark.sql(f"""
                    SELECT COUNT(*) as cnt 
                    FROM {table_name} 
                    WHERE dt = '{check_date}' 
                    AND (is_empty_partition IS NULL OR is_empty_partition = false)
                """).collect()[0]['cnt']
                
                if count > 0:
                    break  # Found non-empty data
                else:
                    consecutive_count += 1
            except:
                consecutive_count += 1  # Assume empty if partition doesn't exist
        
        if consecutive_count >= max_consecutive:
            logging.warning(f"Found {consecutive_count} consecutive empty days for {table_name}")
            return True
        
        return False
        
    except Exception as e:
        logging.error(f"Failed to check consecutive empty days: {e}")
        return False