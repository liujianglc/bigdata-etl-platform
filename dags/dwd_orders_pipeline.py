from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def extract_orders_data(**context):
    """从Hive提取Orders相关数据（基于mysql_to_hive_sync_dag的备份数据）"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, datediff, year, month, dayofmonth, dayofweek, quarter, coalesce, lit, min, max
    import logging
    import os
    import tempfile
    from datetime import datetime, timedelta
    
    spark = None
    try:
        # 使用与mysql_to_hive_sync_dag相同的Spark配置模式
        spark = SparkSession.builder \
            .appName("DWD Orders Extract from Hive") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.cores", "1") \
            .config("spark.network.timeout", "300s") \
            .config("spark.rpc.askTimeout", "300s") \
            .config("spark.rpc.lookupTimeout", "300s") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.local.dir", "/tmp/spark") \
            .config("spark.worker.dir", "/tmp/spark-worker") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .config("spark.sql.adaptive.skewJoin.enabled", "false") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.host", "localhost") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "2") \
            .enableHiveSupport() \
            .getOrCreate()
        
        # 确保Spark临时目录存在
        spark_temp_dirs = ['/tmp/spark', '/tmp/spark-worker']
        for temp_dir in spark_temp_dirs:
            os.makedirs(temp_dir, exist_ok=True)
        
        logging.info("✅ Spark会话创建成功，开始从Hive读取数据")
        
        # 检查依赖的Hive表是否存在（这些表应该由mysql_to_hive_sync_dag创建）
        required_tables = ['ods.Orders', 'ods.Customers', 'ods.Employees']
        
        # 首先显示所有可用的数据库和表
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            logging.info(f"可用数据库: {[db[0] for db in databases]}")
            
            # 检查ods数据库是否存在
            ods_exists = any(db[0] == 'ods' for db in databases)
            if ods_exists:
                spark.sql("USE ods")
                tables = spark.sql("SHOW TABLES").collect()
                table_names = [table[1] for table in tables]
                logging.info(f"ODS数据库中的表: {table_names}")
            else:
                logging.error("❌ ODS数据库不存在")
                raise Exception("ODS数据库不存在，请先运行 mysql_to_hive_sync_dag")
        except Exception as e:
            logging.error(f"检查数据库时出错: {e}")
        
        # 检查每个必需的表
        missing_tables = []
        for table in required_tables:
            try:
                spark.sql(f"DESCRIBE {table}")
                logging.info(f"✅ 依赖表 {table} 存在")
            except Exception as e:
                logging.error(f"❌ 依赖表 {table} 不存在: {e}")
                missing_tables.append(table)
        
        if missing_tables:
            logging.error(f"缺少以下表: {missing_tables}")
            logging.error("请先运行 mysql_to_hive_sync_dag 同步基础数据表")
            logging.error("或者运行测试脚本: python /opt/airflow/dags/test_mysql_hive_sync.py")
            raise Exception(f"缺少依赖表: {missing_tables}")
        
        # 计算增量数据的时间范围（最近30天的数据）
        batch_date = context['ds']  # 当前批次日期
        start_date = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logging.info(f"提取时间范围: {start_date} 到 {batch_date}")
        
        # 首先检查可用的分区
        try:
            partitions_df = spark.sql("SHOW PARTITIONS ods.Orders")
            available_partitions = [row[0] for row in partitions_df.collect()]
            logging.info(f"Orders表可用分区: {available_partitions[-5:]}")  # 显示最近5个分区
            
            # 检查当前批次分区是否存在
            target_partition = f"dt={batch_date}"
            if target_partition not in available_partitions:
                logging.warning(f"目标分区 {target_partition} 不存在")
                # 使用最新的可用分区
                if available_partitions:
                    latest_partition = available_partitions[-1]
                    latest_date = latest_partition.split('=')[1]
                    logging.info(f"使用最新分区: {latest_partition}")
                    batch_date = latest_date
                else:
                    raise Exception("没有找到任何可用分区")
        except Exception as e:
            logging.warning(f"无法获取分区信息: {e}")
            logging.info("继续使用原始批次日期")
        
        # 从Hive读取Orders数据，关联维度表
        # 先尝试不加分区限制的查询，如果数据量太大再加限制
        orders_query = f"""
        SELECT 
            o.OrderID,
            o.CustomerID,
            COALESCE(c.CustomerName, 'Unknown Customer') as CustomerName,
            COALESCE(c.CustomerType, 'Regular') as CustomerType,
            o.OrderDate,
            o.RequiredDate,
            o.ShippedDate,
            o.Status as OrderStatus,
            o.PaymentMethod,
            o.PaymentStatus,
            o.TotalAmount,
            COALESCE(o.Discount, 0) as OrderDiscount,
            COALESCE(o.ShippingAddress, 'Address Not Provided') as ShippingAddress,
            o.ShippingMethod,
            COALESCE(o.Remarks, '') as Remarks,
            o.CreatedBy,
            COALESCE(e.EmployeeName, 'Unknown Employee') as CreatedByName,
            COALESCE(e.Department, 'Unknown Department') as CreatedByDepartment,
            o.CreatedDate,
            o.UpdatedDate
        FROM ods.Orders o
        LEFT JOIN ods.Customers c ON o.CustomerID = c.CustomerID AND c.dt = '{batch_date}'
        LEFT JOIN ods.Employees e ON o.CreatedBy = e.EmployeeID AND e.dt = '{batch_date}'
        WHERE o.dt = '{batch_date}'
        """
        
        # 执行查询
        logging.info("执行Hive查询...")
        logging.info(f"查询语句: {orders_query}")
        try:
            df = spark.sql(orders_query)
            logging.info("✅ Hive查询执行成功")
        except Exception as e:
            logging.error(f"❌ Hive查询执行失败: {e}")
            logging.error(f"查询语句: {orders_query}")
            
            # 尝试简化查询进行调试
            try:
                logging.info("尝试简化查询进行调试...")
                simple_query = f"SELECT COUNT(*) as cnt FROM ods.Orders WHERE dt = '{batch_date}'"
                simple_result = spark.sql(simple_query).collect()[0]['cnt']
                logging.info(f"Orders表在分区 dt={batch_date} 中有 {simple_result} 条记录")
                
                if simple_result == 0:
                    # 检查所有分区的数据
                    all_data_query = "SELECT COUNT(*) as cnt FROM ods.Orders"
                    all_result = spark.sql(all_data_query).collect()[0]['cnt']
                    logging.info(f"Orders表总共有 {all_result} 条记录")
            except Exception as debug_e:
                logging.error(f"调试查询也失败: {debug_e}")
            
            raise
        
        # 添加计算字段（使用Spark SQL函数）
        logging.info("开始添加计算字段...")
        df = df.withColumn("LeadTimeDays", 
                          when(col("RequiredDate").isNotNull(), 
                               datediff(col("RequiredDate"), col("OrderDate")))
                          .otherwise(None)) \
              .withColumn("ActualLeadTimeDays",
                         when(col("ShippedDate").isNotNull(),
                              datediff(col("ShippedDate"), col("OrderDate")))
                         .otherwise(None)) \
              .withColumn("DeliveryStatus",
                         when(col("OrderStatus").isin(["已取消", "Cancelled"]), "Cancelled")
                         .when(col("OrderStatus").isin(["已完成", "Delivered"]), "Completed")
                         .when((col("ShippedDate").isNotNull()) & 
                              (col("ShippedDate") <= col("RequiredDate")), "OnTime")
                         .when((col("ShippedDate").isNotNull()) & 
                               (col("ShippedDate") > col("RequiredDate")), "Late")
                         .when((col("RequiredDate") < lit(batch_date)) & 
                               col("ShippedDate").isNull(), "Overdue")
                         .otherwise("Pending")) \
              .withColumn("OrderYear", year(col("OrderDate"))) \
              .withColumn("OrderMonth", month(col("OrderDate"))) \
              .withColumn("OrderDay", dayofmonth(col("OrderDate"))) \
              .withColumn("OrderDayOfWeek", dayofweek(col("OrderDate"))) \
              .withColumn("OrderQuarter", quarter(col("OrderDate"))) \
              .withColumn("NetAmount", col("TotalAmount") - col("OrderDiscount")) \
              .withColumn("OrderSizeCategory",
                         when(col("TotalAmount") >= 10000, "Large")
                         .when(col("TotalAmount") >= 5000, "Medium")
                         .when(col("TotalAmount") >= 1000, "Small")
                         .otherwise("Micro"))
        
        # 缓存数据框以提高性能
        logging.info("缓存数据框并计算记录数...")
        df.cache()
        try:
            record_count = df.count()
            logging.info("✅ 数据计算完成")
        except Exception as e:
            logging.error(f"❌ 数据计算失败: {e}")
            # 清理缓存
            df.unpersist()
            raise
        
        logging.info(f"从Hive提取到 {record_count} 条订单记录")
        
        if record_count == 0:
            logging.warning("没有找到符合条件的数据，请检查:")
            logging.warning("1. mysql_to_hive_sync_dag 是否已成功运行")
            logging.warning("2. 源数据表中是否有最近30天的数据")
            logging.warning("3. 分区字段 dt 是否正确")
        else:
            # 显示数据统计
            status_stats = df.groupBy("OrderStatus").count().collect()
            logging.info("订单状态分布:")
            for row in status_stats:
                logging.info(f"  {row['OrderStatus']}: {row['count']} 条")
            
            # 显示时间范围
            date_range = df.select(
                min(col("OrderDate")).alias("min_date"),
                max(col("OrderDate")).alias("max_date")
            ).collect()[0]
            logging.info(f"数据时间范围: {date_range['min_date']} 到 {date_range['max_date']}")
        
        # 将数据转换为Pandas DataFrame并保存到本地文件
        # 这样避免了Spark写入HDFS的权限问题
        logging.info("将Spark DataFrame转换为Pandas DataFrame...")
        pandas_df = df.toPandas()
        
        # 清理Spark缓存
        df.unpersist()
        
        # 创建本地临时文件
        local_temp_dir = tempfile.mkdtemp(prefix='orders_extract_')
        temp_file = os.path.join(local_temp_dir, f'orders_extract_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
        # 使用Pandas保存到本地文件系统
        pandas_df.to_parquet(temp_file, index=False)
        logging.info(f"数据已保存到本地文件: {temp_file}")
        
        # 验证文件是否创建成功
        if os.path.exists(temp_file):
            file_size = os.path.getsize(temp_file)
            logging.info(f"✅ 文件创建成功，大小: {file_size} bytes")
        else:
            raise Exception("本地文件创建失败")
        
        # 保存元数据到XCom
        context['task_instance'].xcom_push(key='extract_file', value=temp_file)
        context['task_instance'].xcom_push(key='temp_dir', value=local_temp_dir)
        context['task_instance'].xcom_push(key='record_count', value=record_count)
        context['task_instance'].xcom_push(key='source_type', value='hive')
        context['task_instance'].xcom_push(key='batch_date', value=batch_date)
        
        return temp_file
        
    except Exception as e:
        logging.error(f"从Hive提取Orders数据失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logging.info("Spark会话已关闭")
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def transform_orders_data(**context):
    """转换Orders数据，应用业务规则和数据清洗"""
    import pandas as pd
    import numpy as np
    import logging
    import yaml
    import os
    from datetime import datetime
    
    try:
        # 加载状态映射配置
        config_path = '/opt/airflow/dags/config/order_status_mapping.yaml'
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logging.info("✅ 成功加载订单状态映射配置")
        else:
            # 使用默认配置
            config = {
                'order_status_mapping': {
                    '待确认': 'Pending', '待处理': 'Pending', '已确认': 'Confirmed',
                    '配送中': 'Shipping', '已发货': 'Shipped', '已完成': 'Delivered', '已取消': 'Cancelled'
                },
                'payment_status_mapping': {
                    '未支付': 'Unpaid', '已支付': 'Paid', '部分支付': 'Partial', '已退款': 'Refunded'
                }
            }
            logging.warning("⚠️ 配置文件不存在，使用默认配置")
        # 获取提取的数据文件
        extract_file = context['task_instance'].xcom_pull(task_ids='extract_orders_data', key='extract_file')
        temp_dir = context['task_instance'].xcom_pull(task_ids='extract_orders_data', key='temp_dir')
        record_count = context['task_instance'].xcom_pull(task_ids='extract_orders_data', key='record_count')
        
        # 检查是否有数据需要转换
        if record_count == 0:
            logging.warning("⚠️ 提取阶段没有数据，创建空的转换结果")
            # 创建空的DataFrame结构
            empty_df = pd.DataFrame()
            
            # 创建空的转换文件
            import tempfile
            transform_temp_dir = tempfile.mkdtemp(prefix='orders_transform_empty_')
            transform_file = os.path.join(transform_temp_dir, f'orders_transform_empty_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
            
            # 保存空文件（需要有基本结构）
            empty_stats = {
                'total_records': 0,
                'quality_distribution': {},
                'status_distribution': {},
                'priority_distribution': {},
                'delayed_orders': 0,
                'avg_processing_days': 0
            }
            
            # 创建一个包含基本列结构的空DataFrame
            columns = ['OrderID', 'CustomerID', 'CustomerName', 'OrderStatus', 'TotalAmount', 
                      'etl_created_date', 'etl_updated_date', 'etl_batch_id', 'etl_source_system']
            empty_df = pd.DataFrame(columns=columns)
            empty_df.to_parquet(transform_file, index=False)
            
            context['task_instance'].xcom_push(key='transform_file', value=transform_file)
            context['task_instance'].xcom_push(key='transform_temp_dir', value=transform_temp_dir)
            context['task_instance'].xcom_push(key='transform_stats', value=empty_stats)
            
            return transform_file
        
        # 读取数据 - 如果是目录则读取整个目录，如果是文件则直接读取
        try:
            if os.path.isdir(extract_file):
                df = pd.read_parquet(extract_file)
            else:
                df = pd.read_parquet(extract_file)
        except Exception as e:
            logging.error(f"读取parquet文件失败: {e}")
            # 尝试从临时目录读取
            if temp_dir and os.path.exists(temp_dir):
                df = pd.read_parquet(temp_dir)
            else:
                raise
        logging.info(f"开始转换 {len(df)} 条订单记录")
        
        # 数据清洗和转换
        
        # 1. 处理空值
        df['CustomerName'] = df['CustomerName'].fillna('Unknown Customer')
        df['CustomerType'] = df['CustomerType'].fillna('Regular')
        df['ShippingAddress'] = df['ShippingAddress'].fillna('Address Not Provided')
        df['Remarks'] = df['Remarks'].fillna('')
        
        # 2. 标准化状态字段（使用配置文件映射）
        status_mapping = config['order_status_mapping']
        original_statuses = df['OrderStatus'].unique()
        logging.info(f"原始订单状态: {original_statuses}")
        
        df['OrderStatus'] = df['OrderStatus'].map(status_mapping).fillna(df['OrderStatus'])
        
        # 记录未映射的状态
        mapped_statuses = list(status_mapping.values())
        unmapped_statuses = df[~df['OrderStatus'].isin(mapped_statuses)]['OrderStatus'].unique()
        if len(unmapped_statuses) > 0:
            logging.warning(f"发现未映射的订单状态: {unmapped_statuses}")
            # 对未映射的状态标记为Unknown
            df.loc[~df['OrderStatus'].isin(mapped_statuses), 'OrderStatus'] = 'Unknown'
        
        # 显示状态映射结果
        final_statuses = df['OrderStatus'].value_counts()
        logging.info(f"状态映射后分布: {final_statuses.to_dict()}")
        
        # 3. 支付状态标准化（使用配置文件映射）
        payment_mapping = config.get('payment_status_mapping', {})
        if payment_mapping:
            original_payment_statuses = df['PaymentStatus'].unique()
            logging.info(f"原始支付状态: {original_payment_statuses}")
            
            df['PaymentStatus'] = df['PaymentStatus'].map(payment_mapping).fillna(df['PaymentStatus'])
            
            # 记录未映射的支付状态
            unmapped_payment = df[~df['PaymentStatus'].isin(payment_mapping.values())]['PaymentStatus'].unique()
            if len(unmapped_payment) > 0:
                logging.warning(f"发现未映射的支付状态: {unmapped_payment}")
        else:
            logging.info("未配置支付状态映射，保持原状态")
        
        # 4. 计算更多业务指标
        
        # 订单处理时长（从创建到发货）
        df['ProcessingDays'] = np.where(
            df['ShippedDate'].notna(),
            (pd.to_datetime(df['ShippedDate']) - pd.to_datetime(df['CreatedDate'])).dt.days,
            None
        )
        
        # 是否延期交付
        df['IsDelayed'] = np.where(
            (df['ShippedDate'].notna()) & (df['RequiredDate'].notna()),
            pd.to_datetime(df['ShippedDate']) > pd.to_datetime(df['RequiredDate']),
            False
        )
        
        # 订单优先级（基于金额和客户类型）
        def calculate_priority(row):
            if row['CustomerType'] == 'VIP':
                return 'High'
            elif row['TotalAmount'] >= 10000:
                return 'High'
            elif row['TotalAmount'] >= 5000:
                return 'Medium'
            else:
                return 'Low'
        
        df['OrderPriority'] = df.apply(calculate_priority, axis=1)
        
        # 5. 添加数据质量标记
        df['DataQualityScore'] = 100
        
        # 扣分规则
        df.loc[df['CustomerName'] == 'Unknown Customer', 'DataQualityScore'] -= 10
        df.loc[df['ShippingAddress'] == 'Address Not Provided', 'DataQualityScore'] -= 15
        df.loc[df['RequiredDate'].isna(), 'DataQualityScore'] -= 5
        df.loc[df['TotalAmount'] <= 0, 'DataQualityScore'] -= 20
        
        # 数据质量等级
        df['DataQualityLevel'] = pd.cut(
            df['DataQualityScore'], 
            bins=[0, 70, 85, 95, 100], 
            labels=['Poor', 'Fair', 'Good', 'Excellent'],
            include_lowest=True
        )
        
        # 6. 添加ETL元数据
        df['etl_created_date'] = datetime.now()
        df['etl_updated_date'] = datetime.now()
        df['etl_batch_id'] = context['ds_nodash']
        df['etl_source_system'] = 'MySQL_ERP'
        
        # 数据验证
        logging.info("数据转换完成，执行质量检查...")
        
        # 检查必填字段
        required_fields = ['OrderID', 'CustomerID', 'OrderDate', 'OrderStatus']
        for field in required_fields:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                logging.warning(f"字段 {field} 有 {null_count} 个空值")
        
        # 检查数据范围
        invalid_amounts = df[df['TotalAmount'] < 0]
        if len(invalid_amounts) > 0:
            logging.warning(f"发现 {len(invalid_amounts)} 条负金额记录")
        
        # 检查日期逻辑
        invalid_dates = df[
            (df['RequiredDate'].notna()) & 
            (pd.to_datetime(df['RequiredDate']) < pd.to_datetime(df['OrderDate']))
        ]
        if len(invalid_dates) > 0:
            logging.warning(f"发现 {len(invalid_dates)} 条要求日期早于订单日期的记录")
        
        # 保存转换后的数据到本地临时文件
        import tempfile
        transform_temp_dir = tempfile.mkdtemp(prefix='orders_transform_')
        transform_file = os.path.join(transform_temp_dir, f'orders_transform_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        df.to_parquet(transform_file, index=False)
        
        # 清理提取阶段的临时目录
        if temp_dir and os.path.exists(temp_dir):
            import shutil
            try:
                shutil.rmtree(temp_dir)
                logging.info(f"已清理提取临时目录: {temp_dir}")
            except Exception as e:
                logging.warning(f"清理提取临时目录失败: {e}")
        
        # 统计信息
        transform_stats = {
            'total_records': len(df),
            'quality_distribution': df['DataQualityLevel'].value_counts().to_dict(),
            'status_distribution': df['OrderStatus'].value_counts().to_dict(),
            'priority_distribution': df['OrderPriority'].value_counts().to_dict(),
            'delayed_orders': df['IsDelayed'].sum(),
            'avg_processing_days': df['ProcessingDays'].mean() if df['ProcessingDays'].notna().any() else 0
        }
        
        logging.info(f"转换统计: {transform_stats}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='transform_file', value=transform_file)
        context['task_instance'].xcom_push(key='transform_temp_dir', value=transform_temp_dir)
        context['task_instance'].xcom_push(key='transform_stats', value=transform_stats)
        
        return transform_file
        
    except Exception as e:
        logging.error(f"Orders数据转换失败: {e}")
        raise

def load_orders_to_hdfs(**context):
    """将转换后的Orders数据加载到HDFS - 使用Spark方式"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit
    import pandas as pd
    import logging
    import os
    import tempfile
    from datetime import datetime
    
    spark = None
    try:
        # 获取转换后的数据文件
        transform_file = context['task_instance'].xcom_pull(task_ids='transform_orders_data', key='transform_file')
        transform_temp_dir = context['task_instance'].xcom_pull(task_ids='transform_orders_data', key='transform_temp_dir')
        
        # 读取Pandas数据
        pandas_df = pd.read_parquet(transform_file)
        logging.info(f"读取到 {len(pandas_df)} 条转换后的记录")
        
        # 检查数据是否为空
        if len(pandas_df) == 0:
            logging.warning("⚠️ 转换后的数据为空，跳过HDFS加载")
            # 创建空的加载摘要
            empty_summary = {
                'total_partitions': 0,
                'total_records': 0,
                'total_size': 0,
                'partitions': [],
                'status': 'SKIPPED_EMPTY_DATA'
            }
            context['task_instance'].xcom_push(key='hdfs_load_summary', value=empty_summary)
            context['task_instance'].xcom_push(key='hdfs_base_path', value='')
            return 'SKIPPED_EMPTY_DATA'
        
        # 创建Spark会话 - 使用与mysql_to_hive_sync_dag相同的配置
        spark = SparkSession.builder \
            .appName("DWD Orders Load to HDFS") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", "1g") \
            .config("spark.driver.maxResultSize", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.cores", "1") \
            .config("spark.network.timeout", "300s") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.local.dir", "/tmp/spark") \
            .config("spark.worker.dir", "/tmp/spark-worker") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.host", "localhost") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.default.parallelism", "2") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logging.info("✅ Spark会话创建成功")
        
        # 确保DWD数据库存在
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd_db")
        spark.sql("USE dwd_db")
        logging.info("✅ DWD数据库已准备就绪")
        
        # 将Pandas DataFrame转换为Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df)
        logging.info("✅ 数据已转换为Spark DataFrame")
        
        # 添加分区列（如果不存在）
        batch_date = context['ds']
        if 'dt' not in spark_df.columns:
            spark_df = spark_df.withColumn('dt', lit(batch_date))
            logging.info(f"添加分区列 dt = {batch_date}")
        
        # HDFS表位置 - 使用与mysql_to_hive_sync_dag相同的路径模式
        table_location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orders"
        logging.info(f"表将存储在 HDFS 位置: {table_location}")
        
        # 检查表是否已存在
        table_exists = False
        try:
            spark.sql("DESCRIBE dwd_orders")
            table_exists = True
            logging.info("DWD Orders表已存在")
        except Exception:
            table_exists = False
            logging.info("DWD Orders表不存在，将创建新表")
        
        # 写入配置 - 使用与mysql_to_hive_sync_dag相同的模式
        write_options = {
            "path": table_location,
            "compression": "snappy"
        }
        
        # 缓存DataFrame以提高性能
        spark_df.cache()
        record_count = spark_df.count()
        logging.info(f"准备写入 {record_count} 条记录")
        
        if table_exists:
            # 对于已存在的表，删除当天分区再写入
            partition_spec = f"dt='{batch_date}'"
            try:
                spark.sql(f"ALTER TABLE dwd_orders DROP IF EXISTS PARTITION ({partition_spec})")
                logging.info(f"删除已存在的分区: {partition_spec}")
            except Exception as e:
                logging.warning(f"删除分区时出现警告: {e}")
            
            # 按分区写入
            spark_df.coalesce(2).write.mode("append").partitionBy("dt").options(**write_options).saveAsTable("dwd_orders")
            logging.info("✅ 增量数据写入完成")
        else:
            # 创建新表
            spark_df.coalesce(2).write.mode("overwrite").partitionBy("dt").options(**write_options).saveAsTable("dwd_orders")
            logging.info("✅ 新表创建完成")
        
        # 清理缓存
        spark_df.unpersist()
        
        # 刷新表元数据
        spark.sql("REFRESH TABLE dwd_orders")
        logging.info("已刷新表元数据")
        
        # 验证写入结果
        final_count = spark.sql("SELECT COUNT(*) FROM dwd_orders").collect()[0][0]
        logging.info(f"✅ 写入验证成功！表中共有 {final_count} 条记录")
        
        # 显示分区信息
        try:
            partitions = spark.sql("SHOW PARTITIONS dwd_orders").collect()
            logging.info(f"表分区信息: {[row[0] for row in partitions[-3:]]}")
        except Exception as e:
            logging.warning(f"无法获取分区信息: {e}")
        
        # 构建加载摘要
        load_summary = {
            'total_partitions': 1,  # 按dt分区，当前批次只有一个分区
            'total_records': record_count,
            'total_size': 0,  # Spark不容易获取确切大小
            'partitions': [{
                'dt': batch_date,
                'records': record_count,
                'path': table_location
            }]
        }
        
        logging.info(f"HDFS加载完成: {load_summary}")
        
        # 清理转换阶段的临时目录
        if transform_temp_dir and os.path.exists(transform_temp_dir):
            import shutil
            try:
                shutil.rmtree(transform_temp_dir)
                logging.info(f"已清理转换临时目录: {transform_temp_dir}")
            except Exception as e:
                logging.warning(f"清理转换临时目录失败: {e}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='hdfs_load_summary', value=load_summary)
        context['task_instance'].xcom_push(key='hdfs_base_path', value=table_location)
        
        return table_location
        
    except Exception as e:
        logging.error(f"HDFS加载失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logging.info("Spark会话已关闭")
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def create_orders_hive_views(**context):
    """创建Orders DWD层Hive视图（表已在load阶段创建）"""
    from pyspark.sql import SparkSession
    import logging
    
    spark = None
    try:
        # 获取HDFS路径信息
        hdfs_base_path = context['task_instance'].xcom_pull(task_ids='load_orders_to_hdfs', key='hdfs_base_path')
        load_summary = context['task_instance'].xcom_pull(task_ids='load_orders_to_hdfs', key='hdfs_load_summary')
        
        # 检查是否跳过了加载（因为数据为空）
        if hdfs_base_path == 'SKIPPED_EMPTY_DATA' or load_summary.get('status') == 'SKIPPED_EMPTY_DATA':
            logging.warning("⚠️ 由于数据为空，跳过视图创建")
            context['task_instance'].xcom_push(key='table_name', value='')
            context['task_instance'].xcom_push(key='views_created', value=0)
            return 'SKIPPED_EMPTY_DATA'
        
        # 创建Spark会话来创建视图
        spark = SparkSession.builder \
            .appName("Create DWD Orders Views") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("USE dwd_db")
        
        # 验证表是否存在
        try:
            table_info = spark.sql("DESCRIBE dwd_orders").collect()
            logging.info(f"✅ DWD Orders表已存在，字段数: {len(table_info)}")
        except Exception as e:
            logging.error(f"❌ DWD Orders表不存在: {e}")
            raise Exception("DWD Orders表不存在，请检查load阶段")
        
        # 创建常用视图
        views_sql = [
            # 1. 当前活跃订单视图
            """
            CREATE OR REPLACE VIEW dwd_orders_active AS
            SELECT *
            FROM dwd_orders
            WHERE OrderStatus IN ('Pending', 'Confirmed', 'Shipping', 'Shipped')
              AND DataQualityLevel IN ('Good', 'Excellent')
            """,
            
            # 2. 延期订单视图  
            """
            CREATE OR REPLACE VIEW dwd_orders_delayed AS
            SELECT *
            FROM dwd_orders
            WHERE IsDelayed = true
              AND OrderStatus NOT IN ('Cancelled', 'Delivered')
            """,
            
            # 3. 各状态订单统计视图
            """
            CREATE OR REPLACE VIEW dwd_orders_status_summary AS
            SELECT 
                OrderStatus,
                COUNT(*) as order_count,
                SUM(TotalAmount) as total_amount,
                AVG(TotalAmount) as avg_amount,
                COUNT(CASE WHEN IsDelayed = true THEN 1 END) as delayed_count
            FROM dwd_orders
            GROUP BY OrderStatus
            """,
            
            # 4. 高价值订单视图
            """
            CREATE OR REPLACE VIEW dwd_orders_high_value AS
            SELECT *
            FROM dwd_orders
            WHERE OrderPriority = 'High'
              AND TotalAmount >= 10000
            """
        ]
        
        # 执行视图创建
        for i, view_sql in enumerate(views_sql, 1):
            try:
                spark.sql(view_sql)
                logging.info(f"✅ 视图 {i} 创建成功")
            except Exception as e:
                logging.warning(f"⚠️ 视图 {i} 创建失败: {e}")
        
        # 验证视图创建结果
        try:
            views = spark.sql("SHOW VIEWS").collect()
            view_names = [row[1] for row in views if 'orders' in row[1]]
            logging.info(f"创建的Orders相关视图: {view_names}")
        except Exception as e:
            logging.warning(f"无法列出视图: {e}")
        
        logging.info("✅ DWD Orders视图创建完成")
        logging.info(f"表位置: {hdfs_base_path}")
        logging.info(f"分区数量: {load_summary['total_partitions']}")
        logging.info(f"总记录数: {load_summary['total_records']}")
        
        # 保存信息到XCom
        context['task_instance'].xcom_push(key='table_name', value='dwd_db.dwd_orders')
        context['task_instance'].xcom_push(key='views_created', value=len(views_sql))
        
        return 'dwd_db.dwd_orders'
        
    except Exception as e:
        logging.error(f"Hive视图创建失败: {e}")
        import traceback
        logging.error(f"详细错误: {traceback.format_exc()}")
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logging.info("Spark会话已关闭")
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def validate_orders_dwd(**context):
    """验证Orders DWD层数据质量"""
    import logging
    
    try:
        # 获取相关信息
        table_name = context['task_instance'].xcom_pull(task_ids='create_orders_hive_views', key='table_name')
        load_summary = context['task_instance'].xcom_pull(task_ids='load_orders_to_hdfs', key='hdfs_load_summary')
        transform_stats = context['task_instance'].xcom_pull(task_ids='transform_orders_data', key='transform_stats')
        
        # 检查是否因为数据为空而跳过了处理
        if (table_name == 'SKIPPED_EMPTY_DATA' or table_name == '' or 
            load_summary.get('status') == 'SKIPPED_EMPTY_DATA'):
            logging.warning("⚠️ 由于数据为空，验证结果标记为警告状态")
            empty_validation = {
                'table_created': False,
                'partitions_loaded': 0,
                'records_loaded': 0,
                'data_quality_check': {},
                'business_rules_check': {},
                'overall_quality_score': 70,  # 警告级别
                'issues': ['没有数据需要处理 - 可能是上游数据源为空或过滤条件过于严格'],
                'validation_status': 'WARNING'
            }
            context['task_instance'].xcom_push(key='validation_results', value=empty_validation)
            return empty_validation
        
        # 数据质量验证规则
        validation_results = {
            'table_created': True,
            'partitions_loaded': load_summary['total_partitions'],
            'records_loaded': load_summary['total_records'],
            'data_quality_check': transform_stats['quality_distribution'],
            'business_rules_check': {
                'delayed_orders': transform_stats['delayed_orders'],
                'avg_processing_days': round(transform_stats['avg_processing_days'], 2),
                'status_distribution': transform_stats['status_distribution'],
                'priority_distribution': transform_stats['priority_distribution']
            }
        }
        
        # 验证结果评估
        quality_score = 100
        issues = []
        
        # 检查数据质量分布
        poor_quality_ratio = transform_stats['quality_distribution'].get('Poor', 0) / transform_stats['total_records']
        if poor_quality_ratio > 0.1:  # 超过10%的数据质量差
            quality_score -= 20
            issues.append(f"数据质量差的记录占比过高: {poor_quality_ratio:.2%}")
        
        # 检查延期订单比例
        if transform_stats['total_records'] > 0:
            delayed_ratio = transform_stats['delayed_orders'] / transform_stats['total_records']
            if delayed_ratio > 0.2:  # 超过20%的订单延期
                quality_score -= 10
                issues.append(f"延期订单比例过高: {delayed_ratio:.2%}")
        
        # 检查分区数量
        if load_summary['total_partitions'] == 0:
            quality_score -= 30
            issues.append("没有成功创建分区")
        
        validation_results['overall_quality_score'] = quality_score
        validation_results['issues'] = issues
        validation_results['validation_status'] = 'PASSED' if quality_score >= 80 else 'WARNING' if quality_score >= 60 else 'FAILED'
        
        # 记录验证结果
        logging.info("=== DWD Orders 数据质量验证结果 ===")
        logging.info(f"表名: {table_name}")
        logging.info(f"总体质量分数: {quality_score}/100")
        logging.info(f"验证状态: {validation_results['validation_status']}")
        logging.info(f"加载分区数: {load_summary['total_partitions']}")
        logging.info(f"加载记录数: {load_summary['total_records']:,}")
        
        if issues:
            logging.warning("发现的问题:")
            for issue in issues:
                logging.warning(f"  - {issue}")
        else:
            logging.info("✅ 所有验证检查通过")
        
        # 保存验证结果
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        
        return validation_results
        
    except Exception as e:
        logging.error(f"数据验证失败: {e}")
        raise

# 创建DAG
with DAG(
    'dwd_orders_pipeline',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # 每日凌晨3点执行（在mysql_to_hive_sync_dag的2点之后）
    catchup=False,
    max_active_runs=1,
    tags=['dwd', 'orders', 'etl', 'data-warehouse', 'downstream'],
    description='DWD层Orders表ETL流程 - 基于Hive备份数据进行订单数据的详细处理和质量保证',
) as dag:

    # 依赖检查任务
    def check_upstream_dependencies(**context):
        """检查上游依赖是否就绪"""
        from pyspark.sql import SparkSession
        import logging
        
        spark = None
        try:
            spark = SparkSession.builder \
                .appName("Check Dependencies") \
                .master("local[1]") \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
                .enableHiveSupport() \
                .getOrCreate()
            
            # 检查必需的表是否存在且有当天数据
            required_tables = ['ods.Orders', 'ods.Customers', 'ods.Employees']
            batch_date = context['ds']
            
            for table in required_tables:
                try:
                    # 检查表是否存在
                    spark.sql(f"DESCRIBE {table}")
                    
                    # 检查是否有当天分区数据
                    count = spark.sql(f"SELECT COUNT(*) FROM {table} WHERE dt = '{batch_date}'").collect()[0][0]
                    
                    if count > 0:
                        logging.info(f"✅ {table} 存在且有数据: {count} 条记录")
                    else:
                        logging.warning(f"⚠️ {table} 存在但当天无数据")
                        
                except Exception as e:
                    logging.error(f"❌ {table} 检查失败: {e}")
                    raise Exception(f"上游依赖 {table} 不可用，请先运行 mysql_to_hive_sync_dag")
            
            logging.info("✅ 所有上游依赖检查通过")
            return "dependencies_ready"
            
        except Exception as e:
            logging.error(f"依赖检查失败: {e}")
            raise
        finally:
            if spark:
                spark.stop()

    check_dependencies_task = PythonOperator(
        task_id='check_upstream_dependencies',
        python_callable=check_upstream_dependencies,
        doc_md="检查上游mysql_to_hive_sync_dag是否已成功运行并产生所需数据"
    )

    # 开始任务
    start_task = DummyOperator(
        task_id='start_dwd_orders_pipeline',
        doc_md="""
        ## DWD Orders Pipeline 开始
        
        这个流程专门处理Orders表的DWD层ETL:
        1. 检查上游依赖（mysql_to_hive_sync_dag的输出）
        2. 从Hive提取Orders及相关维度数据
        3. 应用业务规则和数据清洗
        4. 按年月分区加载到HDFS
        5. 创建Hive表和视图
        6. 执行数据质量验证
        """
    )

    # 1. 数据提取
    extract_task = PythonOperator(
        task_id='extract_orders_data',
        python_callable=extract_orders_data,
        doc_md="从Hive提取Orders表数据（基于mysql_to_hive_sync_dag的备份数据），包含客户和员工维度信息"
    )

    # 2. 数据转换
    transform_task = PythonOperator(
        task_id='transform_orders_data',
        python_callable=transform_orders_data,
        doc_md="应用业务规则，计算衍生字段，执行数据清洗和质量评分"
    )

    # 3. 加载到HDFS
    load_task = PythonOperator(
        task_id='load_orders_to_hdfs',
        python_callable=load_orders_to_hdfs,
        doc_md="将转换后的数据按年月分区加载到HDFS"
    )

    # 4. 创建Hive视图
    create_views_task = PythonOperator(
        task_id='create_orders_hive_views',
        python_callable=create_orders_hive_views,
        doc_md="创建DWD层Orders相关视图（表已在load阶段创建）"
    )

    # 5. 数据验证
    validate_task = PythonOperator(
        task_id='validate_orders_dwd',
        python_callable=validate_orders_dwd,
        doc_md="执行数据质量验证和业务规则检查"
    )

    # 6. HDFS验证
    verify_hdfs_task = BashOperator(
        task_id='verify_hdfs_partitions',
        bash_command='''
        echo "=== 验证HDFS分区结构 ==="
        echo "检查DWD Orders表目录:"
        curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dwd_db/orders?op=LISTSTATUS" | python -m json.tool || echo "目录检查失败"
        
        echo "检查年份分区:"
        curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dwd_db/orders?op=LISTSTATUS&recursive=true" | python -m json.tool || echo "递归检查失败"
        
        echo "HDFS验证完成"
        ''',
        doc_md="验证HDFS中的分区结构和数据文件"
    )

    # 结束任务
    end_task = DummyOperator(
        task_id='end_dwd_orders_pipeline',
        doc_md="DWD Orders Pipeline 完成"
    )

    # 定义任务依赖关系
    start_task >> check_dependencies_task >> extract_task >> transform_task >> load_task >> create_views_task >> validate_task >> verify_hdfs_task >> end_task