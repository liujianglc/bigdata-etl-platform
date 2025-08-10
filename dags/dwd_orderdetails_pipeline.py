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

def extract_orderdetails_data(**context):
    """从Hive提取OrderDetails相关数据（基于mysql_to_hive_sync_dag的备份数据）"""
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
            .appName("DWD OrderDetails Extract from Hive") \
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
        
        # 检查依赖的Hive表是否存在
        required_tables = ['ods.OrderDetails', 'ods.Orders', 'ods.Products', 'ods.Warehouses']
        
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
            raise Exception(f"缺少依赖表: {missing_tables}")
        
        # 计算增量数据的时间范围（最近30天的数据）
        batch_date = context['ds']  # 当前批次日期
        start_date = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logging.info(f"提取时间范围: {start_date} 到 {batch_date}")
        
        # 首先检查可用的分区
        try:
            partitions_df = spark.sql("SHOW PARTITIONS ods.OrderDetails")
            available_partitions = [row[0] for row in partitions_df.collect()]
            logging.info(f"OrderDetails表可用分区: {available_partitions[-5:]}")  # 显示最近5个分区
            
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
        
        # 从Hive读取OrderDetails数据，关联维度表
        orderdetails_query = f"""
        SELECT 
            od.OrderDetailID,
            od.OrderID,
            COALESCE(o.CustomerID, -1) as CustomerID,
            COALESCE(c.CustomerName, 'Unknown Customer') as CustomerName,
            COALESCE(c.CustomerType, 'Regular') as CustomerType,
            od.ProductID,
            COALESCE(p.ProductName, 'Unknown Product') as ProductName,
            COALESCE(p.Category, 'Unknown Category') as ProductCategory,
            COALESCE(p.Specification, '') as ProductSpecification,
            od.Quantity,
            od.UnitPrice,
            COALESCE(od.Discount, 0) as Discount,
            COALESCE(od.Amount, od.Quantity * od.UnitPrice) as Amount,
            od.WarehouseID,
            COALESCE(w.WarehouseName, 'Unknown Warehouse') as WarehouseName,
            COALESCE(w.Manager, 'Unknown Manager') as WarehouseManager,
            COALESCE(f.FactoryName, 'Unknown Factory') as FactoryName,
            COALESCE(f.Location, 'Unknown Location') as FactoryLocation,
            od.Status as OrderDetailStatus,
            COALESCE(o.OrderDate, '1900-01-01') as OrderDate,
            COALESCE(o.Status, 'Unknown') as OrderStatus,
            COALESCE(o.PaymentMethod, 'Unknown') as PaymentMethod,
            COALESCE(o.PaymentStatus, 'Unknown') as PaymentStatus,
            od.CreatedDate,
            od.UpdatedDate
        FROM ods.OrderDetails od
        LEFT JOIN ods.Orders o ON od.OrderID = o.OrderID AND o.dt = '{batch_date}'
        LEFT JOIN ods.Customers c ON o.CustomerID = c.CustomerID AND c.dt = '{batch_date}'
        LEFT JOIN ods.Products p ON od.ProductID = p.ProductID AND p.dt = '{batch_date}'
        LEFT JOIN ods.Warehouses w ON od.WarehouseID = w.WarehouseID AND w.dt = '{batch_date}'
        LEFT JOIN ods.Factories f ON w.FactoryID = f.FactoryID AND f.dt = '{batch_date}'
        WHERE od.dt = '{batch_date}'
        """
        
        # 执行查询
        logging.info("执行Hive查询...")
        logging.info(f"查询语句: {orderdetails_query}")
        try:
            df = spark.sql(orderdetails_query)
            logging.info("✅ Hive查询执行成功")
        except Exception as e:
            logging.error(f"❌ Hive查询执行失败: {e}")
            logging.error(f"查询语句: {orderdetails_query}")
            
            # 尝试简化查询进行调试
            try:
                logging.info("尝试简化查询进行调试...")
                simple_query = f"SELECT COUNT(*) as cnt FROM ods.OrderDetails WHERE dt = '{batch_date}'"
                simple_result = spark.sql(simple_query).collect()[0]['cnt']
                logging.info(f"OrderDetails表在分区 dt={batch_date} 中有 {simple_result} 条记录")
                
                if simple_result == 0:
                    # 检查所有分区的数据
                    all_data_query = "SELECT COUNT(*) as cnt FROM ods.OrderDetails"
                    all_result = spark.sql(all_data_query).collect()[0]['cnt']
                    logging.info(f"OrderDetails表总共有 {all_result} 条记录")
            except Exception as debug_e:
                logging.error(f"调试查询也失败: {debug_e}")
            
            raise
        
        # 添加计算字段（使用Spark SQL函数）
        logging.info("开始添加计算字段...")
        df = df.withColumn("LineTotal", 
                          col("Quantity") * col("UnitPrice")) \
              .withColumn("DiscountAmount",
                         col("LineTotal") * col("Discount") / 100) \
              .withColumn("NetAmount",
                         col("LineTotal") - col("DiscountAmount")) \
              .withColumn("PriceCategory",
                         when(col("UnitPrice") >= 1000, "Premium")
                         .when(col("UnitPrice") >= 500, "High")
                         .when(col("UnitPrice") >= 100, "Medium")
                         .otherwise("Low")) \
              .withColumn("QuantityCategory",
                         when(col("Quantity") >= 100, "Bulk")
                         .when(col("Quantity") >= 50, "Large")
                         .when(col("Quantity") >= 10, "Medium")
                         .otherwise("Small")) \
              .withColumn("OrderYear", year(col("OrderDate"))) \
              .withColumn("OrderMonth", month(col("OrderDate"))) \
              .withColumn("OrderDay", dayofmonth(col("OrderDate"))) \
              .withColumn("OrderDayOfWeek", dayofweek(col("OrderDate"))) \
              .withColumn("OrderQuarter", quarter(col("OrderDate"))) \
              .withColumn("IsHighValue",
                         when(col("NetAmount") >= 10000, True).otherwise(False)) \
              .withColumn("IsDiscounted",
                         when(col("Discount") > 0, True).otherwise(False)) \
              .withColumn("ProfitMarginCategory",
                         when(col("Discount") == 0, "Full Price")
                         .when(col("Discount") <= 5, "Low Discount")
                         .when(col("Discount") <= 15, "Medium Discount")
                         .otherwise("High Discount"))
        
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
        
        logging.info(f"从Hive提取到 {record_count} 条订单明细记录")
        
        if record_count == 0:
            logging.warning("没有找到符合条件的数据，请检查:")
            logging.warning("1. mysql_to_hive_sync_dag 是否已成功运行")
            logging.warning("2. 源数据表中是否有最近30天的数据")
            logging.warning("3. 分区字段 dt 是否正确")
        else:
            # 显示数据统计
            status_stats = df.groupBy("OrderDetailStatus").count().collect()
            logging.info("订单明细状态分布:")
            for row in status_stats:
                logging.info(f"  {row['OrderDetailStatus']}: {row['count']} 条")
            
            # 显示产品类别分布
            category_stats = df.groupBy("ProductCategory").count().collect()
            logging.info("产品类别分布:")
            for row in category_stats[:5]:  # 显示前5个类别
                logging.info(f"  {row['ProductCategory']}: {row['count']} 条")
            
            # 显示时间范围
            date_range = df.select(
                min(col("OrderDate")).alias("min_date"),
                max(col("OrderDate")).alias("max_date")
            ).collect()[0]
            logging.info(f"数据时间范围: {date_range['min_date']} 到 {date_range['max_date']}")
        
        # 将数据转换为Pandas DataFrame并保存到本地文件
        logging.info("将Spark DataFrame转换为Pandas DataFrame...")
        pandas_df = df.toPandas()
        
        # 清理Spark缓存
        df.unpersist()
        
        # 创建本地临时文件
        local_temp_dir = tempfile.mkdtemp(prefix='orderdetails_extract_')
        temp_file = os.path.join(local_temp_dir, f'orderdetails_extract_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
        
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
        logging.error(f"从Hive提取OrderDetails数据失败: {e}")
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

def transform_orderdetails_data(**context):
    """转换OrderDetails数据，应用业务规则和数据清洗"""
    import pandas as pd
    import numpy as np
    import logging
    import yaml
    import os
    from datetime import datetime
    
    try:
        # 加载状态映射配置
        config_path = '/opt/airflow/dags/config/orderdetail_status_mapping.yaml'
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logging.info("✅ 成功加载订单明细状态映射配置")
        else:
            # 使用默认配置
            config = {
                'orderdetail_status_mapping': {
                    '待配货': 'Pending', '配货中': 'Picking', '已配货': 'Picked',
                    '已发货': 'Shipped', '已完成': 'Delivered', '已取消': 'Cancelled'
                },
                'product_category_mapping': {
                    '电子产品': 'Electronics', '服装': 'Clothing', '食品': 'Food',
                    '家具': 'Furniture', '图书': 'Books', '其他': 'Others'
                }
            }
            logging.warning("⚠️ 配置文件不存在，使用默认配置")
        
        # 获取提取的数据文件
        extract_file = context['task_instance'].xcom_pull(task_ids='extract_orderdetails_data', key='extract_file')
        temp_dir = context['task_instance'].xcom_pull(task_ids='extract_orderdetails_data', key='temp_dir')
        record_count = context['task_instance'].xcom_pull(task_ids='extract_orderdetails_data', key='record_count')
        
        # 检查是否有数据需要转换
        if record_count == 0:
            logging.warning("⚠️ 提取阶段没有数据，创建空的转换结果")
            # 创建空的DataFrame结构
            empty_df = pd.DataFrame()
            
            # 创建空的转换文件
            import tempfile
            transform_temp_dir = tempfile.mkdtemp(prefix='orderdetails_transform_empty_')
            transform_file = os.path.join(transform_temp_dir, f'orderdetails_transform_empty_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
            
            # 保存空文件（需要有基本结构）
            empty_stats = {
                'total_records': 0,
                'quality_distribution': {},
                'status_distribution': {},
                'category_distribution': {},
                'high_value_items': 0,
                'avg_unit_price': 0
            }
            
            # 创建一个包含基本列结构的空DataFrame
            columns = ['OrderDetailID', 'OrderID', 'ProductID', 'ProductName', 'Quantity', 'UnitPrice', 'Amount',
                      'etl_created_date', 'etl_updated_date', 'etl_batch_id', 'etl_source_system']
            empty_df = pd.DataFrame(columns=columns)
            empty_df.to_parquet(transform_file, index=False)
            
            context['task_instance'].xcom_push(key='transform_file', value=transform_file)
            context['task_instance'].xcom_push(key='transform_temp_dir', value=transform_temp_dir)
            context['task_instance'].xcom_push(key='transform_stats', value=empty_stats)
            
            return transform_file
        
        # 读取数据
        try:
            if os.path.isdir(extract_file):
                df = pd.read_parquet(extract_file)
            else:
                df = pd.read_parquet(extract_file)
        except Exception as e:
            logging.error(f"读取parquet文件失败: {e}")
            if temp_dir and os.path.exists(temp_dir):
                df = pd.read_parquet(temp_dir)
            else:
                raise
        
        logging.info(f"开始转换 {len(df)} 条订单明细记录")
        
        # 数据清洗和转换
        
        # 1. 处理空值
        df['ProductName'] = df['ProductName'].fillna('Unknown Product')
        df['ProductCategory'] = df['ProductCategory'].fillna('Unknown Category')
        df['ProductSpecification'] = df['ProductSpecification'].fillna('')
        df['WarehouseName'] = df['WarehouseName'].fillna('Unknown Warehouse')
        df['WarehouseManager'] = df['WarehouseManager'].fillna('Unknown Manager')
        df['FactoryName'] = df['FactoryName'].fillna('Unknown Factory')
        df['FactoryLocation'] = df['FactoryLocation'].fillna('Unknown Location')
        
        # 2. 标准化状态字段
        status_mapping = config['orderdetail_status_mapping']
        original_statuses = df['OrderDetailStatus'].unique()
        logging.info(f"原始订单明细状态: {original_statuses}")
        
        df['OrderDetailStatus'] = df['OrderDetailStatus'].map(status_mapping).fillna(df['OrderDetailStatus'])
        
        # 记录未映射的状态
        mapped_statuses = list(status_mapping.values())
        unmapped_statuses = df[~df['OrderDetailStatus'].isin(mapped_statuses)]['OrderDetailStatus'].unique()
        if len(unmapped_statuses) > 0:
            logging.warning(f"发现未映射的订单明细状态: {unmapped_statuses}")
            df.loc[~df['OrderDetailStatus'].isin(mapped_statuses), 'OrderDetailStatus'] = 'Unknown'
        
        # 显示状态映射结果
        final_statuses = df['OrderDetailStatus'].value_counts()
        logging.info(f"状态映射后分布: {final_statuses.to_dict()}")
        
        # 3. 产品类别标准化
        category_mapping = config.get('product_category_mapping', {})
        if category_mapping:
            original_categories = df['ProductCategory'].unique()
            logging.info(f"原始产品类别: {original_categories}")
            
            df['ProductCategory'] = df['ProductCategory'].map(category_mapping).fillna(df['ProductCategory'])
            
            unmapped_categories = df[~df['ProductCategory'].isin(category_mapping.values())]['ProductCategory'].unique()
            if len(unmapped_categories) > 0:
                logging.warning(f"发现未映射的产品类别: {unmapped_categories}")
        
        # 4. 计算更多业务指标
        
        # 单价区间分析
        df['UnitPriceRange'] = pd.cut(
            df['UnitPrice'], 
            bins=[0, 50, 200, 500, 1000, float('inf')], 
            labels=['Very Low', 'Low', 'Medium', 'High', 'Premium'],
            include_lowest=True
        )
        
        # 数量区间分析
        df['QuantityRange'] = pd.cut(
            df['Quantity'], 
            bins=[0, 5, 20, 50, 100, float('inf')], 
            labels=['Very Small', 'Small', 'Medium', 'Large', 'Bulk'],
            include_lowest=True
        )
        
        # 金额区间分析
        df['AmountRange'] = pd.cut(
            df['NetAmount'], 
            bins=[0, 100, 500, 2000, 10000, float('inf')], 
            labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'],
            include_lowest=True
        )
        
        # 折扣效果分析
        df['DiscountEffectiveness'] = np.where(
            df['Discount'] == 0, 'No Discount',
            np.where(df['Discount'] <= 5, 'Low Impact',
                    np.where(df['Discount'] <= 15, 'Medium Impact', 'High Impact'))
        )
        
        # 产品价值密度（单价/规格复杂度的代理指标）
        df['ProductValueDensity'] = np.where(
            df['ProductSpecification'].str.len() > 0,
            df['UnitPrice'] / (df['ProductSpecification'].str.len() + 1),
            df['UnitPrice']
        )
        
        # 仓库效率指标（基于仓库的订单明细处理情况）
        warehouse_efficiency = df.groupby('WarehouseName')['OrderDetailStatus'].apply(
            lambda x: (x == 'Delivered').sum() / len(x) * 100
        ).to_dict()
        df['WarehouseEfficiency'] = df['WarehouseName'].map(warehouse_efficiency)
        
        # 5. 添加数据质量标记
        df['DataQualityScore'] = 100
        
        # 扣分规则
        df.loc[df['ProductName'] == 'Unknown Product', 'DataQualityScore'] -= 15
        df.loc[df['ProductCategory'] == 'Unknown Category', 'DataQualityScore'] -= 10
        df.loc[df['WarehouseName'] == 'Unknown Warehouse', 'DataQualityScore'] -= 10
        df.loc[df['UnitPrice'] <= 0, 'DataQualityScore'] -= 25
        df.loc[df['Quantity'] <= 0, 'DataQualityScore'] -= 25
        df.loc[df['Amount'] != df['LineTotal'], 'DataQualityScore'] -= 5  # 金额计算不一致
        
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
        required_fields = ['OrderDetailID', 'OrderID', 'ProductID', 'Quantity', 'UnitPrice']
        for field in required_fields:
            null_count = df[field].isnull().sum()
            if null_count > 0:
                logging.warning(f"字段 {field} 有 {null_count} 个空值")
        
        # 检查数据范围
        invalid_prices = df[df['UnitPrice'] < 0]
        if len(invalid_prices) > 0:
            logging.warning(f"发现 {len(invalid_prices)} 条负单价记录")
        
        invalid_quantities = df[df['Quantity'] <= 0]
        if len(invalid_quantities) > 0:
            logging.warning(f"发现 {len(invalid_quantities)} 条无效数量记录")
        
        # 检查金额计算逻辑
        amount_mismatch = df[abs(df['Amount'] - df['LineTotal']) > 0.01]
        if len(amount_mismatch) > 0:
            logging.warning(f"发现 {len(amount_mismatch)} 条金额计算不匹配的记录")
        
        # 保存转换后的数据到本地临时文件
        import tempfile
        transform_temp_dir = tempfile.mkdtemp(prefix='orderdetails_transform_')
        transform_file = os.path.join(transform_temp_dir, f'orderdetails_transform_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet')
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
            'status_distribution': df['OrderDetailStatus'].value_counts().to_dict(),
            'category_distribution': df['ProductCategory'].value_counts().to_dict(),
            'high_value_items': df['IsHighValue'].sum(),
            'avg_unit_price': df['UnitPrice'].mean()
        }
        
        logging.info(f"转换统计: {transform_stats}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='transform_file', value=transform_file)
        context['task_instance'].xcom_push(key='transform_temp_dir', value=transform_temp_dir)
        context['task_instance'].xcom_push(key='transform_stats', value=transform_stats)
        
        return transform_file
        
    except Exception as e:
        logging.error(f"OrderDetails数据转换失败: {e}")
        raise

def load_orderdetails_to_hdfs(**context):
    """将转换后的OrderDetails数据加载到HDFS - 使用Spark方式"""
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
        transform_file = context['task_instance'].xcom_pull(task_ids='transform_orderdetails_data', key='transform_file')
        transform_temp_dir = context['task_instance'].xcom_pull(task_ids='transform_orderdetails_data', key='transform_temp_dir')
        
        # 读取Pandas数据
        pandas_df = pd.read_parquet(transform_file)
        logging.info(f"读取到 {len(pandas_df)} 条转换后的记录")
        
        # 检查数据是否为空
        if len(pandas_df) == 0:
            logging.warning("⚠️ 转换后的数据为空，跳过HDFS加载")
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
        
        # 创建Spark会话
        spark = SparkSession.builder \
            .appName("DWD OrderDetails Load to HDFS") \
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
        
        # HDFS表位置
        table_location = "hdfs://namenode:9000/user/hive/warehouse/dwd_db.db/dwd_orderdetails"
        logging.info(f"表将存储在 HDFS 位置: {table_location}")
        
        # 检查表是否已存在
        table_exists = False
        try:
            spark.sql("DESCRIBE dwd_orderdetails")
            table_exists = True
            logging.info("DWD OrderDetails表已存在")
        except Exception:
            table_exists = False
            logging.info("DWD OrderDetails表不存在，将创建新表")
        
        # 写入配置
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
                spark.sql(f"ALTER TABLE dwd_orderdetails DROP IF EXISTS PARTITION ({partition_spec})")
                logging.info(f"删除已存在的分区: {partition_spec}")
            except Exception as e:
                logging.warning(f"删除分区时出现警告: {e}")
            
            # 按分区写入
            spark_df.coalesce(2).write.mode("append").partitionBy("dt").options(**write_options).saveAsTable("dwd_orderdetails")
            logging.info("✅ 增量数据写入完成")
        else:
            # 创建新表
            spark_df.coalesce(2).write.mode("overwrite").partitionBy("dt").options(**write_options).saveAsTable("dwd_orderdetails")
            logging.info("✅ 新表创建完成")
        
        # 清理缓存
        spark_df.unpersist()
        
        # 刷新表元数据
        spark.sql("REFRESH TABLE dwd_orderdetails")
        logging.info("已刷新表元数据")
        
        # 验证写入结果
        final_count = spark.sql("SELECT COUNT(*) FROM dwd_orderdetails").collect()[0][0]
        logging.info(f"✅ 写入验证成功！表中共有 {final_count} 条记录")
        
        # 显示分区信息
        try:
            partitions = spark.sql("SHOW PARTITIONS dwd_orderdetails").collect()
            logging.info(f"表分区信息: {[row[0] for row in partitions[-3:]]}")
        except Exception as e:
            logging.warning(f"无法获取分区信息: {e}")
        
        # 构建加载摘要
        load_summary = {
            'total_partitions': 1,
            'total_records': record_count,
            'total_size': 0,
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

def create_orderdetails_hive_views(**context):
    """创建OrderDetails DWD层Hive视图"""
    from pyspark.sql import SparkSession
    import logging
    
    spark = None
    try:
        # 获取HDFS路径信息
        hdfs_base_path = context['task_instance'].xcom_pull(task_ids='load_orderdetails_to_hdfs', key='hdfs_base_path')
        load_summary = context['task_instance'].xcom_pull(task_ids='load_orderdetails_to_hdfs', key='hdfs_load_summary')
        
        # 检查是否跳过了加载（因为数据为空）
        if hdfs_base_path == 'SKIPPED_EMPTY_DATA' or load_summary.get('status') == 'SKIPPED_EMPTY_DATA':
            logging.warning("⚠️ 由于数据为空，跳过视图创建")
            context['task_instance'].xcom_push(key='table_name', value='')
            context['task_instance'].xcom_push(key='views_created', value=0)
            return 'SKIPPED_EMPTY_DATA'
        
        # 创建Spark会话来创建视图
        spark = SparkSession.builder \
            .appName("Create DWD OrderDetails Views") \
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
            table_info = spark.sql("DESCRIBE dwd_orderdetails").collect()
            logging.info(f"✅ DWD OrderDetails表已存在，字段数: {len(table_info)}")
        except Exception as e:
            logging.error(f"❌ DWD OrderDetails表不存在: {e}")
            raise Exception("DWD OrderDetails表不存在，请检查load阶段")
        
        # 创建常用视图
        views_sql = [
            # 1. 高价值订单明细视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_high_value AS
            SELECT *
            FROM dwd_orderdetails
            WHERE IsHighValue = true
              AND DataQualityLevel IN ('Good', 'Excellent')
            """,
            
            # 2. 折扣商品视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_discounted AS
            SELECT *
            FROM dwd_orderdetails
            WHERE IsDiscounted = true
              AND Discount > 0
            """,
            
            # 3. 产品销售统计视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_product_summary AS
            SELECT 
                ProductID,
                ProductName,
                ProductCategory,
                COUNT(*) as order_count,
                SUM(Quantity) as total_quantity,
                SUM(NetAmount) as total_amount,
                AVG(UnitPrice) as avg_unit_price,
                AVG(Discount) as avg_discount
            FROM dwd_orderdetails
            GROUP BY ProductID, ProductName, ProductCategory
            """,
            
            # 4. 仓库效率视图
            """
            CREATE OR REPLACE VIEW dwd_orderdetails_warehouse_performance AS
            SELECT 
                WarehouseID,
                WarehouseName,
                FactoryName,
                COUNT(*) as total_items,
                SUM(CASE WHEN OrderDetailStatus = 'Delivered' THEN 1 ELSE 0 END) as delivered_items,
                AVG(WarehouseEfficiency) as efficiency_score
            FROM dwd_orderdetails
            GROUP BY WarehouseID, WarehouseName, FactoryName
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
            view_names = [row[1] for row in views if 'orderdetails' in row[1]]
            logging.info(f"创建的OrderDetails相关视图: {view_names}")
        except Exception as e:
            logging.warning(f"无法列出视图: {e}")
        
        logging.info("✅ DWD OrderDetails视图创建完成")
        logging.info(f"表位置: {hdfs_base_path}")
        logging.info(f"分区数量: {load_summary['total_partitions']}")
        logging.info(f"总记录数: {load_summary['total_records']}")
        
        # 保存信息到XCom
        context['task_instance'].xcom_push(key='table_name', value='dwd_db.dwd_orderdetails')
        context['task_instance'].xcom_push(key='views_created', value=len(views_sql))
        
        return 'dwd_db.dwd_orderdetails'
        
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

def validate_orderdetails_dwd(**context):
    """验证OrderDetails DWD层数据质量"""
    import logging
    
    try:
        # 获取相关信息
        table_name = context['task_instance'].xcom_pull(task_ids='create_orderdetails_hive_views', key='table_name')
        load_summary = context['task_instance'].xcom_pull(task_ids='load_orderdetails_to_hdfs', key='hdfs_load_summary')
        transform_stats = context['task_instance'].xcom_pull(task_ids='transform_orderdetails_data', key='transform_stats')
        
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
                'overall_quality_score': 70,
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
                'high_value_items': transform_stats['high_value_items'],
                'avg_unit_price': round(transform_stats['avg_unit_price'], 2),
                'status_distribution': transform_stats['status_distribution'],
                'category_distribution': transform_stats['category_distribution']
            }
        }
        
        # 验证结果评估
        quality_score = 100
        issues = []
        
        # 检查数据质量分布
        poor_quality_ratio = transform_stats['quality_distribution'].get('Poor', 0) / transform_stats['total_records']
        if poor_quality_ratio > 0.1:
            quality_score -= 20
            issues.append(f"数据质量差的记录占比过高: {poor_quality_ratio:.2%}")
        
        # 检查高价值商品比例
        if transform_stats['total_records'] > 0:
            high_value_ratio = transform_stats['high_value_items'] / transform_stats['total_records']
            if high_value_ratio > 0.5:  # 超过50%的商品都是高价值，可能有问题
                quality_score -= 5
                issues.append(f"高价值商品比例异常: {high_value_ratio:.2%}")
        
        # 检查平均单价合理性
        if transform_stats['avg_unit_price'] <= 0:
            quality_score -= 30
            issues.append("平均单价异常")
        elif transform_stats['avg_unit_price'] > 10000:
            quality_score -= 5
            issues.append(f"平均单价过高: {transform_stats['avg_unit_price']:.2f}")
        
        # 检查分区数量
        if load_summary['total_partitions'] == 0:
            quality_score -= 30
            issues.append("没有成功创建分区")
        
        validation_results['overall_quality_score'] = quality_score
        validation_results['issues'] = issues
        validation_results['validation_status'] = 'PASSED' if quality_score >= 80 else 'WARNING' if quality_score >= 60 else 'FAILED'
        
        # 记录验证结果
        logging.info("=== DWD OrderDetails 数据质量验证结果 ===")
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
    'dwd_orderdetails_pipeline',
    default_args=default_args,
    schedule_interval='0 3 * * *',  # 每日凌晨3点执行（在mysql_to_hive_sync_dag的2点之后）
    catchup=False,
    max_active_runs=1,
    tags=['dwd', 'orderdetails', 'etl', 'data-warehouse', 'downstream'],
    description='DWD层OrderDetails表ETL流程 - 基于Hive备份数据进行订单明细数据的详细处理和质量保证',
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
            required_tables = ['ods.OrderDetails', 'ods.Orders', 'ods.Products', 'ods.Warehouses']
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
        task_id='start_dwd_orderdetails_pipeline',
        doc_md="""
        ## DWD OrderDetails Pipeline 开始
        
        这个流程专门处理OrderDetails表的DWD层ETL:
        1. 检查上游依赖（mysql_to_hive_sync_dag的输出）
        2. 从Hive提取OrderDetails及相关维度数据
        3. 应用业务规则和数据清洗
        4. 按年月分区加载到HDFS
        5. 创建Hive表和视图
        6. 执行数据质量验证
        """
    )

    # 1. 数据提取
    extract_task = PythonOperator(
        task_id='extract_orderdetails_data',
        python_callable=extract_orderdetails_data,
        doc_md="从Hive提取OrderDetails表数据，包含订单、产品、仓库等维度信息"
    )

    # 2. 数据转换
    transform_task = PythonOperator(
        task_id='transform_orderdetails_data',
        python_callable=transform_orderdetails_data,
        doc_md="应用业务规则，计算衍生字段，执行数据清洗和质量评分"
    )

    # 3. 加载到HDFS
    load_task = PythonOperator(
        task_id='load_orderdetails_to_hdfs',
        python_callable=load_orderdetails_to_hdfs,
        doc_md="将转换后的数据按年月分区加载到HDFS"
    )

    # 4. 创建Hive视图
    create_views_task = PythonOperator(
        task_id='create_orderdetails_hive_views',
        python_callable=create_orderdetails_hive_views,
        doc_md="创建DWD层OrderDetails相关视图"
    )

    # 5. 数据验证
    validate_task = PythonOperator(
        task_id='validate_orderdetails_dwd',
        python_callable=validate_orderdetails_dwd,
        doc_md="执行数据质量验证和业务规则检查"
    )

    # 6. HDFS验证
    verify_hdfs_task = BashOperator(
        task_id='verify_hdfs_partitions',
        bash_command='''
        echo "=== 验证HDFS分区结构 ==="
        echo "检查DWD OrderDetails表目录:"
        curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dwd_db/orderdetails?op=LISTSTATUS" | python -m json.tool || echo "目录检查失败"
        
        echo "检查年份分区:"
        curl -s "http://namenode:9870/webhdfs/v1/user/hive/warehouse/dwd_db/orderdetails?op=LISTSTATUS&recursive=true" | python -m json.tool || echo "递归检查失败"
        
        echo "HDFS验证完成"
        ''',
        doc_md="验证HDFS中的分区结构和数据文件"
    )

    # 结束任务
    end_task = DummyOperator(
        task_id='end_dwd_orderdetails_pipeline',
        doc_md="DWD OrderDetails Pipeline 完成"
    )

    # 定义任务依赖关系
    start_task >> check_dependencies_task >> extract_task >> transform_task >> load_task >> create_views_task >> validate_task >> verify_hdfs_task >> end_task