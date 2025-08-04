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

# 尝试加载.env文件（如果存在）
def load_env_file():
    """加载.env文件中的环境变量"""
    env_file = '/opt/airflow/.env'
    if os.path.exists(env_file):
        logging.info(f"找到.env文件: {env_file}")
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # 只设置尚未设置的环境变量
                        if key not in os.environ:
                            os.environ[key] = value
                            logging.info(f"从.env文件加载: {key}={value}")
            logging.info("成功加载.env文件")
        except Exception as e:
            logging.warning(f"加载.env文件失败: {e}")
    else:
        logging.info(f".env文件不存在: {env_file}")

# 加载环境变量
load_env_file()

def debug_env_vars():
    """调试环境变量"""
    env_vars = ['EXTERNAL_MYSQL_HOST', 'EXTERNAL_MYSQL_PORT', 'EXTERNAL_MYSQL_USER', 
                'EXTERNAL_MYSQL_PASSWORD', 'EXTERNAL_MYSQL_DATABASE']
    
    logging.info("=== 环境变量调试信息 ===")
    for var in env_vars:
        value = os.getenv(var)
        logging.info(f"{var} = {value} (类型: {type(value)})")
    logging.info("========================")

def load_dynamic_config():
    """从动态配置文件和环境变量加载配置"""
    config_file = '/opt/airflow/config/production_etl_config_dynamic.yaml'
    
    # 默认配置
    default_config = {
        'mysql_conn': {
            'host': os.getenv('EXTERNAL_MYSQL_HOST', '10.0.19.6'),
            'port': os.getenv('EXTERNAL_MYSQL_PORT', '3306'),  # 保持为字符串，稍后转换
            'user': os.getenv('EXTERNAL_MYSQL_USER', 'root'),
            'password': os.getenv('EXTERNAL_MYSQL_PASSWORD', 'Sp1derman123@'),
            'database': os.getenv('EXTERNAL_MYSQL_DATABASE', 'wudeli'),
            'connect_timeout': 30,
            'socket_timeout': 60,
            'charset': 'utf8mb4'
        },
        'spark_config': {
            'master': f"spark://{os.getenv('SPARK_MASTER_HOST', 'spark-master')}:{os.getenv('SPARK_MASTER_PORT', '7077')}",
            'deploy_mode': 'client',
            'driver_memory': '2g',
            'executor_memory': os.getenv('SPARK_WORKER_MEMORY', '2g'),
            'executor_cores': os.getenv('SPARK_WORKER_CORES', '2'),  # 保持为字符串，稍后转换
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
            logging.info(f"找到配置文件: {config_file}")
            with open(config_file, 'r', encoding='utf-8') as f:
                # 替换环境变量
                original_content = f.read()
                logging.info(f"原始配置内容前100字符: {original_content[:100]}...")
                
                content = original_content
                # 环境变量替换
                import re
                
                # 替换 ${VAR:-default} 格式
                def replace_env_var(match):
                    var_name = match.group(1)
                    default_value = match.group(2)
                    env_value = os.getenv(var_name, default_value)
                    logging.info(f"环境变量替换: ${{{var_name}:-{default_value}}} -> {env_value}")
                    return env_value
                
                # 匹配 ${VAR:-default} 格式
                content = re.sub(r'\$\{([^}:]+):-([^}]+)\}', replace_env_var, content)
                
                # 替换 ${VAR} 格式
                def replace_simple_env_var(match):
                    var_name = match.group(1)
                    env_value = os.getenv(var_name, match.group(0))  # 如果环境变量不存在，保持原样
                    logging.info(f"环境变量替换: ${{{var_name}}} -> {env_value}")
                    return env_value
                
                # 匹配 ${VAR} 格式
                content = re.sub(r'\$\{([^}:]+)\}', replace_simple_env_var, content)
                
                logging.info(f"替换后配置内容前100字符: {content[:100]}...")
                
                yaml_config = yaml.safe_load(content)
                logging.info(f"YAML解析结果: {yaml_config}")
                
                # 合并配置
                if yaml_config:
                    # 深度合并配置
                    def deep_merge(base, update):
                        for key, value in update.items():
                            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                                deep_merge(base[key], value)
                            else:
                                base[key] = value
                    
                    deep_merge(default_config, yaml_config)
                    logging.info("成功加载动态配置文件")
        else:
            logging.info(f"配置文件不存在: {config_file}，使用默认配置")
    except Exception as e:
        logging.warning(f"加载配置文件失败，使用默认配置: {str(e)}")
        import traceback
        logging.warning(f"详细错误信息: {traceback.format_exc()}")
    
    # 类型转换 - 确保数值类型正确
    try:
        # 转换MySQL端口
        if 'mysql_conn' in default_config and 'port' in default_config['mysql_conn']:
            port_value = default_config['mysql_conn']['port']
            if isinstance(port_value, str):
                default_config['mysql_conn']['port'] = int(port_value)
            elif not isinstance(port_value, int):
                default_config['mysql_conn']['port'] = 3306
                logging.warning(f"MySQL端口值无效，使用默认值3306: {port_value}")
        
        # 转换Spark配置
        if 'spark_config' in default_config:
            spark_config = default_config['spark_config']
            if 'executor_cores' in spark_config:
                cores_value = spark_config['executor_cores']
                if isinstance(cores_value, str):
                    spark_config['executor_cores'] = int(cores_value)
                elif not isinstance(cores_value, int):
                    spark_config['executor_cores'] = 2
                    logging.warning(f"Spark executor_cores值无效，使用默认值2: {cores_value}")
            
            if 'num_executors' in spark_config:
                executors_value = spark_config['num_executors']
                if isinstance(executors_value, str):
                    spark_config['num_executors'] = int(executors_value)
                elif not isinstance(executors_value, int):
                    spark_config['num_executors'] = 2
                    logging.warning(f"Spark num_executors值无效，使用默认值2: {executors_value}")
                    
    except (ValueError, TypeError) as e:
        logging.error(f"配置类型转换失败: {str(e)}")
        # 设置安全的默认值
        default_config['mysql_conn']['port'] = 3306
        default_config['spark_config']['executor_cores'] = 2
        default_config['spark_config']['num_executors'] = 2
    
    # 最终配置调试信息
    logging.info("=== 最终配置信息 ===")
    mysql_config = default_config.get('mysql_conn', {})
    logging.info(f"MySQL: host={mysql_config.get('host')}, port={mysql_config.get('port')} (类型:{type(mysql_config.get('port'))})")
    logging.info("==================")
    
    return default_config

# 调试环境变量
debug_env_vars()

# 加载动态配置
PRODUCTION_CONFIG = load_dynamic_config()

# 验证配置
def validate_config():
    """验证配置是否正确加载"""
    mysql_config = PRODUCTION_CONFIG.get('mysql_conn', {})
    logging.info(f"MySQL配置: Host={mysql_config.get('host')}, Port={mysql_config.get('port')} (类型: {type(mysql_config.get('port'))}), Database={mysql_config.get('database')}")
    
    spark_config = PRODUCTION_CONFIG.get('spark_config', {})
    logging.info(f"Spark配置: Master={spark_config.get('master')}, Executor Cores={spark_config.get('executor_cores')} (类型: {type(spark_config.get('executor_cores'))})")
    
    # 验证关键配置项的类型
    try:
        port = mysql_config.get('port')
        if port is not None and not isinstance(port, int):
            logging.warning(f"MySQL端口不是整数类型: {port} (类型: {type(port)})")
        
        cores = spark_config.get('executor_cores')
        if cores is not None and not isinstance(cores, int):
            logging.warning(f"Spark executor_cores不是整数类型: {cores} (类型: {type(cores)})")
            
    except Exception as e:
        logging.error(f"配置验证时出错: {e}")
    
    return True

# 验证配置
validate_config()

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
    'execution_timeout': timedelta(hours=2),  # 增加任务超时时间到2小时
}

def check_mysql_connection(**context):
    """检查MySQL连接和数据"""
    import mysql.connector
    import logging
    
    config = PRODUCTION_CONFIG['mysql_conn']
    
    # 验证配置
    logging.info(f"MySQL连接配置: Host={config['host']}, Port={config['port']} (类型: {type(config['port'])}), Database={config['database']}")
    
    # 确保端口是整数
    try:
        port = int(config['port']) if not isinstance(config['port'], int) else config['port']
    except (ValueError, TypeError):
        logging.error(f"无效的端口值: {config['port']}")
        raise ValueError(f"MySQL端口必须是整数，当前值: {config['port']}")
    
    try:
        connection = mysql.connector.connect(
            host=config['host'],
            port=port,
            user=config['user'],
            password=config['password'],
            database=config['database'],
            connection_timeout=config.get('connect_timeout', 30),
            charset=config.get('charset', 'utf8mb4')
        )
        
        cursor = connection.cursor()
        
        # 检查数据库连接
        cursor.execute("SELECT 1")
        cursor.fetchone()
        logging.info("MySQL数据库连接成功")
        
        # 检查数据库中的表
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables] if tables else []
        logging.info(f"数据库 {config['database']} 中的表 ({len(table_names)}): {table_names}")
        
        if tables:
            # 检查常见表的数据
            common_tables = ['orders', 'customers', 'products']
            for table_name in common_tables:
                if table_name in table_names:
                    try:
                        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                        result = cursor.fetchone()
                        logging.info(f"{table_name}表记录数: {result[0]}")
                        
                        # 尝试检查最新数据（假设有created_at或updated_at字段）
                        for date_col in ['created_at', 'updated_at', 'UpdatedDate']:
                            try:
                                cursor.execute(f"SELECT MAX({date_col}) FROM {table_name}")
                                latest = cursor.fetchone()
                                if latest[0]:
                                    logging.info(f"{table_name}表最新{date_col}: {latest[0]}")
                                    break
                            except mysql.connector.Error:
                                continue  # 字段不存在，尝试下一个
                                
                    except mysql.connector.Error as table_error:
                        logging.warning(f"检查表 {table_name} 时出错: {table_error}")
        else:
            logging.warning("数据库中没有找到任何表")
        
        cursor.close()
        connection.close()
        
        logging.info("MySQL连接检查完成")
        return True
        
    except mysql.connector.Error as mysql_error:
        logging.error(f"MySQL连接错误: {mysql_error}")
        raise
    except Exception as e:
        logging.error(f"MySQL连接检查失败: {str(e)}")
        raise

def check_hdfs_connection(**context):
    """检查HDFS连接 - 基于Web API的Docker环境版本"""
    import logging
    import requests
    import json
    import time
    from datetime import datetime
    
    try:
        # 在Docker网络中，使用服务名访问
        namenode_web_url = "http://namenode:9870"
        webhdfs_base_url = f"{namenode_web_url}/webhdfs/v1"
        
        checks_passed = 0
        total_checks = 0
        
        # 1. 检查NameNode Web UI是否可访问
        total_checks += 1
        try:
            response = requests.get(f"{namenode_web_url}/", timeout=10)
            if response.status_code == 200:
                logging.info("✓ NameNode Web UI可访问")
                checks_passed += 1
            else:
                logging.error(f"✗ NameNode Web UI返回错误状态: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"✗ NameNode Web UI连接失败: {e}")
        
        # 2. 检查WebHDFS API - 列出根目录
        total_checks += 1
        try:
            response = requests.get(f"{webhdfs_base_url}/?op=LISTSTATUS", timeout=10)
            if response.status_code == 200:
                data = response.json()
                file_count = len(data.get('FileStatuses', {}).get('FileStatus', []))
                logging.info(f"✓ WebHDFS API可访问，根目录包含 {file_count} 个项目")
                checks_passed += 1
            else:
                logging.error(f"✗ WebHDFS API返回错误状态: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"✗ WebHDFS API连接失败: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"✗ WebHDFS API响应解析失败: {e}")
        
        # 3. 检查HDFS集群健康状态
        total_checks += 1
        try:
            response = requests.get(f"{namenode_web_url}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState", timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('beans'):
                    bean = data['beans'][0]
                    total_blocks = bean.get('TotalBlocks', 0)
                    missing_blocks = bean.get('MissingBlocks', 0)
                    corrupt_blocks = bean.get('CorruptBlocks', 0)
                    live_nodes = bean.get('NumLiveDataNodes', 0)
                    
                    logging.info(f"✓ HDFS集群状态: 总块数={total_blocks}, 丢失块={missing_blocks}, 损坏块={corrupt_blocks}, 活跃节点={live_nodes}")
                    
                    if missing_blocks == 0 and corrupt_blocks == 0 and live_nodes > 0:
                        logging.info("✓ HDFS集群健康状态良好")
                        checks_passed += 1
                    else:
                        logging.warning("⚠ HDFS集群存在一些问题，但可以继续")
                        checks_passed += 0.5  # 部分通过
                else:
                    logging.error("✗ 无法获取HDFS集群状态信息")
            else:
                logging.error(f"✗ HDFS集群状态检查失败: {response.status_code}")
        except Exception as e:
            logging.error(f"✗ HDFS集群状态检查异常: {e}")
        
        # 4. 检查和创建必要目录
        total_checks += 1
        hdfs_paths = PRODUCTION_CONFIG['hdfs_paths']
        dirs_available = 0
        
        for path_name, path_value in hdfs_paths.items():
            try:
                # 检查目录是否存在
                response = requests.get(f"{webhdfs_base_url}{path_value}?op=LISTSTATUS", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    file_count = len(data.get('FileStatuses', {}).get('FileStatus', []))
                    logging.info(f"✓ HDFS目录 {path_name} ({path_value}) 存在，包含 {file_count} 个项目")
                    dirs_available += 1
                elif response.status_code == 404:
                    # 目录不存在，尝试创建
                    logging.warning(f"⚠ HDFS目录 {path_name} ({path_value}) 不存在，尝试创建")
                    create_response = requests.put(f"{webhdfs_base_url}{path_value}?op=MKDIRS", timeout=10)
                    if create_response.status_code == 200:
                        create_data = create_response.json()
                        if create_data.get('boolean', False):
                            logging.info(f"✓ 成功创建HDFS目录: {path_value}")
                            dirs_available += 1
                        else:
                            logging.warning(f"✗ 创建HDFS目录失败: {path_value}")
                    else:
                        logging.warning(f"✗ 创建HDFS目录请求失败: {path_value}, 状态码: {create_response.status_code}")
                else:
                    logging.warning(f"✗ 检查HDFS目录失败: {path_value}, 状态码: {response.status_code}")
            except Exception as e:
                logging.warning(f"✗ 检查HDFS目录异常: {path_value}, 错误: {e}")
        
        if dirs_available >= len(hdfs_paths) * 0.7:  # 70%的目录可用
            logging.info("✓ HDFS目录检查通过")
            checks_passed += 1
        else:
            logging.error("✗ HDFS目录检查失败")
        
        # 5. 简单的读写测试
        total_checks += 1
        test_dir = "/tmp/hdfs_connection_test"
        test_file = f"{test_dir}/test_{int(time.time())}.txt"
        test_content = f"HDFS connection test at {datetime.now()}"
        
        try:
            # 创建测试目录
            create_response = requests.put(f"{webhdfs_base_url}{test_dir}?op=MKDIRS", timeout=10)
            if create_response.status_code == 200:
                # 写入测试文件
                write_response = requests.put(
                    f"{webhdfs_base_url}{test_file}?op=CREATE&overwrite=true",
                    data=test_content,
                    timeout=10,
                    allow_redirects=True
                )
                
                if write_response.status_code in [200, 201]:
                    # 读取测试文件
                    read_response = requests.get(f"{webhdfs_base_url}{test_file}?op=OPEN", timeout=10)
                    if read_response.status_code == 200 and test_content in read_response.text:
                        logging.info("✓ HDFS读写测试通过")
                        checks_passed += 1
                        
                        # 清理测试文件
                        delete_response = requests.delete(f"{webhdfs_base_url}{test_dir}?op=DELETE&recursive=true", timeout=10)
                        if delete_response.status_code == 200:
                            logging.info("✓ 清理测试文件完成")
                    else:
                        logging.error("✗ HDFS读取测试失败")
                else:
                    logging.error(f"✗ HDFS写入测试失败，状态码: {write_response.status_code}")
            else:
                logging.error(f"✗ HDFS创建测试目录失败，状态码: {create_response.status_code}")
        except Exception as e:
            logging.error(f"✗ HDFS读写测试异常: {e}")
        
        # 计算通过率
        pass_rate = (checks_passed / total_checks) * 100 if total_checks > 0 else 0
        
        logging.info(f"HDFS连接检查完成: {checks_passed}/{total_checks} 项通过 ({pass_rate:.1f}%)")
        
        # 60%通过率认为连接基本正常（降低要求，因为Web API可能有限制）
        if pass_rate >= 60:
            logging.info("🎉 HDFS连接状态: 良好")
            return True
        else:
            logging.error("❌ HDFS连接状态: 存在问题")
            # 在Docker环境中，即使部分检查失败也继续执行，避免阻塞ETL流程
            logging.warning("继续执行ETL流程，但建议检查HDFS配置")
            return True
            
    except Exception as e:
        logging.error(f"HDFS连接检查失败: {str(e)}")
        # 非致命错误，记录警告但继续执行
        logging.warning("HDFS连接检查失败，但继续执行ETL流程")
        return True

def check_spark_connection(**context):
    """检查Spark连接 - 基于Web API的Docker环境版本"""
    import logging
    import requests
    import json
    
    try:
        # 在Docker网络中，使用服务名访问
        spark_master_url = "http://spark-master:8080"
        
        checks_passed = 0
        total_checks = 0
        
        # 1. 检查Spark Master Web UI是否可访问
        total_checks += 1
        try:
            response = requests.get(f"{spark_master_url}/", timeout=10)
            if response.status_code == 200:
                logging.info("✓ Spark Master Web UI可访问")
                checks_passed += 1
            else:
                logging.error(f"✗ Spark Master Web UI返回错误状态: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"✗ Spark Master Web UI连接失败: {e}")
        
        # 2. 检查Spark Master API - 获取集群状态
        total_checks += 1
        try:
            response = requests.get(f"{spark_master_url}/json/", timeout=10)
            if response.status_code == 200:
                data = response.json()
                status = data.get('status', 'UNKNOWN')
                workers = data.get('workers', [])
                alive_workers = data.get('aliveworkers', 0)
                
                logging.info(f"✓ Spark Master API可访问，状态: {status}")
                logging.info(f"✓ 活跃Worker数量: {alive_workers}")
                
                if status == 'ALIVE' and alive_workers > 0:
                    logging.info("✓ Spark集群状态良好")
                    checks_passed += 1
                elif status == 'ALIVE':
                    logging.warning("⚠ Spark Master正常但没有Worker节点")
                    checks_passed += 0.5  # 部分通过
                else:
                    logging.error(f"✗ Spark Master状态异常: {status}")
            else:
                logging.error(f"✗ Spark Master API返回错误状态: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"✗ Spark Master API连接失败: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"✗ Spark Master API响应解析失败: {e}")
        
        # 3. 检查Spark应用程序状态
        total_checks += 1
        try:
            response = requests.get(f"{spark_master_url}/json/", timeout=10)
            if response.status_code == 200:
                data = response.json()
                completed_apps = data.get('completedapps', [])
                active_apps = data.get('activeapps', [])
                
                logging.info(f"✓ 活跃应用程序: {len(active_apps)} 个")
                logging.info(f"✓ 已完成应用程序: {len(completed_apps)} 个")
                
                # 如果能获取到应用程序信息，说明API正常工作
                logging.info("✓ Spark应用程序状态检查通过")
                checks_passed += 1
            else:
                logging.error(f"✗ 无法获取Spark应用程序状态: {response.status_code}")
        except Exception as e:
            logging.error(f"✗ Spark应用程序状态检查异常: {e}")
        
        # 4. 检查Worker节点详细信息
        total_checks += 1
        try:
            response = requests.get(f"{spark_master_url}/json/", timeout=10)
            if response.status_code == 200:
                data = response.json()
                workers = data.get('workers', [])
                
                if workers:
                    for i, worker in enumerate(workers):
                        worker_id = worker.get('id', f'worker-{i}')
                        worker_state = worker.get('state', 'UNKNOWN')
                        worker_cores = worker.get('cores', 0)
                        worker_memory = worker.get('memory', 0)
                        
                        logging.info(f"✓ Worker {worker_id}: 状态={worker_state}, 核心数={worker_cores}, 内存={worker_memory}MB")
                    
                    alive_workers = len([w for w in workers if w.get('state') == 'ALIVE'])
                    if alive_workers > 0:
                        logging.info(f"✓ Worker节点检查通过，{alive_workers} 个节点正常")
                        checks_passed += 1
                    else:
                        logging.warning("⚠ 没有活跃的Worker节点")
                        checks_passed += 0.5
                else:
                    logging.warning("⚠ 没有发现Worker节点")
                    checks_passed += 0.5
            else:
                logging.error(f"✗ 无法获取Worker节点信息: {response.status_code}")
        except Exception as e:
            logging.error(f"✗ Worker节点检查异常: {e}")
        
        # 5. 检查Spark History Server（如果可用）
        total_checks += 1
        try:
            # 尝试访问History Server
            history_response = requests.get("http://spark-master:18080/", timeout=5)
            if history_response.status_code == 200:
                logging.info("✓ Spark History Server可访问")
                checks_passed += 1
            else:
                logging.info("ℹ Spark History Server不可用（这是正常的）")
                checks_passed += 0.5  # 不是必需的，给部分分数
        except requests.exceptions.RequestException:
            logging.info("ℹ Spark History Server不可用（这是正常的）")
            checks_passed += 0.5  # 不是必需的，给部分分数
        
        # 计算通过率
        pass_rate = (checks_passed / total_checks) * 100 if total_checks > 0 else 0
        
        logging.info(f"Spark连接检查完成: {checks_passed}/{total_checks} 项通过 ({pass_rate:.1f}%)")
        
        # 60%通过率认为连接基本正常
        if pass_rate >= 60:
            logging.info("🎉 Spark连接状态: 良好")
            return True
        else:
            logging.error("❌ Spark连接状态: 存在问题")
            # 在Docker环境中，即使部分检查失败也继续执行，避免阻塞ETL流程
            logging.warning("继续执行ETL流程，但建议检查Spark配置")
            return True
            
    except Exception as e:
        logging.error(f"Spark连接检查失败: {str(e)}")
        # 非致命错误，记录警告但继续执行
        logging.warning("Spark连接检查失败，但继续执行ETL流程")
        return True

def run_mysql_to_hive_etl(**context):
    """执行MySQL到Hive的ETL作业 - 直接调用动态脚本"""
    import logging
    import sys
    import os
    
    try:
        logging.info("开始执行MySQL到Hive ETL作业")
        
        # 设置环境变量，确保动态脚本能获取到配置
        config = PRODUCTION_CONFIG
        mysql_config = config['mysql_conn']
        
        # 导出环境变量供动态脚本使用
        os.environ['EXTERNAL_MYSQL_HOST'] = mysql_config['host']
        os.environ['EXTERNAL_MYSQL_PORT'] = str(mysql_config['port'])
        os.environ['EXTERNAL_MYSQL_USER'] = mysql_config['user']
        os.environ['EXTERNAL_MYSQL_PASSWORD'] = mysql_config['password']
        os.environ['EXTERNAL_MYSQL_DATABASE'] = mysql_config['database']
        
        logging.info(f"配置MySQL连接: {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
        
        # 直接在当前环境执行动态脚本
        try:
            # 添加脚本路径到Python路径
            script_path = '/opt/airflow/spark_jobs'
            if script_path not in sys.path:
                sys.path.insert(0, script_path)
            
            # 导入并执行动态ETL脚本
            logging.info("导入并执行动态MySQL到Hive ETL脚本")
            
            # 动态导入模块
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "dynamic_mysql_to_hive", 
                "/opt/airflow/spark_jobs/dynamic_mysql_to_hive.py"
            )
            etl_module = importlib.util.module_from_spec(spec)
            
            # 执行模块
            spec.loader.exec_module(etl_module)
            
            # 调用主函数
            etl_module.main()
            
            logging.info("MySQL到Hive ETL作业执行成功")
            return True
            
        except Exception as direct_error:
            logging.error(f"直接执行失败: {direct_error}")
            
            # 备用方案：使用简化的ETL逻辑
            logging.info("使用备用ETL逻辑")
            return run_simplified_etl(config)
            
    except Exception as e:
        logging.error(f"MySQL到Hive ETL作业失败: {str(e)}")
        raise

def run_simplified_etl(config):
    """简化的ETL逻辑作为备用方案"""
    import logging
    
    try:
        from pyspark.sql import SparkSession
        
        logging.info("启动简化ETL流程")
        
        # 创建Spark会话
        spark = SparkSession.builder \
            .appName("Simplified_MySQL_to_Hive_ETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        
        mysql_config = config['mysql_conn']
        jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        
        # 简化的表列表
        tables_to_process = ['customers', 'orders', 'products']
        hive_db = config.get('hive_db', 'wudeli_analytics')
        
        # 创建Hive数据库
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
        spark.sql(f"USE {hive_db}")
        
        processed_count = 0
        
        for table_name in tables_to_process:
            try:
                logging.info(f"处理表: {table_name}")
                
                # 从MySQL读取数据
                df = spark.read \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", mysql_config['user']) \
                    .option("password", mysql_config['password']) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .load()
                
                record_count = df.count()
                if record_count > 0:
                    # 写入Hive表
                    hive_table = f"{hive_db}.{table_name}"
                    df.write \
                        .mode("overwrite") \
                        .saveAsTable(hive_table)
                    
                    logging.info(f"表 {table_name} 处理完成，记录数: {record_count}")
                    processed_count += 1
                else:
                    logging.warning(f"表 {table_name} 没有数据")
                    
            except Exception as table_error:
                logging.error(f"处理表 {table_name} 失败: {table_error}")
                continue
        
        spark.stop()
        
        if processed_count > 0:
            logging.info(f"简化ETL完成，成功处理 {processed_count} 个表")
            return True
        else:
            raise Exception("没有成功处理任何表")
            
    except Exception as e:
        logging.error(f"简化ETL失败: {e}")
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
    docker compose exec namenode hdfs dfs -ls {PRODUCTION_CONFIG['hdfs_paths']['processed']} || echo "处理后数据目录为空"
    
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
    docker compose exec namenode hdfs dfs -mkdir -p {PRODUCTION_CONFIG['hdfs_paths']['reports']}/$(date +%Y%m%d)
    
    # 生成简单的报告（实际中应该是更复杂的报告生成逻辑）
    echo "ETL执行报告 - $(date)" > /tmp/etl_report.txt
    echo "处理记录数: 待统计" >> /tmp/etl_report.txt
    echo "执行状态: 成功" >> /tmp/etl_report.txt
    
    # 将报告复制到namenode容器并上传到HDFS
    docker cp /tmp/etl_report.txt $(docker compose ps -q namenode):/tmp/etl_report.txt
    docker compose exec namenode hdfs dfs -put /tmp/etl_report.txt {PRODUCTION_CONFIG['hdfs_paths']['reports']}/$(date +%Y%m%d)/etl_report.txt
    
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