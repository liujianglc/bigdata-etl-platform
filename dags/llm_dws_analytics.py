"""
LLM - Airflow DAG Version
使用Agno + DeepSeek大模型分析每日订单明细KPI数据的Airflow DAG
支持Agno API (优先) 和 DeepSeek API (备选)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
import pandas as pd

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def analyze_daily_kpi_with_llm(**context):
    """使用大模型分析每日KPI数据"""
    import os
    import sys
    import json
    import logging
    from datetime import datetime, timedelta
    from typing import Dict, List, Any, Optional
    import pandas as pd
    from pyspark.sql import SparkSession
    import tempfile
    
    spark = None
    try:
        # 创建Spark会话 - 使用与其他DAG相同的配置
        spark = SparkSession.builder \
            .appName("Daily KPI LLM Analysis") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
            .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
            .enableHiveSupport() \
            .getOrCreate()
        
        logging.info("✅ Spark会话创建成功")
        
        # 检查DWS数据库和视图是否存在
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            logging.info(f"可用数据库: {db_names}")
            
            target_db = 'dws_db' if 'dws_db' in db_names else 'default'
            logging.info(f"使用数据库: {target_db}")
            
            spark.sql(f"USE {target_db}")
            
            # 检查视图是否存在
            views = spark.sql("SHOW VIEWS").collect()
            view_names = [view[1] for view in views]
            logging.info(f"{target_db}中的视图: {view_names}")
            
            if 'dws_orderdetails_daily_kpi' not in view_names:
                logging.error(f"❌ 在 {target_db} 数据库中未找到 dws_orderdetails_daily_kpi 视图")
                raise Exception(f"dws_orderdetails_daily_kpi 视图不存在，请先运行 dws_orderdetails_analytics DAG")
            
        except Exception as e:
            logging.error(f"❌ DWS视图检查失败: {e}")
            raise Exception("DWS视图不存在，请先运行 dws_orderdetails_analytics DAG")
        
        # 获取分析天数参数
        days = context.get('params', {}).get('analysis_days', 7)
        logging.info(f"📊 获取最近 {days} 天的KPI数据...")
        
        # 查询KPI数据
        query = f'''
        SELECT 
            order_date,
            total_items,
            total_amount,
            avg_item_value,
            delivery_rate,
            cancellation_rate,
            discount_rate,
            high_value_rate,
            performance_grade
        FROM dws_orderdetails_daily_kpi
        WHERE order_date >= date_sub(current_date(), {days})
        ORDER BY order_date DESC
        '''

        df = spark.sql(query)
        pandas_df = df.toPandas()
        
        if pandas_df.empty:
            logging.warning("⚠️ 没有可用的KPI数据")
            context['task_instance'].xcom_push(key='analysis_status', value='no_data')
            return "没有可用的KPI数据进行分析"
        
        logging.info(f"✅ 获取到 {len(pandas_df)} 天的KPI数据")
        
        # 准备分析上下文
        logging.info("📝 准备分析上下文...")
        context_data = prepare_analysis_context(pandas_df)
        
        # 调用LLM分析
        logging.info("🤖 调用大模型进行分析...")
        model = context.get('params', {}).get('model', 'deepseek-chat')
        analysis = call_llm_analysis(context_data, model)
        
        # 保存分析报告
        logging.info("💾 保存分析报告...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存到临时文件
        temp_dir = tempfile.mkdtemp(prefix='llm_analysis_')
        report_file = os.path.join(temp_dir, f'daily_kpi_analysis_{timestamp}.md')
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"# 订单明细每日KPI分析报告\n\n")
            f.write(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**分析天数**: {days} 天\n")
            f.write(f"**使用模型**: {model}\n\n")
            f.write(analysis)
        
        logging.info(f"✅ 分析报告已保存到: {report_file}")
        
        # 保存到XCom
        context['task_instance'].xcom_push(key='analysis_report_file', value=report_file)
        context['task_instance'].xcom_push(key='analysis_temp_dir', value=temp_dir)
        context['task_instance'].xcom_push(key='analysis_status', value='success')
        context['task_instance'].xcom_push(key='analysis_summary', value={
            'days_analyzed': days,
            'records_count': len(pandas_df),
            'model_used': model,
            'report_file': report_file
        })
        
        logging.info("✅ LLM分析完成!")
        return report_file
        
    except Exception as e:
        logging.error(f"❌ LLM分析失败: {e}")
        context['task_instance'].xcom_push(key='analysis_status', value='failed')
        raise
    finally:
        if spark:
            try:
                spark.stop()
                logging.info("Spark会话已关闭")
            except Exception as e:
                logging.warning(f"关闭Spark会话时出现警告: {e}")

def prepare_analysis_context(df: pd.DataFrame) -> str:
    """准备分析上下文数据"""
    if df.empty:
        return "没有可用的KPI数据进行分析。"
    
    # 基础统计信息
    stats = {
        "数据概览": {
            "分析天数": len(df),
            "日期范围": f"{df['order_date'].min()} 到 {df['order_date'].max()}",
            "总订单明细数": int(df['total_items'].sum()),
            "总交易金额": float(df['total_amount'].sum())
        },
        "关键指标统计": {
            "平均每日订单明细数": float(df['total_items'].mean()),
            "平均每日交易金额": float(df['total_amount'].mean()),
            "平均单项价值": float(df['avg_item_value'].mean()),
            "平均交付率": f"{df['delivery_rate'].mean():.1f}%",
            "平均取消率": f"{df['cancellation_rate'].mean():.1f}%",
            "平均折扣率": f"{df['discount_rate'].mean():.1f}%",
            "平均高价值订单率": f"{df['high_value_rate'].mean():.1f}%"
        },
        "绩效等级分布": df['performance_grade'].value_counts().to_dict(),
        "趋势分析": {
            "订单明细数趋势": "增长" if df['total_items'].iloc[0] > df['total_items'].iloc[-1] else "下降",
            "交易金额趋势": "增长" if df['total_amount'].iloc[0] > df['total_amount'].iloc[-1] else "下降",
            "交付率趋势": "改善" if df['delivery_rate'].iloc[0] > df['delivery_rate'].iloc[-1] else "恶化"
        }
    }
    
    # 构建详细的每日数据
    daily_details = []
    for _, row in df.iterrows():
        daily_details.append({
            "日期": row['order_date'],
            "订单明细数": int(row['total_items']),
            "交易金额": float(row['total_amount']),
            "平均单项价值": float(row['avg_item_value']),
            "交付率": f"{row['delivery_rate']:.1f}%",
            "取消率": f"{row['cancellation_rate']:.1f}%",
            "折扣率": f"{row['discount_rate']:.1f}%",
            "高价值订单率": f"{row['high_value_rate']:.1f}%",
            "绩效等级": row['performance_grade']
        })
    
    context = f'''
# 订单明细每日KPI分析数据

## 数据概览
{json.dumps(stats['数据概览'], ensure_ascii=False, indent=2)}

## 关键指标统计
{json.dumps(stats['关键指标统计'], ensure_ascii=False, indent=2)}

## 绩效等级分布
{json.dumps(stats['绩效等级分布'], ensure_ascii=False, indent=2)}

## 趋势分析
{json.dumps(stats['趋势分析'], ensure_ascii=False, indent=2)}

## 每日详细数据
{json.dumps(daily_details, ensure_ascii=False, indent=2)}
'''
    
    return context

def call_llm_analysis(context: str, model: str = "gpt-4o-mini") -> str:
    """调用大模型进行分析 - 支持Agno和DeepSeek"""
    import logging
    import os
    import requests
    
    try:
        # 构建分析提示
        prompt = f'''
你是一个专业的数据分析师，请基于以下订单明细每日KPI数据进行深入分析：

{context}

请从以下几个维度进行分析：

1. **业务健康度评估**
   - 整体业务表现如何？
   - 关键指标是否在健康范围内？
   - 有哪些积极或消极的信号？

2. **趋势分析**
   - 各项指标的变化趋势如何？
   - 是否存在明显的周期性或季节性模式？
   - 哪些指标需要重点关注？

3. **问题识别**
   - 发现了哪些潜在问题？
   - 哪些指标表现异常？
   - 可能的原因是什么？

4. **改进建议**
   - 针对发现的问题，有什么具体的改进建议？
   - 如何提升关键指标的表现？
   - 有什么预防措施可以采取？

5. **预警和监控**
   - 哪些指标需要设置预警阈值？
   - 建议的监控频率是什么？
   - 如何建立有效的监控体系？

请用中文回答，分析要具体、实用，并提供可执行的建议。
'''

        # 优先尝试使用Agno API
        agno_api_key = os.getenv("AGNO_API_KEY")
        if agno_api_key and agno_api_key != "your-api-key":
            try:
                logging.info("🤖 使用Agno API进行分析...")
                headers = {
                    "Authorization": f"Bearer {agno_api_key}",
                    "Content-Type": "application/json"
                }
                
                data = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "你是一个专业的数据分析师，擅长电商和订单数据分析。"},
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 4000,
                    "temperature": 0.7
                }
                
                response = requests.post(
                    "https://api.agno.com/v1/chat/completions",
                    headers=headers,
                    json=data,
                    timeout=60
                )
                
                if response.status_code == 200:
                    analysis = response.json()["choices"][0]["message"]["content"]
                    logging.info("✅ Agno分析完成")
                    return analysis
                else:
                    logging.warning(f"Agno API调用失败: {response.status_code}, 尝试DeepSeek")
                    
            except Exception as e:
                logging.warning(f"Agno API调用异常: {e}, 尝试DeepSeek")
        
        # 备选：使用DeepSeek API
        deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
        if deepseek_api_key and deepseek_api_key != "your-api-key":
            try:
                logging.info("🤖 使用DeepSeek API进行分析...")
                headers = {
                    "Authorization": f"Bearer {deepseek_api_key}",
                    "Content-Type": "application/json"
                }
                
                data = {
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "你是一个专业的数据分析师，擅长电商和订单数据分析。"},
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 4000,
                    "temperature": 0.7
                }
                
                response = requests.post(
                    "https://api.deepseek.com/v1/chat/completions",
                    headers=headers,
                    json=data,
                    timeout=60
                )
                
                if response.status_code == 200:
                    analysis = response.json()["choices"][0]["message"]["content"]
                    logging.info("✅ DeepSeek分析完成")
                    return analysis
                else:
                    raise Exception(f"DeepSeek API调用失败: {response.status_code} - {response.text}")
                    
            except Exception as e:
                logging.error(f"DeepSeek API调用失败: {e}")
        
        # 如果所有API都不可用，返回基础分析
        logging.warning("⚠️ 所有LLM API都不可用，使用基础分析")
        return generate_basic_analysis(context)
        
    except Exception as e:
        logging.error(f"❌ LLM分析失败: {e}")
        return generate_basic_analysis(context)

def generate_basic_analysis(context: str) -> str:
    """生成基础分析（当LLM不可用时）"""
    return f'''
# 基础数据分析报告

## 数据概览
基于提供的KPI数据，以下是基础分析结果：

{context}

## 基础建议
1. **监控关键指标**: 重点关注交付率和取消率的变化
2. **优化流程**: 如果取消率较高，需要分析取消原因
3. **提升效率**: 关注平均单项价值的变化趋势
4. **质量控制**: 确保绩效等级保持在良好水平

注意: 这是基础分析结果。要获得更深入的洞察，请配置LLM API密钥。
'''

def check_llm_dependencies(**context):
    """检查LLM分析依赖"""
    import logging
    import os
    
    try:
        # 检查环境变量
        agno_api_key = os.getenv("AGNO_API_KEY")
        deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
        
        llm_available = False
        
        if agno_api_key and agno_api_key != "your-api-key":
            logging.info("✅ AGNO_API_KEY 已配置")
            llm_available = True
        elif deepseek_api_key and deepseek_api_key != "your-api-key":
            logging.info("✅ DEEPSEEK_API_KEY 已配置")
            llm_available = True
        else:
            logging.warning("⚠️ 未设置 AGNO_API_KEY 或 DEEPSEEK_API_KEY 环境变量，将使用基础分析")
            logging.info("设置方法: export AGNO_API_KEY='your-api-key' 或 export DEEPSEEK_API_KEY='your-api-key'")
        
        context['task_instance'].xcom_push(key='llm_available', value=llm_available)
        
        # 检查依赖的DAG是否已运行
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("Check LLM Dependencies") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()
        
        try:
            databases = spark.sql("SHOW DATABASES").collect()
            db_names = [db[0] for db in databases]
            
            if 'dws_db' not in db_names:
                raise Exception("DWS数据库不存在，请先运行 dws_orderdetails_analytics DAG")
            
            spark.sql("USE dws_db")
            views = spark.sql("SHOW VIEWS").collect()
            view_names = [view[1] for view in views]
            
            if 'dws_orderdetails_daily_kpi' not in view_names:
                raise Exception("dws_orderdetails_daily_kpi 视图不存在，请先运行 dws_orderdetails_analytics DAG")
            
            logging.info("✅ DWS依赖检查通过")
            context['task_instance'].xcom_push(key='dws_available', value=True)
            
        except Exception as e:
            logging.error(f"❌ DWS依赖检查失败: {e}")
            context['task_instance'].xcom_push(key='dws_available', value=False)
            raise
        finally:
            spark.stop()
        
        return "dependencies_checked"
        
    except Exception as e:
        logging.error(f"依赖检查失败: {e}")
        raise

def publish_analysis_report(**context):
    """发布分析报告"""
    import logging
    import os
    import shutil
    
    try:
        # 获取分析结果
        analysis_status = context['task_instance'].xcom_pull(task_ids='analyze_daily_kpi_with_llm', key='analysis_status')
        
        if analysis_status != 'success':
            logging.warning(f"⚠️ 分析状态异常: {analysis_status}")
            return "analysis_failed"
        
        report_file = context['task_instance'].xcom_pull(task_ids='analyze_daily_kpi_with_llm', key='analysis_report_file')
        temp_dir = context['task_instance'].xcom_pull(task_ids='analyze_daily_kpi_with_llm', key='analysis_temp_dir')
        analysis_summary = context['task_instance'].xcom_pull(task_ids='analyze_daily_kpi_with_llm', key='analysis_summary')
        
        if not report_file or not os.path.exists(report_file):
            logging.error("❌ 分析报告文件不存在")
            return "report_not_found"
        
        # 创建报告目录
        reports_dir = "/opt/airflow/reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # 复制报告到永久位置
        final_report_path = os.path.join(reports_dir, os.path.basename(report_file))
        shutil.copy2(report_file, final_report_path)
        
        logging.info(f"✅ 分析报告已发布到: {final_report_path}")
        logging.info(f"📊 分析摘要: {analysis_summary}")
        
        # 清理临时目录
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logging.info(f"已清理临时目录: {temp_dir}")
            except Exception as e:
                logging.warning(f"清理临时目录失败: {e}")
        
        # 保存发布信息
        context['task_instance'].xcom_push(key='published_report_path', value=final_report_path)
        context['task_instance'].xcom_push(key='publish_status', value='success')
        
        return final_report_path
        
    except Exception as e:
        logging.error(f"发布报告失败: {e}")
        context['task_instance'].xcom_push(key='publish_status', value='failed')
        raise

# 创建DAG
with DAG(
    'llm_dws_analytics',
    default_args=default_args,
    schedule_interval='0 5 * * *',  # 每日凌晨5点执行（在dws_orderdetails_analytics之后）
    catchup=False,
    max_active_runs=1,
    tags=['llm', 'analytics', 'dws', 'kpi', 'ai'],
    description='使用大模型分析DWS层每日KPI数据',
    params={
        'analysis_days': 7,  # 默认分析最近7天
        'model': 'gpt-4o-mini'  # 默认使用的模型 (Agno支持)
    }
) as dag:

    # 开始任务
    start_task = DummyOperator(
        task_id='start_llm_analysis_pipeline',
        doc_md="""
        ## LLM DWS Analytics Pipeline 开始
        
        这个流程使用大模型分析DWS层的每日KPI数据:
        1. 检查LLM和DWS依赖
        2. 从DWS层获取KPI数据
        3. 使用大模型进行深度分析
        4. 生成分析报告
        5. 发布报告到指定位置
        """
    )

    # 1. 依赖检查
    check_deps_task = PythonOperator(
        task_id='check_llm_dependencies',
        python_callable=check_llm_dependencies,
        doc_md="检查LLM API配置和DWS层数据依赖"
    )

    # 2. LLM分析
    llm_analysis_task = PythonOperator(
        task_id='analyze_daily_kpi_with_llm',
        python_callable=analyze_daily_kpi_with_llm,
        doc_md="使用大模型分析每日KPI数据，生成深度分析报告"
    )

    # 3. 发布报告
    publish_task = PythonOperator(
        task_id='publish_analysis_report',
        python_callable=publish_analysis_report,
        doc_md="发布分析报告到指定位置并清理临时文件"
    )

    # 4. 验证报告
    verify_report_task = BashOperator(
        task_id='verify_analysis_report',
        bash_command='''
        echo "=== 验证LLM分析报告 ==="
        REPORTS_DIR="/opt/airflow/reports"
        
        if [ -d "$REPORTS_DIR" ]; then
            echo "报告目录存在: $REPORTS_DIR"
            echo "最新报告文件:"
            ls -la "$REPORTS_DIR"/daily_kpi_analysis_*.md | tail -5
            
            # 检查最新报告内容
            LATEST_REPORT=$(ls -t "$REPORTS_DIR"/daily_kpi_analysis_*.md | head -1)
            if [ -f "$LATEST_REPORT" ]; then
                echo "最新报告: $LATEST_REPORT"
                echo "报告大小: $(wc -c < "$LATEST_REPORT") 字节"
                echo "报告行数: $(wc -l < "$LATEST_REPORT") 行"
                echo "报告预览:"
                head -20 "$LATEST_REPORT"
            else
                echo "❌ 未找到报告文件"
                exit 1
            fi
        else
            echo "❌ 报告目录不存在"
            exit 1
        fi
        
        echo "✅ LLM分析报告验证完成"
        ''',
        doc_md="验证生成的LLM分析报告"
    )

    # 结束任务
    end_task = DummyOperator(
        task_id='end_llm_analysis_pipeline',
        doc_md="LLM DWS Analytics Pipeline 完成"
    )

    # 定义任务依赖关系
    start_task >> check_deps_task >> llm_analysis_task >> publish_task >> verify_report_task >> end_task