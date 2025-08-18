from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

import pandas as pd
from airflow.models import Variable
import logging

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
}

def call_deepseek_api(prompt, system_message, analysis_type="通用", max_tokens=2000):
    """
    统一的DeepSeek API调用函数，包含重试机制和错误处理
    """
    import requests
    import time
    import logging
    
    try:
        deepseek_api_key = Variable.get("DEEPSEEK_API_KEY", default_var="your_deepseek_api_key")
        
        # 从Airflow Variables获取配置，提供默认值
        max_retries = int(Variable.get("DEEPSEEK_MAX_RETRIES", default_var="3"))
        retry_delay = int(Variable.get("DEEPSEEK_RETRY_DELAY", default_var="5"))
        timeout = int(Variable.get("DEEPSEEK_TIMEOUT", default_var="60"))
        
        for attempt in range(max_retries):
            try:
                logging.info(f"{analysis_type}DeepSeek API调用尝试 {attempt + 1}/{max_retries}")
                
                response = requests.post(
                    "https://api.deepseek.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {deepseek_api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": "deepseek-chat",
                        "messages": [
                            {"role": "system", "content": system_message},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.3,
                        "max_tokens": max_tokens
                    },
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    analysis_result = response.json()['choices'][0]['message']['content']
                    logging.info(f"✅ {analysis_type}DeepSeek分析完成")
                    return analysis_result
                else:
                    logging.warning(f"{analysis_type}DeepSeek API返回错误状态码: {response.status_code}, 响应: {response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return f"{analysis_type}LLM分析服务返回错误状态码: {response.status_code}"
                        
            except requests.exceptions.Timeout as e:
                logging.warning(f"{analysis_type}DeepSeek API超时 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    return f"{analysis_type}LLM分析服务超时，请稍后重试。建议检查网络连接或联系管理员。"
                    
            except requests.exceptions.ConnectionError as e:
                logging.warning(f"{analysis_type}DeepSeek API连接错误 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    return f"{analysis_type}LLM分析服务连接失败，请检查网络连接。"
                    
    except Exception as e:
        logging.error(f"{analysis_type}LLM分析失败: {e}")
        return f"{analysis_type}LLM分析过程中出现错误: {str(e)}"

def analyze_daily_kpi_with_llm(**context):
    """使用LLM分析日KPI数据"""
    from pyspark.sql import SparkSession
    import json
    import logging
    from datetime import datetime, timedelta
    
    spark = None
    try:
        # 创建Spark会话
        spark = SparkSession.builder \
            .appName("LLM Daily KPI Analysis") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("USE dws_db")
        
        # 获取最近7天的日KPI数据
        batch_date = context['ds']
        start_date = (datetime.strptime(batch_date, '%Y-%m-%d') - timedelta(days=6)).strftime('%Y-%m-%d')
        
        daily_kpi_query = f"""
        SELECT 
            order_date,
            total_orders,
            total_amount,
            avg_order_value,
            completion_rate,
            cancellation_rate,
            delay_rate,
            performance_grade
        FROM dws_daily_kpi
        WHERE order_date >= '{start_date}' AND order_date <= '{batch_date}'
        ORDER BY order_date DESC
        """
        
        daily_kpi_df = spark.sql(daily_kpi_query)
        daily_kpi_data = daily_kpi_df.collect()
        
        if not daily_kpi_data:
            logging.warning("没有找到日KPI数据")
            return "no_data"
        
        # 准备数据给LLM分析
        kpi_summary = []
        for row in daily_kpi_data:
            kpi_summary.append({
                'date': str(row['order_date']),
                'orders': int(row['total_orders']) if row['total_orders'] else 0,
                'amount': float(row['total_amount']) if row['total_amount'] else 0.0,
                'avg_order_value': float(row['avg_order_value']) if row['avg_order_value'] else 0.0,
                'completion_rate': float(row['completion_rate']) if row['completion_rate'] else 0.0,
                'cancellation_rate': float(row['cancellation_rate']) if row['cancellation_rate'] else 0.0,
                'delay_rate': float(row['delay_rate']) if row['delay_rate'] else 0.0,
                'performance_grade': str(row['performance_grade']) if row['performance_grade'] else 'Unknown'
            })
        
        # 使用agno + deepseek进行分析
        analysis_prompt = f"""
        作为一名资深的数据分析师，请分析以下最近7天的订单KPI数据：

        {json.dumps(kpi_summary, indent=2, ensure_ascii=False)}

        请从以下维度进行深入分析：
        1. 订单量趋势分析（增长/下降趋势，异常波动）
        2. 订单金额变化分析（收入趋势，平均订单价值变化）
        3. 运营效率分析（完成率、取消率、延迟率的表现）
        4. 性能等级分布和变化
        5. 关键风险点识别
        6. 业务改进建议

        请用中文回答，提供具体的数据洞察和可执行的建议。
        """
        
        # 使用统一的API调用函数
        analysis_result = call_deepseek_api(
            prompt=analysis_prompt,
            system_message="你是一名专业的数据分析师，擅长电商订单数据分析。",
            analysis_type="日KPI",
            max_tokens=2000
        )
        
        # 保存分析结果
        analysis_data = {
            'analysis_date': batch_date,
            'data_period': f"{start_date} to {batch_date}",
            'kpi_data': kpi_summary,
            'llm_analysis': analysis_result,
            'analysis_type': 'daily_kpi'
        }
        
        context['task_instance'].xcom_push(key='daily_kpi_analysis', value=analysis_data)
        
        logging.info("✅ 日KPI LLM分析完成")
        return analysis_data
        
    except Exception as e:
        logging.error(f"日KPI分析失败: {e}")
        raise
    finally:
        if spark:
            spark.stop()

def analyze_customer_segments_with_llm(**context):
    """使用LLM分析客户分段数据"""
    from pyspark.sql import SparkSession
    import json
    import logging
    
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("LLM Customer Segments Analysis") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("USE dws_db")
        
        # 获取客户价值分段数据
        customer_segments_query = """
        SELECT 
            customer_segment,
            customer_count,
            segment_revenue,
            avg_customer_value,
            avg_orders_per_customer,
            avg_order_value,
            active_customers,
            ROUND(active_customers * 100.0 / customer_count, 2) as active_rate
        FROM dws_customer_value_segments
        ORDER BY 
            CASE customer_segment 
                WHEN 'Platinum' THEN 1
                WHEN 'Gold' THEN 2
                WHEN 'Silver' THEN 3
                WHEN 'Bronze' THEN 4
            END
        """
        
        segments_df = spark.sql(customer_segments_query)
        segments_data = segments_df.collect()
        
        # 获取客户状态分析数据
        customer_status_query = """
        SELECT 
            customer_status,
            customer_count,
            status_revenue,
            avg_days_since_last_order,
            avg_monthly_frequency
        FROM dws_customer_status_analysis
        ORDER BY customer_count DESC
        """
        
        status_df = spark.sql(customer_status_query)
        status_data = status_df.collect()
        
        if not segments_data and not status_data:
            logging.warning("没有找到客户分析数据")
            return "no_data"
        
        # 准备数据给LLM分析
        segments_summary = []
        for row in segments_data:
            segments_summary.append({
                'segment': str(row['customer_segment']),
                'customer_count': int(row['customer_count']) if row['customer_count'] else 0,
                'segment_revenue': float(row['segment_revenue']) if row['segment_revenue'] else 0.0,
                'avg_customer_value': float(row['avg_customer_value']) if row['avg_customer_value'] else 0.0,
                'avg_orders_per_customer': float(row['avg_orders_per_customer']) if row['avg_orders_per_customer'] else 0.0,
                'avg_order_value': float(row['avg_order_value']) if row['avg_order_value'] else 0.0,
                'active_customers': int(row['active_customers']) if row['active_customers'] else 0,
                'active_rate': float(row['active_rate']) if row['active_rate'] else 0.0
            })
        
        status_summary = []
        for row in status_data:
            status_summary.append({
                'status': str(row['customer_status']),
                'customer_count': int(row['customer_count']) if row['customer_count'] else 0,
                'status_revenue': float(row['status_revenue']) if row['status_revenue'] else 0.0,
                'avg_days_since_last_order': float(row['avg_days_since_last_order']) if row['avg_days_since_last_order'] else 0.0,
                'avg_monthly_frequency': float(row['avg_monthly_frequency']) if row['avg_monthly_frequency'] else 0.0
            })
        
        # 使用agno + deepseek进行分析
        analysis_prompt = f"""
        作为一名客户关系管理专家，请分析以下客户分段和状态数据：

        客户价值分段数据：
        {json.dumps(segments_summary, indent=2, ensure_ascii=False)}

        客户状态分析数据：
        {json.dumps(status_summary, indent=2, ensure_ascii=False)}

        请从以下维度进行深入分析：
        1. 客户价值分段分析（各分段的贡献度、特征、价值密度）
        2. 客户活跃度分析（活跃率、流失风险、生命周期状态）
        3. 收入结构分析（各分段和状态的收入贡献）
        4. 客户行为模式识别（订单频次、消费习惯）
        5. 客户流失风险评估
        6. 客户价值提升策略建议
        7. 精准营销建议

        请用中文回答，提供具体的客户洞察和可执行的CRM策略。
        """
        
        # 使用统一的API调用函数
        analysis_result = call_deepseek_api(
            prompt=analysis_prompt,
            system_message="你是一名专业的客户关系管理专家，擅长客户分析和CRM策略制定。",
            analysis_type="客户分段",
            max_tokens=2500
        )
        
        # 保存分析结果
        analysis_data = {
            'analysis_date': context['ds'],
            'segments_data': segments_summary,
            'status_data': status_summary,
            'llm_analysis': analysis_result,
            'analysis_type': 'customer_segments'
        }
        
        context['task_instance'].xcom_push(key='customer_segments_analysis', value=analysis_data)
        
        logging.info("✅ 客户分段LLM分析完成")
        return analysis_data
        
    except Exception as e:
        logging.error(f"客户分段分析失败: {e}")
        raise
    finally:
        if spark:
            spark.stop()

def analyze_monthly_trends_with_llm(**context):
    """使用LLM分析月度趋势数据"""
    from pyspark.sql import SparkSession
    import json
    import logging
    from datetime import datetime, timedelta
    
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("LLM Monthly Trends Analysis") \
            .master("local[1]") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.ui.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sql("USE dws_db")
        
        # 获取最近6个月的月度趋势数据
        batch_date = context['ds']
        current_date = datetime.strptime(batch_date, '%Y-%m-%d')
        
        monthly_trends_query = """
        SELECT 
            year_month,
            total_orders,
            total_amount,
            avg_order_value,
            vip_ratio,
            large_order_ratio,
            completion_rate,
            prev_month_orders,
            prev_month_amount,
            CASE 
                WHEN prev_month_orders > 0 THEN 
                    ROUND((total_orders - prev_month_orders) * 100.0 / prev_month_orders, 2)
                ELSE NULL 
            END as orders_growth_rate,
            CASE 
                WHEN prev_month_amount > 0 THEN 
                    ROUND((total_amount - prev_month_amount) * 100.0 / prev_month_amount, 2)
                ELSE NULL 
            END as amount_growth_rate
        FROM dws_orders_monthly_trend
        ORDER BY year_month DESC
        LIMIT 6
        """
        
        trends_df = spark.sql(monthly_trends_query)
        trends_data = trends_df.collect()
        
        # 获取月度业务指标数据
        business_metrics_query = """
        SELECT 
            year_month,
            total_orders,
            total_amount,
            vip_orders,
            large_orders,
            vip_percentage,
            large_order_percentage,
            completion_percentage,
            avg_processing_days
        FROM dws_monthly_business_metrics
        ORDER BY year_month DESC
        LIMIT 6
        """
        
        metrics_df = spark.sql(business_metrics_query)
        metrics_data = metrics_df.collect()
        
        if not trends_data and not metrics_data:
            logging.warning("没有找到月度趋势数据")
            return "no_data"
        
        # 准备数据给LLM分析
        trends_summary = []
        for row in trends_data:
            trends_summary.append({
                'month': str(row['year_month']),
                'total_orders': int(row['total_orders']) if row['total_orders'] else 0,
                'total_amount': float(row['total_amount']) if row['total_amount'] else 0.0,
                'avg_order_value': float(row['avg_order_value']) if row['avg_order_value'] else 0.0,
                'vip_ratio': float(row['vip_ratio']) if row['vip_ratio'] else 0.0,
                'large_order_ratio': float(row['large_order_ratio']) if row['large_order_ratio'] else 0.0,
                'completion_rate': float(row['completion_rate']) if row['completion_rate'] else 0.0,
                'orders_growth_rate': float(row['orders_growth_rate']) if row['orders_growth_rate'] else None,
                'amount_growth_rate': float(row['amount_growth_rate']) if row['amount_growth_rate'] else None
            })
        
        metrics_summary = []
        for row in metrics_data:
            metrics_summary.append({
                'month': str(row['year_month']),
                'vip_orders': int(row['vip_orders']) if row['vip_orders'] else 0,
                'large_orders': int(row['large_orders']) if row['large_orders'] else 0,
                'vip_percentage': float(row['vip_percentage']) if row['vip_percentage'] else 0.0,
                'large_order_percentage': float(row['large_order_percentage']) if row['large_order_percentage'] else 0.0,
                'completion_percentage': float(row['completion_percentage']) if row['completion_percentage'] else 0.0,
                'avg_processing_days': float(row['avg_processing_days']) if row['avg_processing_days'] else 0.0
            })
        
        # 使用agno + deepseek进行分析
        analysis_prompt = f"""
        作为一名业务分析专家，请分析以下最近6个月的订单趋势数据：

        月度趋势数据：
        {json.dumps(trends_summary, indent=2, ensure_ascii=False)}

        月度业务指标数据：
        {json.dumps(metrics_summary, indent=2, ensure_ascii=False)}

        请从以下维度进行深入分析：
        1. 订单量和收入趋势分析（增长模式、季节性特征、异常波动）
        2. 平均订单价值变化分析（价值提升/下降原因）
        3. VIP客户和大订单占比趋势（高价值客户发展情况）
        4. 运营效率趋势（完成率、处理时间变化）
        5. 增长率分析（环比增长的稳定性和可持续性）
        6. 业务健康度评估
        7. 未来趋势预测和战略建议

        请用中文回答，提供具体的趋势洞察和战略建议。
        """
        
        # 使用统一的API调用函数
        analysis_result = call_deepseek_api(
            prompt=analysis_prompt,
            system_message="你是一名专业的业务分析专家，擅长趋势分析和战略规划。",
            analysis_type="月度趋势",
            max_tokens=2500
        )
        
        # 保存分析结果
        analysis_data = {
            'analysis_date': context['ds'],
            'trends_data': trends_summary,
            'metrics_data': metrics_summary,
            'llm_analysis': analysis_result,
            'analysis_type': 'monthly_trends'
        }
        
        context['task_instance'].xcom_push(key='monthly_trends_analysis', value=analysis_data)
        
        logging.info("✅ 月度趋势LLM分析完成")
        return analysis_data
        
    except Exception as e:
        logging.error(f"月度趋势分析失败: {e}")
        raise
    finally:
        if spark:
            spark.stop()

def generate_comprehensive_report(**context):
    """生成综合分析报告"""
    import logging
    from datetime import datetime
    
    try:
        # 获取所有分析结果
        daily_kpi_analysis = context['task_instance'].xcom_pull(task_ids='analyze_daily_kpi_with_llm', key='daily_kpi_analysis')
        customer_segments_analysis = context['task_instance'].xcom_pull(task_ids='analyze_customer_segments_with_llm', key='customer_segments_analysis')
        monthly_trends_analysis = context['task_instance'].xcom_pull(task_ids='analyze_monthly_trends_with_llm', key='monthly_trends_analysis')
        
        # 生成HTML报告
        report_html = f"""
        <!DOCTYPE html>
        <html lang="zh-CN">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>订单数据智能分析报告 - {context['ds']}</title>
            <style>
                body {{ font-family: 'Microsoft YaHei', Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .header {{ text-align: center; margin-bottom: 30px; padding-bottom: 20px; border-bottom: 2px solid #e0e0e0; }}
                .header h1 {{ color: #2c3e50; margin-bottom: 10px; }}
                .header p {{ color: #7f8c8d; font-size: 16px; }}
                .section {{ margin-bottom: 40px; }}
                .section h2 {{ color: #34495e; border-left: 4px solid #3498db; padding-left: 15px; margin-bottom: 20px; }}
                .analysis-content {{ background-color: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #17a2b8; }}
                .data-summary {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; margin-bottom: 15px; }}
                .footer {{ text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #e0e0e0; color: #7f8c8d; }}
                .highlight {{ background-color: #fff3cd; padding: 10px; border-radius: 5px; border-left: 4px solid #ffc107; margin: 10px 0; }}
                pre {{ background-color: #f4f4f4; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🤖 订单数据智能分析报告</h1>
                    <p>基于DeepSeek大模型的深度数据洞察 | 分析日期: {context['ds']}</p>
                </div>
        """
        
        # 添加日KPI分析
        if daily_kpi_analysis and daily_kpi_analysis != "no_data":
            report_html += f"""
                <div class="section">
                    <h2>📊 日度KPI分析</h2>
                    <div class="data-summary">
                        <strong>分析周期:</strong> {daily_kpi_analysis.get('data_period', 'N/A')}<br>
                        <strong>数据点数:</strong> {len(daily_kpi_analysis.get('kpi_data', []))} 天
                    </div>
                    <div class="analysis-content">
                        <pre>{daily_kpi_analysis.get('llm_analysis', '分析结果不可用')}</pre>
                    </div>
                </div>
            """
        
        # 添加客户分段分析
        if customer_segments_analysis and customer_segments_analysis != "no_data":
            report_html += f"""
                <div class="section">
                    <h2>👥 客户分段分析</h2>
                    <div class="data-summary">
                        <strong>分析日期:</strong> {customer_segments_analysis.get('analysis_date', 'N/A')}<br>
                        <strong>客户分段数:</strong> {len(customer_segments_analysis.get('segments_data', []))}<br>
                        <strong>状态分类数:</strong> {len(customer_segments_analysis.get('status_data', []))}
                    </div>
                    <div class="analysis-content">
                        <pre>{customer_segments_analysis.get('llm_analysis', '分析结果不可用')}</pre>
                    </div>
                </div>
            """
        
        # 添加月度趋势分析
        if monthly_trends_analysis and monthly_trends_analysis != "no_data":
            report_html += f"""
                <div class="section">
                    <h2>📈 月度趋势分析</h2>
                    <div class="data-summary">
                        <strong>分析日期:</strong> {monthly_trends_analysis.get('analysis_date', 'N/A')}<br>
                        <strong>趋势数据月数:</strong> {len(monthly_trends_analysis.get('trends_data', []))}<br>
                        <strong>业务指标月数:</strong> {len(monthly_trends_analysis.get('metrics_data', []))}
                    </div>
                    <div class="analysis-content">
                        <pre>{monthly_trends_analysis.get('llm_analysis', '分析结果不可用')}</pre>
                    </div>
                </div>
            """
        
        # 添加报告尾部
        report_html += f"""
                <div class="highlight">
                    <strong>💡 报告说明:</strong><br>
                    • 本报告基于DWS层订单分析数据，使用DeepSeek大模型进行智能分析<br>
                    • 分析结果仅供参考，具体决策请结合业务实际情况<br>
                    • 如有疑问，请联系数据团队进行详细解读
                </div>
                
                <div class="footer">
                    <p>报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 数据团队出品</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # 保存报告
        context['task_instance'].xcom_push(key='comprehensive_report', value=report_html)
        
        logging.info("✅ 综合分析报告生成完成")
        return report_html
        
    except Exception as e:
        logging.error(f"报告生成失败: {e}")
        raise

# 创建DAG
with DAG(
    'llm_orders_analytics',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # 每日早上6点执行（在dws_orders_analytics之后）
    catchup=False,
    max_active_runs=1,
    tags=['llm', 'analytics', 'orders', 'deepseek', 'ai-insights'],
    description='使用DeepSeek大模型分析DWS层订单数据，生成智能洞察报告',
) as dag:

    # 开始任务
    start_task = DummyOperator(
        task_id='start_llm_analytics',
        doc_md="""
        ## LLM订单数据智能分析流程
        
        使用DeepSeek大模型对DWS层订单分析数据进行深度洞察：
        1. 分析日度KPI趋势和异常
        2. 分析客户分段和价值分布
        3. 分析月度业务趋势
        4. 生成综合智能报告
        5. 发送邮件通知相关人员
        """
    )

    # 检查DWS依赖
    def check_dws_dependencies(**context):
        """检查DWS层数据是否就绪"""
        from pyspark.sql import SparkSession
        import logging
        
        spark = None
        try:
            spark = SparkSession.builder \
                .appName("Check DWS Dependencies") \
                .master("local[1]") \
                .config("spark.sql.catalogImplementation", "hive") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
                .config("spark.driver.memory", "1g") \
                .config("spark.executor.memory", "1g") \
                .config("spark.ui.enabled", "false") \
                .enableHiveSupport() \
                .getOrCreate()
            
            # 检查DWS数据库和视图
            spark.sql("USE dws_db")
            
            required_views = [
                'dws_daily_kpi',
                'dws_customer_value_segments', 
                'dws_customer_status_analysis',
                'dws_orders_monthly_trend',
                'dws_monthly_business_metrics'
            ]
            
            views = spark.sql("SHOW VIEWS").collect()
            available_views = [row[1] for row in views]
            
            missing_views = []
            for view in required_views:
                if view not in available_views:
                    missing_views.append(view)
            
            if missing_views:
                logging.error(f"❌ 缺少必要的DWS视图: {missing_views}")
                raise Exception(f"请先运行 dws_orders_analytics 创建必要的分析视图")
            
            logging.info("✅ DWS依赖检查通过")
            return "dws_dependencies_ready"
            
        except Exception as e:
            logging.error(f"DWS依赖检查失败: {e}")
            raise
        finally:
            if spark:
                spark.stop()

    check_dependencies_task = PythonOperator(
        task_id='check_dws_dependencies',
        python_callable=check_dws_dependencies
    )

    # LLM分析任务
    daily_kpi_analysis_task = PythonOperator(
        task_id='analyze_daily_kpi_with_llm',
        python_callable=analyze_daily_kpi_with_llm,
        doc_md="使用DeepSeek分析最近7天的日度KPI数据"
    )

    customer_segments_analysis_task = PythonOperator(
        task_id='analyze_customer_segments_with_llm',
        python_callable=analyze_customer_segments_with_llm,
        doc_md="使用DeepSeek分析客户分段和状态数据"
    )

    monthly_trends_analysis_task = PythonOperator(
        task_id='analyze_monthly_trends_with_llm',
        python_callable=analyze_monthly_trends_with_llm,
        doc_md="使用DeepSeek分析最近6个月的业务趋势"
    )

    # 生成综合报告
    generate_report_task = PythonOperator(
        task_id='generate_comprehensive_report',
        python_callable=generate_comprehensive_report,
        doc_md="整合所有LLM分析结果，生成综合智能报告"
    )

    # 发送邮件报告
    def get_email_content(**context):
        """获取邮件内容"""
        report_html = context['task_instance'].xcom_pull(task_ids='generate_comprehensive_report', key='comprehensive_report')
        return report_html or "报告生成失败，请检查日志。"

    def get_email_config(**context):
        """从Airflow Variables获取邮件配置"""
        from airflow.models import Variable
        import json
        
        # 获取邮件收件人列表
        try:
            recipients_str = Variable.get("EMAIL_RECIPIENTS", default_var='["liujianglc@163.com"]')
            recipients = json.loads(recipients_str)
        except (json.JSONDecodeError, Exception):
            recipients = ['liujianglc@163.com']
        
        # 获取邮件主题
        subject = Variable.get("EMAIL_SUBJECT", default_var="📊 订单数据智能分析报告 - {{ ds }}")
        
        return recipients, subject

    def send_email_with_config(**context):
        """使用配置发送邮件"""
        from airflow.models import Variable
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.utils import formatdate
        
        # 获取邮件配置
        recipients, subject = get_email_config(**context)
        
        # 获取报告内容
        report_content = context['task_instance'].xcom_pull(
            task_ids='generate_comprehensive_report', 
            key='comprehensive_report'
        ) or "报告生成失败，请检查日志。"
        
        # 获取SMTP配置
        smtp_host = Variable.get("SMTP_HOST", default_var="localhost")
        smtp_port = int(Variable.get("SMTP_PORT", default_var="25"))
        smtp_user = Variable.get("SMTP_USER", default_var="")
        smtp_password = Variable.get("SMTP_PASSWORD", default_var="")
        smtp_use_tls = Variable.get("SMTP_USE_TLS", default_var="False").lower() == "true"
        sender = Variable.get("EMAIL_SENDER", default_var="airflow@localhost")
        
        # 创建邮件
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ", ".join(recipients) if isinstance(recipients, list) else recipients
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        
        # 添加HTML内容
        msg.attach(MIMEText(report_content, 'html', 'utf-8'))
        
        try:
            # 发送邮件
            server = smtplib.SMTP(smtp_host, smtp_port)
            if smtp_use_tls:
                server.starttls()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            
            server.sendmail(sender, recipients, msg.as_string())
            server.quit()
            return "✅ 邮件发送成功"
        except Exception as e:
            raise Exception(f"❌ 邮件发送失败: {str(e)}")

    # 使用PythonOperator替代EmailOperator
    send_email_task = PythonOperator(
        task_id='send_analysis_report',
        python_callable=send_email_with_config,
        doc_md="发送包含LLM分析洞察的综合报告邮件"
    )

    # 结束任务
    end_task = DummyOperator(
        task_id='end_llm_analytics',
        doc_md="LLM订单数据智能分析流程完成"
    )

    # 定义任务依赖关系
    start_task >> check_dependencies_task
    
    # 三个LLM分析任务可以并行执行
    check_dependencies_task >> [
        daily_kpi_analysis_task,
        customer_segments_analysis_task, 
        monthly_trends_analysis_task
    ]
    
    # 等待所有分析完成后生成报告
    [
        daily_kpi_analysis_task,
        customer_segments_analysis_task,
        monthly_trends_analysis_task
    ] >> generate_report_task
    
    # 发送邮件并结束
    generate_report_task >> send_email_task >> end_task