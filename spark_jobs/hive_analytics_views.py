import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import json
from datetime import datetime, timedelta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hive-db", required=True, help="Hive database name")
    parser.add_argument("--source-table", required=True, help="Source Hive table")
    parser.add_argument("--report-output-path", required=True, help="Output path for analysis report")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    spark = SparkSession.builder \
        .appName("HiveAnalyticsViews") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.adaptive.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        logger.info(f"Creating analytics views for database: {args.hive_db}")
        
        # 检查源表是否存在
        tables = spark.sql(f"SHOW TABLES IN {args.hive_db}").collect()
        source_table_exists = any(row['tableName'] == args.source_table for row in tables)
        
        if not source_table_exists:
            raise Exception(f"Source table {args.hive_db}.{args.source_table} does not exist")

        # 获取表结构信息
        table_schema = spark.sql(f"DESCRIBE {args.hive_db}.{args.source_table}").collect()
        logger.info("Table schema:")
        for row in table_schema:
            logger.info(f"  {row['col_name']} - {row['data_type']}")

        # 1. 创建每日汇总视图
        logger.info("Creating daily_summary view...")
        daily_summary_sql = f"""
        CREATE OR REPLACE VIEW {args.hive_db}.daily_summary AS
        SELECT 
            DATE(created_at) as order_date,
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_revenue,
            SUM(amount) as total_amount,
            AVG(amount) as avg_order_value,
            MIN(amount) as min_order_value,
            MAX(amount) as max_order_value,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_orders,
            COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_orders,
            ROUND(COUNT(CASE WHEN status = 'completed' THEN 1 END) * 100.0 / COUNT(*), 2) as completion_rate
        FROM {args.hive_db}.{args.source_table}
        WHERE created_at IS NOT NULL
        GROUP BY DATE(created_at)
        """
        spark.sql(daily_summary_sql)

        # 2. 创建产品销售排行视图
        logger.info("Creating product_ranking view...")
        product_ranking_sql = f"""
        CREATE OR REPLACE VIEW {args.hive_db}.product_ranking AS
        SELECT 
            product_id,
            COUNT(*) as order_count,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_count,
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as total_revenue,
            AVG(CASE WHEN status = 'completed' THEN amount END) as avg_revenue_per_order,
            ROUND(COUNT(CASE WHEN status = 'completed' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate,
            MIN(created_at) as first_order_date,
            MAX(created_at) as last_order_date
        FROM {args.hive_db}.{args.source_table}
        WHERE product_id IS NOT NULL
        GROUP BY product_id
        """
        spark.sql(product_ranking_sql)

        # 3. 创建客户分析视图
        logger.info("Creating customer_analysis view...")
        customer_analysis_sql = f"""
        CREATE OR REPLACE VIEW {args.hive_db}.customer_analysis AS
        SELECT 
            customer_id,
            COUNT(*) as total_orders,
            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_orders,
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as total_spent,
            AVG(CASE WHEN status = 'completed' THEN amount END) as avg_order_value,
            MIN(created_at) as first_order_date,
            MAX(created_at) as last_order_date,
            DATEDIFF(MAX(created_at), MIN(created_at)) as customer_lifetime_days,
            ROUND(COUNT(CASE WHEN status = 'completed' THEN 1 END) * 100.0 / COUNT(*), 2) as order_success_rate
        FROM {args.hive_db}.{args.source_table}
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
        """
        spark.sql(customer_analysis_sql)

        # 4. 创建月度趋势视图
        logger.info("Creating monthly_trends view...")
        monthly_trends_sql = f"""
        CREATE OR REPLACE VIEW {args.hive_db}.monthly_trends AS
        SELECT 
            YEAR(created_at) as year,
            MONTH(created_at) as month,
            CONCAT(YEAR(created_at), '-', LPAD(MONTH(created_at), 2, '0')) as year_month,
            COUNT(*) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products,
            SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as revenue,
            AVG(CASE WHEN status = 'completed' THEN amount END) as avg_order_value,
            ROUND(COUNT(CASE WHEN status = 'completed' THEN 1 END) * 100.0 / COUNT(*), 2) as completion_rate
        FROM {args.hive_db}.{args.source_table}
        WHERE created_at IS NOT NULL
        GROUP BY YEAR(created_at), MONTH(created_at)
        ORDER BY year, month
        """
        spark.sql(monthly_trends_sql)

        # 5. 执行分析查询并生成报告
        logger.info("Generating analytics report...")
        
        # 获取数据概况
        total_records = spark.sql(f"SELECT COUNT(*) as count FROM {args.hive_db}.{args.source_table}").collect()[0]['count']
        
        # 最近7天的趋势
        recent_trends = spark.sql(f"""
            SELECT * FROM {args.hive_db}.daily_summary 
            WHERE order_date >= DATE_SUB(CURRENT_DATE(), 7)
            ORDER BY order_date DESC
        """).collect()

        # 顶级产品
        top_products = spark.sql(f"""
            SELECT * FROM {args.hive_db}.product_ranking 
            ORDER BY total_revenue DESC 
            LIMIT 10
        """).collect()

        # 顶级客户
        top_customers = spark.sql(f"""
            SELECT * FROM {args.hive_db}.customer_analysis 
            ORDER BY total_spent DESC 
            LIMIT 10
        """).collect()

        # 月度趋势
        monthly_data = spark.sql(f"""
            SELECT * FROM {args.hive_db}.monthly_trends 
            ORDER BY year DESC, month DESC 
            LIMIT 12
        """).collect()

        # 生成报告
        report = {
            "report_generated_at": datetime.now().isoformat(),
            "database": args.hive_db,
            "source_table": args.source_table,
            "total_records": total_records,
            "views_created": [
                "daily_summary",
                "product_ranking", 
                "customer_analysis",
                "monthly_trends"
            ],
            "analysis_results": {
                "recent_7_days_trends": [dict(row.asDict()) for row in recent_trends],
                "top_10_products": [dict(row.asDict()) for row in top_products],
                "top_10_customers": [dict(row.asDict()) for row in top_customers],
                "monthly_trends": [dict(row.asDict()) for row in monthly_data]
            }
        }

        # 保存报告到 HDFS
        report_json = json.dumps(report, indent=2, default=str)
        
        # 使用 Spark 保存报告
        report_df = spark.createDataFrame([{"report_content": report_json}])
        report_df.write.mode("overwrite").text(args.report_output_path)

        logger.info(f"Analytics report saved to: {args.report_output_path}")
        logger.info("Views created successfully:")
        logger.info(f"  - {args.hive_db}.daily_summary")
        logger.info(f"  - {args.hive_db}.product_ranking") 
        logger.info(f"  - {args.hive_db}.customer_analysis")
        logger.info(f"  - {args.hive_db}.monthly_trends")

        # 打印简要报告到日志
        logger.info("=== ANALYTICS SUMMARY ===")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Top product by revenue: {top_products[0]['product_id'] if top_products else 'N/A'}")
        logger.info(f"Top customer by spending: {top_customers[0]['customer_id'] if top_customers else 'N/A'}")
        
        if recent_trends:
            latest_day = recent_trends[0]
            logger.info(f"Latest day performance:")
            logger.info(f"  - Orders: {latest_day['total_orders']}")
            logger.info(f"  - Revenue: {latest_day['completed_revenue']}")
            logger.info(f"  - Completion rate: {latest_day['completion_rate']}%")

    except Exception as e:
        logger.error(f"Analytics job failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()