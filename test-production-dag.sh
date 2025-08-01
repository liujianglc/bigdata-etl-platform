#!/bin/bash

echo "=== 测试生产级DAG ==="

echo "1. 检查DAG文件是否存在："
ls -la dags/production_bigdata_pipeline.py

echo ""
echo "2. 检查DAG语法："
docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/production_bigdata_pipeline.py

if [ $? -eq 0 ]; then
    echo "✅ 生产级DAG语法检查通过"
else
    echo "❌ 生产级DAG语法检查失败"
fi

echo ""
echo "3. 检查Airflow是否识别新DAG："
docker-compose exec airflow-webserver airflow dags list | grep production_bigdata_pipeline

echo ""
echo "4. 检查DAG任务列表："
docker-compose exec airflow-webserver airflow tasks list production_bigdata_pipeline

echo ""
echo "5. 检查WebHDFS API是否可用："
curl -s "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS" | head -100

echo ""
echo "=== 测试完成 ==="
echo ""
echo "现在你可以在Airflow UI中找到 'production_bigdata_pipeline' DAG"
echo "这个DAG会真实地："
echo "1. 从MySQL提取数据到CSV"
echo "2. 通过WebHDFS API上传到HDFS"
echo "3. 创建Hive外部表定义"
echo "4. 执行数据分析"
echo "5. 验证HDFS中的数据"