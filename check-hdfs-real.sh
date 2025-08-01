#!/bin/bash

echo "=== 检查HDFS真实数据 ==="

echo "1. 检查HDFS根目录："
curl -s "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS" | python -m json.tool 2>/dev/null || echo "WebHDFS API不可用"

echo ""
echo "2. 检查/user目录："
curl -s "http://localhost:9870/webhdfs/v1/user?op=LISTSTATUS" | python -m json.tool 2>/dev/null || echo "/user目录不存在"

echo ""
echo "3. 检查Hive warehouse目录："
curl -s "http://localhost:9870/webhdfs/v1/user/hive/warehouse?op=LISTSTATUS" | python -m json.tool 2>/dev/null || echo "warehouse目录不存在"

echo ""
echo "4. 检查sales_db目录："
curl -s "http://localhost:9870/webhdfs/v1/user/hive/warehouse/sales_db?op=LISTSTATUS" | python -m json.tool 2>/dev/null || echo "sales_db目录不存在"

echo ""
echo "5. 检查orders_data目录："
curl -s "http://localhost:9870/webhdfs/v1/user/hive/warehouse/sales_db/orders_data?op=LISTSTATUS" | python -m json.tool 2>/dev/null || echo "orders_data目录不存在"

echo ""
echo "6. 通过HDFS命令检查（备用方法）："
docker-compose exec namenode hdfs dfs -ls /user/hive/warehouse/sales_db/ 2>/dev/null || echo "HDFS命令检查失败"

echo ""
echo "=== 检查完成 ==="
echo ""
echo "如果看到JSON格式的输出，说明目录存在"
echo "如果看到'不存在'的消息，说明还没有运行生产级DAG或运行失败"