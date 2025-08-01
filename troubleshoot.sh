#!/bin/bash

echo "大数据环境故障排除工具..."

echo "=== 1. 检查Docker资源使用情况 ==="
docker system df
echo ""
docker stats --no-stream

echo ""
echo "=== 2. 检查各服务日志 ==="
echo "--- PostgreSQL日志 ---"
docker-compose logs --tail=10 postgres

echo ""
echo "--- MySQL日志 ---"
docker-compose logs --tail=10 mysql

echo ""
echo "--- HDFS NameNode日志 ---"
docker-compose logs --tail=10 namenode

echo ""
echo "--- Spark Master日志 ---"
docker-compose logs --tail=10 spark-master

echo ""
echo "--- Airflow Webserver日志 ---"
docker-compose logs --tail=10 airflow-webserver

echo ""
echo "=== 3. 检查端口占用情况 ==="
echo "检查关键端口是否被占用..."
netstat -tlnp | grep -E "(3306|5432|8080|8081|9000|9083|9870)" || echo "netstat命令不可用，跳过端口检查"

echo ""
echo "=== 4. 重启建议 ==="
echo "如果遇到问题，可以尝试以下操作："
echo ""
echo "1. 重启特定服务："
echo "   docker-compose restart [service-name]"
echo ""
echo "2. 完全重启环境："
echo "   docker-compose down && docker-compose up -d"
echo ""
echo "3. 清理并重新开始："
echo "   docker-compose down -v  # 删除数据卷"
echo "   docker system prune -f  # 清理未使用的资源"
echo "   ./start.sh              # 重新启动"
echo ""
echo "4. 查看特定服务的详细日志："
echo "   docker-compose logs -f [service-name]"
echo ""
echo "=== 5. 常见问题解决方案 ==="
echo ""
echo "问题1: Airflow任务卡在队列中"
echo "解决: 检查airflow-worker服务是否运行正常"
echo "      docker-compose restart airflow-worker"
echo ""
echo "问题2: Spark作业失败"
echo "解决: 检查HDFS和Hive Metastore是否正常运行"
echo "      确保网络连接正常"
echo ""
echo "问题3: MySQL连接失败"
echo "解决: 等待MySQL完全启动（可能需要1-2分钟）"
echo "      检查用户权限和数据库是否存在"
echo ""
echo "问题4: HDFS操作失败"
echo "解决: 检查namenode和datanode是否都在运行"
echo "      确保目录权限正确设置"