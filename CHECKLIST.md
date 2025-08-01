# 部署检查清单

## 📋 部署前检查

### 环境准备
- [ ] Docker已安装 (版本 >= 20.10)
- [ ] Docker Compose已安装 (版本 >= 2.0)
- [ ] 系统内存 >= 8GB
- [ ] 可用磁盘空间 >= 50GB
- [ ] 网络端口可用 (8080, 8081, 9870, 3306)

### 文件准备
- [ ] 所有必要文件已下载
- [ ] 脚本文件有执行权限
- [ ] 配置文件已根据环境调整
- [ ] .env文件已创建并配置

## 🚀 部署步骤

### 1. 基础服务启动
- [ ] 执行 `./start-basic.sh`
- [ ] 验证PostgreSQL启动: `docker-compose ps postgres`
- [ ] 验证MySQL启动: `docker-compose ps mysql`
- [ ] 验证Redis启动: `docker-compose ps redis`
- [ ] 验证Airflow Web UI可访问: http://localhost:8080

### 2. 大数据组件启动
- [ ] 执行 `./start-hdfs-spark.sh`
- [ ] 验证HDFS NameNode启动: `docker-compose ps namenode`
- [ ] 验证HDFS DataNode启动: `docker-compose ps datanode`
- [ ] 验证Hive Metastore启动: `docker-compose ps hive-metastore`
- [ ] 验证Spark Master启动: `docker-compose ps spark-master`
- [ ] 验证Spark Worker启动: `docker-compose ps spark-worker`

### 3. 服务验证
- [ ] Airflow UI可访问且可登录 (admin/admin)
- [ ] Spark Master UI可访问: http://localhost:8081
- [ ] HDFS NameNode UI可访问: http://localhost:9870
- [ ] MySQL连接正常: `mysql -h localhost -u etl_user -petl_pass`

### 4. DAG验证
- [ ] 所有DAG在Airflow UI中可见
- [ ] simple_mysql_etl DAG可以成功运行
- [ ] real_bigdata_pipeline DAG可以成功运行
- [ ] production_bigdata_pipeline DAG可以成功运行

### 5. 数据验证
- [ ] MySQL中有示例数据
- [ ] 可以执行简单的ETL流程
- [ ] HDFS中可以创建目录和文件
- [ ] Hive表可以创建和查询

## 🔧 故障排除

### 常见问题检查
- [ ] 检查Docker容器日志: `docker-compose logs [service-name]`
- [ ] 检查端口占用: `netstat -tlnp | grep [port]`
- [ ] 检查磁盘空间: `df -h`
- [ ] 检查内存使用: `free -h`
- [ ] 检查Docker资源: `docker system df`

### 服务重启
- [ ] 单个服务重启: `docker-compose restart [service-name]`
- [ ] 完全重启: `docker-compose down && docker-compose up -d`
- [ ] 清理重启: `docker-compose down -v && ./start.sh`

## 📊 性能检查

### 资源使用
- [ ] CPU使用率 < 80%
- [ ] 内存使用率 < 80%
- [ ] 磁盘使用率 < 80%
- [ ] 网络连接正常

### 服务响应
- [ ] Airflow UI响应时间 < 3秒
- [ ] Spark作业提交正常
- [ ] HDFS文件操作正常
- [ ] MySQL查询响应正常

## 🔒 安全检查

### 访问控制
- [ ] 修改默认密码
- [ ] 配置防火墙规则
- [ ] 限制外部访问
- [ ] 启用HTTPS (生产环境)

### 数据安全
- [ ] 数据库连接加密
- [ ] 敏感信息使用环境变量
- [ ] 定期备份配置
- [ ] 日志轮转配置

## 📈 监控设置

### 日志监控
- [ ] 配置日志收集
- [ ] 设置日志轮转
- [ ] 配置错误告警
- [ ] 监控磁盘使用

### 性能监控
- [ ] 配置资源监控
- [ ] 设置性能告警
- [ ] 监控作业执行时间
- [ ] 跟踪数据处理量

## ✅ 部署完成确认

### 功能验证
- [ ] 所有核心服务正常运行
- [ ] DAG可以正常执行
- [ ] 数据可以正常处理
- [ ] 报告可以正常生成

### 文档更新
- [ ] 更新部署文档
- [ ] 记录配置变更
- [ ] 更新运维手册
- [ ] 培训相关人员

## 📞 联系信息

部署完成后，请确保以下信息已记录：
- [ ] 服务器IP地址和访问方式
- [ ] 管理员账号和密码
- [ ] 重要配置文件位置
- [ ] 故障联系人信息

---

**注意**: 请在每个步骤完成后打勾确认，确保部署过程的完整性和可追溯性。