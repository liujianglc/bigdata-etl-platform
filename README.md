# 大数据ETL平台

基于Docker的完整大数据处理平台，包含Airflow、Spark、HDFS、Hive、MySQL等组件。

## 🏗️ 架构组件

- **Apache Airflow** - 工作流调度和监控
- **Apache Spark** - 分布式数据处理
- **HDFS** - 分布式文件系统
- **Apache Hive** - 数据仓库
- **MySQL** - 源数据库
- **PostgreSQL** - Airflow元数据存储
- **Redis** - Airflow任务队列

## 🚀 快速开始

### 1. 环境要求

- Docker & Docker Compose
- 至少8GB内存
- 20GB可用磁盘空间

### 2. 启动服务

```bash
# 启动所有服务
./start.sh

# 或者分步启动（推荐）
./start-basic.sh          # 启动基础服务
./start-hdfs-spark.sh     # 启动大数据组件
```

### 3. 访问界面

- **Airflow Web UI**: http://localhost:8080 (admin/admin)
- **Spark Master UI**: http://localhost:8081
- **HDFS NameNode UI**: http://localhost:9870
- **MySQL**: localhost:3306 (root/rootpass)

## 📊 可用的DAG

### 1. simple_mysql_etl
- **适用场景**: 入门学习
- **功能**: 基础MySQL数据处理
- **推荐**: 初学者首选

### 2. real_bigdata_pipeline
- **适用场景**: 模拟大数据流程
- **功能**: 完整ETL流程演示
- **特点**: 模拟HDFS和Hive操作

### 3. production_bigdata_pipeline
- **适用场景**: 生产环境
- **功能**: 真实大数据ETL
- **特点**: 真实写入HDFS和Hive

### 4. complete_bigdata_etl
- **适用场景**: 完整演示
- **功能**: 端到端大数据处理
- **特点**: 包含所有组件

## 🔧 配置文件

### 核心配置
- `docker-compose.yaml` - 主要服务配置
- `hive-site.xml` - Hive配置
- `spark-defaults.conf.txt` - Spark配置
- `init-mysql-data.sql` - MySQL初始数据

### 启动脚本
- `start.sh` - 完整环境启动
- `start-basic.sh` - 基础服务启动
- `start-hdfs-spark.sh` - 大数据组件启动

## 📁 目录结构

```
├── dags/                    # Airflow DAG文件
├── spark_jobs/              # Spark作业脚本
├── hadoop-config/           # Hadoop配置文件
├── docker-compose.yaml      # Docker服务配置
├── hive-site.xml           # Hive配置
├── init-mysql-data.sql     # MySQL初始数据
└── start.sh                # 启动脚本
```

## 🛠️ 故障排除

### 常见问题

1. **服务启动失败**
   ```bash
   ./troubleshoot.sh
   ```

2. **DAG不显示**
   ```bash
   ./check-dag-syntax.sh
   ```

3. **HDFS数据检查**
   ```bash
   ./check-hdfs-real.sh
   ```

### 重启服务
```bash
# 完全重启
docker-compose down
./start.sh

# 重启特定服务
docker-compose restart airflow-scheduler
```

## 📈 数据流程

```
MySQL → CSV → HDFS → Hive → 分析报告
```

1. **数据提取**: 从MySQL提取业务数据
2. **数据存储**: 写入HDFS分布式文件系统
3. **数据建模**: 在Hive中创建数据仓库表
4. **数据分析**: 执行业务分析查询
5. **报告生成**: 生成详细的执行报告

## 🔍 监控和日志

- **Airflow日志**: `./logs/` 目录
- **服务日志**: `docker-compose logs [service-name]`
- **HDFS状态**: http://localhost:9870
- **Spark作业**: http://localhost:8081

## 📝 开发指南

### 添加新的DAG
1. 在 `dags/` 目录创建Python文件
2. 使用现有DAG作为模板
3. 测试语法: `python -m py_compile dags/your_dag.py`

### 添加新的Spark作业
1. 在 `spark_jobs/` 目录创建Python文件
2. 在DAG中引用作业路径
3. 确保作业可以访问必要的依赖

## 🤝 贡献

1. Fork项目
2. 创建功能分支
3. 提交更改
4. 创建Pull Request

## 📄 许可证

MIT License