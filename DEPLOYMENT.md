# 部署指南

## 🚀 生产环境部署

### 1. 环境准备

#### 系统要求
- **操作系统**: Linux (Ubuntu 20.04+ 推荐)
- **内存**: 最少8GB，推荐16GB+
- **磁盘**: 最少50GB可用空间
- **CPU**: 最少4核，推荐8核+

#### 软件依赖
```bash
# 安装Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# 安装Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# 验证安装
docker --version
docker-compose --version
```

### 2. 项目部署

#### 克隆项目
```bash
git clone <repository-url>
cd large-data
```

#### 配置环境变量
```bash
# 复制环境变量模板
cp .env.example .env

# 编辑配置
vim .env
```

#### 启动服务
```bash
# 给脚本执行权限
chmod +x *.sh

# 启动所有服务
./start.sh

# 或分步启动（推荐）
./start-basic.sh
./start-hdfs-spark.sh
```

### 3. 验证部署

#### 检查服务状态
```bash
# 查看所有容器状态
docker-compose ps

# 检查服务健康状态
./verify-data.sh
```

#### 访问服务
- Airflow: http://your-server:8080
- Spark: http://your-server:8081
- HDFS: http://your-server:9870

### 4. 安全配置

#### 修改默认密码
```bash
# Airflow管理员密码
docker-compose exec airflow-webserver airflow users create \
    --username your-admin \
    --firstname Your \
    --lastname Name \
    --role Admin \
    --email your-email@domain.com \
    --password your-secure-password
```

#### 网络安全
```bash
# 配置防火墙（Ubuntu）
sudo ufw allow 22/tcp
sudo ufw allow 8080/tcp  # Airflow
sudo ufw allow 8081/tcp  # Spark
sudo ufw allow 9870/tcp  # HDFS
sudo ufw enable
```

### 5. 监控和维护

#### 日志管理
```bash
# 查看服务日志
docker-compose logs -f airflow-scheduler

# 清理旧日志
docker system prune -f
```

#### 备份数据
```bash
# 备份MySQL数据
docker-compose exec mysql mysqldump -u root -prootpass --all-databases > backup.sql

# 备份Airflow配置
docker-compose exec postgres pg_dump -U airflow airflow > airflow_backup.sql
```

#### 更新服务
```bash
# 拉取最新镜像
docker-compose pull

# 重启服务
docker-compose down
docker-compose up -d
```

## 🔧 配置优化

### 资源配置

#### Spark配置优化
编辑 `spark-defaults.conf.txt`:
```
spark.executor.memory            2g
spark.driver.memory              1g
spark.executor.cores             2
spark.sql.adaptive.enabled       true
```

#### Airflow配置优化
在 `docker-compose.yaml` 中调整:
```yaml
environment:
  AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
  AIRFLOW__CORE__PARALLELISM: 32
  AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 16
```

### 存储配置

#### HDFS存储优化
```bash
# 增加HDFS副本数（生产环境）
docker-compose exec namenode hdfs dfsadmin -setDefaultReplication 3
```

#### MySQL存储优化
在 `docker-compose.yaml` 中添加:
```yaml
mysql:
  command: --innodb-buffer-pool-size=1G --max-connections=200
```

## 🚨 故障排除

### 常见问题

#### 1. 内存不足
```bash
# 检查内存使用
free -h
docker stats

# 调整服务内存限制
# 在docker-compose.yaml中添加:
deploy:
  resources:
    limits:
      memory: 2G
```

#### 2. 磁盘空间不足
```bash
# 清理Docker资源
docker system prune -a -f

# 清理日志文件
sudo find /var/lib/docker -name "*.log" -delete
```

#### 3. 网络连接问题
```bash
# 重建网络
docker-compose down
docker network prune -f
docker-compose up -d
```

### 监控脚本

创建监控脚本 `monitor.sh`:
```bash
#!/bin/bash
# 检查关键服务状态
services=("airflow-webserver" "spark-master" "namenode" "mysql")
for service in "${services[@]}"; do
    if docker-compose ps $service | grep -q "Up"; then
        echo "✅ $service: Running"
    else
        echo "❌ $service: Down"
        # 发送告警通知
        # curl -X POST "your-webhook-url" -d "Service $service is down"
    fi
done
```

## 📈 性能调优

### 1. JVM调优
在相关服务中添加JVM参数:
```yaml
environment:
  JAVA_OPTS: "-Xmx2g -Xms1g -XX:+UseG1GC"
```

### 2. 数据库调优
MySQL配置优化:
```sql
SET GLOBAL innodb_buffer_pool_size = 1073741824;  -- 1GB
SET GLOBAL max_connections = 200;
SET GLOBAL query_cache_size = 268435456;  -- 256MB
```

### 3. 网络调优
```bash
# 增加网络缓冲区
echo 'net.core.rmem_max = 16777216' >> /etc/sysctl.conf
echo 'net.core.wmem_max = 16777216' >> /etc/sysctl.conf
sysctl -p
```

## 🔄 CI/CD集成

### GitHub Actions示例
创建 `.github/workflows/deploy.yml`:
```yaml
name: Deploy Big Data Platform

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    
    - name: Deploy to server
      run: |
        ssh user@server 'cd /path/to/project && git pull && ./start.sh'
```

## 📞 支持

如遇到部署问题，请：
1. 检查日志文件
2. 运行故障排除脚本
3. 查看GitHub Issues
4. 联系技术支持