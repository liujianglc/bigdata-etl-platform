# 大数据平台部署指南

## 🚀 快速开始

### 方法1: 使用新的部署脚本 (推荐)
```bash
# 开发环境部署
./deploy.sh --mode dev

# 生产环境部署
./deploy.sh --mode prod

# 跳过镜像构建 (如果已经构建过)
./deploy.sh --skip-build
```

### 方法2: 使用原有脚本
```bash
./quick-deploy.sh
```

## 📋 主要改进

### 1. 解决了 PIP 包安装警告
- **问题**: 容器启动时安装Python包效率低且不安全
- **解决方案**: 创建自定义Airflow镜像，预装所有依赖
- **文件**: `Dockerfile.airflow`, `requirements.txt`

### 2. 优化的部署流程
- **新脚本**: `deploy.sh` - 支持开发/生产模式
- **构建脚本**: `build-airflow-image.sh` - 单独构建镜像
- **清理脚本**: `cleanup.sh` - 多级清理选项

### 3. 环境配置优化
- **开发环境**: `docker-compose.override.yml` - 自动加载
- **生产环境**: `docker-compose.prod.yml` - 资源限制和重启策略
- **依赖管理**: `requirements.txt` - 版本锁定

## 🛠️ 文件说明

### 核心配置文件
- `docker-compose.yaml` - 主配置文件 (已优化)
- `Dockerfile.airflow` - 自定义Airflow镜像
- `requirements.txt` - Python依赖列表
- `.dockerignore` - 构建优化

### 环境配置
- `docker-compose.override.yml` - 开发环境覆盖
- `docker-compose.prod.yml` - 生产环境配置
- `.env.example` - 环境变量模板

### 部署脚本
- `deploy.sh` - 主部署脚本 (新)
- `build-airflow-image.sh` - 镜像构建
- `cleanup.sh` - 清理脚本
- `quick-deploy.sh` - 原有脚本 (已更新)

## 🔧 使用说明

### 首次部署
```bash
# 1. 克隆项目
git clone <repository>
cd <project-directory>

# 2. 复制并编辑环境配置
cp .env.example .env
# 编辑 .env 文件设置密码等

# 3. 部署 (自动构建镜像)
./deploy.sh --mode dev
```

### 日常操作
```bash
# 查看服务状态
docker compose ps

# 查看日志
docker compose logs airflow-webserver

# 重启特定服务
docker compose restart airflow-scheduler

# 停止所有服务
docker compose down

# 完全清理
./cleanup.sh
```

### 生产部署
```bash
# 构建生产镜像
./build-airflow-image.sh

# 生产环境部署
./deploy.sh --mode prod
```

## 🌐 访问地址

部署成功后可以访问：

- **Airflow Web UI**: http://localhost:8080 (admin/admin)
- **Spark Master UI**: http://localhost:8081
- **HDFS NameNode UI**: http://localhost:9870
- **MySQL**: localhost:3306
- **PostgreSQL**: localhost:5432

## 🔍 故障排除

### 常见问题

1. **端口冲突**
   ```bash
   # 修改 .env 文件中的端口配置
   AIRFLOW_WEBSERVER_PORT=8090
   SPARK_UI_PORT=8091
   ```

2. **内存不足**
   ```bash
   # 检查系统资源
   free -h
   df -h
   
   # 减少Spark worker内存
   # 编辑 .env 文件
   SPARK_WORKER_MEMORY=1g
   ```

3. **权限问题**
   ```bash
   # 修复目录权限
   sudo chown -R $USER:$USER logs/
   chmod -R 755 logs/
   ```

4. **镜像构建失败**
   ```bash
   # 清理Docker缓存
   docker system prune -f
   
   # 重新构建
   ./build-airflow-image.sh
   ```

### 清理选项
```bash
./cleanup.sh
# 选择适当的清理级别：
# 1. 软清理 - 仅停止服务
# 2. 中等清理 - 删除容器保留数据
# 3. 完全清理 - 删除所有资源
# 4. 深度清理 - 包括自定义镜像
# 5. 仅清理日志
```

## 📊 性能优化

### 开发环境
- 启用热重载
- 暴露数据库端口便于调试
- 较少的资源限制

### 生产环境
- 资源限制和预留
- 自动重启策略
- 禁用调试功能
- 优化的数据库配置

## 🔐 安全建议

1. **修改默认密码**
   ```bash
   # 编辑 .env 文件
   POSTGRES_PASSWORD=your-secure-password
   MYSQL_ROOT_PASSWORD=your-secure-password
   ```

2. **生成Fernet密钥**
   ```python
   from cryptography.fernet import Fernet
   print(Fernet.generate_key().decode())
   ```

3. **网络安全**
   - 生产环境不要暴露数据库端口
   - 使用防火墙限制访问
   - 考虑使用HTTPS

## 📈 监控和维护

### 日志管理
```bash
# 查看特定服务日志
docker compose logs -f airflow-webserver

# 清理日志
./cleanup.sh  # 选择选项5
```

### 健康检查
```bash
# 检查服务健康状态
docker compose ps

# 手动健康检查
curl http://localhost:8080/health
```

### 备份数据
```bash
# 备份数据库
docker compose exec postgres pg_dump -U airflow airflow > backup.sql
docker compose exec mysql mysqldump -u root -p source_db > mysql_backup.sql
```

## 🆘 获取帮助

如果遇到问题：

1. 查看日志: `docker compose logs [service-name]`
2. 检查系统资源: `free -h`, `df -h`
3. 验证网络连接: `docker network ls`
4. 重新部署: `./cleanup.sh` 然后 `./deploy.sh`

---

**注意**: 这个部署方案已经解决了原有的PIP包安装警告，提供了更稳定和高效的部署体验。