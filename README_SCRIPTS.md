# 🚀 大数据平台脚本使用指南

## 📋 脚本概览

| 脚本名称 | 用途 | 推荐场景 |
|---------|------|----------|
| `quick-start.sh` | 一键快速启动 | 新手用户，快速体验 |
| `deploy.sh` | 完整部署脚本 | 生产环境，完整功能 |
| `build-airflow-image.sh` | 构建自定义镜像 | 需要自定义依赖 |
| `test-build.sh` | 测试构建环境 | 构建失败时诊断 |
| `check-status.sh` | 状态检查 | 运行时监控 |
| `cleanup.sh` | 清理脚本 | 维护和重置 |
| `quick-deploy.sh` | 原始部署脚本 | 兼容性保持 |

## 🎯 使用场景

### 场景1: 新手快速体验
```bash
# 最简单的方式，自动选择最佳方案
./quick-start.sh
```
**特点**: 
- 自动处理构建失败
- 提供多种启动选项
- 包含状态验证

### 场景2: 开发环境部署
```bash
# 完整的开发环境
./deploy.sh --mode dev
```
**特点**:
- 包含热重载
- 暴露调试端口
- 详细的错误处理

### 场景3: 生产环境部署
```bash
# 构建优化镜像
./build-airflow-image.sh

# 生产环境部署
./deploy.sh --mode prod
```
**特点**:
- 资源限制
- 自动重启策略
- 安全配置

### 场景4: 仅基础服务
```bash
# 只启动Airflow和数据库
./quick-start.sh  # 选择选项3
```
**特点**:
- 资源占用少
- 启动速度快
- 适合测试

### 场景5: 故障排除
```bash
# 诊断构建问题
./test-build.sh

# 检查运行状态
./check-status.sh

# 清理重置
./cleanup.sh
```

## 🔧 Docker Compose 配置文件

| 文件名称 | 用途 |
|---------|------|
| `docker-compose.yml` | 主配置文件 |
| `docker-compose.override.yml` | 开发环境覆盖 |
| `docker-compose.prod.yml` | 生产环境配置 |
| `docker-compose.fallback.yml` | 原始镜像后备方案 |

### 组合使用示例

```bash
# 开发环境 (自动加载override)
docker-compose up -d

# 生产环境
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# 使用原始镜像
docker-compose -f docker-compose.yml -f docker-compose.fallback.yml up -d
```

## 🐳 Dockerfile 选项

| 文件名称 | 特点 | 适用场景 |
|---------|------|----------|
| `Dockerfile.airflow` | 完整版本，包含所有依赖 | 功能完整的生产环境 |
| `Dockerfile.airflow.simple` | 简化版本，核心依赖 | 快速构建，基础功能 |
| 动态生成的minimal版本 | 最小版本，仅MySQL支持 | 构建失败时的后备 |

## 🚨 故障处理流程

### 1. 构建失败
```bash
# 步骤1: 诊断环境
./test-build.sh

# 步骤2: 尝试不同构建策略
./build-airflow-image.sh

# 步骤3: 使用后备方案
./quick-start.sh  # 选择选项1
```

### 2. 启动失败
```bash
# 步骤1: 检查状态
./check-status.sh

# 步骤2: 查看日志
docker-compose logs airflow-webserver

# 步骤3: 重启服务
docker-compose restart airflow-webserver
```

### 3. 性能问题
```bash
# 步骤1: 检查资源使用
docker stats

# 步骤2: 启动基础服务
./quick-start.sh  # 选择选项3

# 步骤3: 调整配置
# 编辑 .env 文件减少资源分配
```

## 📊 监控和维护

### 日常检查
```bash
# 快速状态检查
./check-status.sh

# 查看容器状态
docker-compose ps

# 查看资源使用
docker stats --no-stream
```

### 日志管理
```bash
# 查看特定服务日志
docker-compose logs -f airflow-webserver

# 清理日志
./cleanup.sh  # 选择选项5
```

### 定期维护
```bash
# 每周清理未使用的镜像
docker system prune -f

# 每月完全重建
./cleanup.sh  # 选择选项4
./deploy.sh --mode prod
```

## 🎛️ 环境变量配置

关键配置项 (在 `.env` 文件中):

```bash
# 端口配置 (避免冲突)
AIRFLOW_WEBSERVER_PORT=8080
SPARK_UI_PORT=8081
HDFS_UI_PORT=9870

# 资源配置 (根据系统调整)
SPARK_WORKER_MEMORY=2g
SPARK_WORKER_CORES=2

# 安全配置 (生产环境必须修改)
POSTGRES_PASSWORD=your-secure-password
MYSQL_ROOT_PASSWORD=your-secure-password
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
```

## 💡 最佳实践

1. **首次使用**: 使用 `./quick-start.sh` 快速体验
2. **开发环境**: 使用 `./deploy.sh --mode dev` 获得完整功能
3. **生产环境**: 先构建镜像，再使用生产配置部署
4. **故障排除**: 按照故障处理流程逐步诊断
5. **定期维护**: 使用清理脚本保持系统整洁

## 🆘 获取帮助

如果遇到问题：

1. 查看 `DEPLOYMENT_GUIDE.md` 获取详细说明
2. 运行 `./check-status.sh` 检查系统状态
3. 使用 `./test-build.sh` 诊断构建问题
4. 查看容器日志: `docker-compose logs [service-name]`

---

**提示**: 所有脚本都包含 `--help` 选项，可以查看详细用法说明。