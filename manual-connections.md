# Airflow连接手动设置指南

如果自动连接设置失败，请按照以下步骤手动添加连接：

## 1. 访问Airflow Web UI
- 打开浏览器访问：http://localhost:8080
- 使用用户名：admin，密码：admin 登录

## 2. 进入连接管理页面
- 点击顶部菜单 "Admin" -> "Connections"

## 3. 添加MySQL连接
点击 "+" 按钮添加新连接：
- **Connection Id**: `mysql_default`
- **Connection Type**: `MySQL`
- **Host**: `mysql`
- **Port**: `3306`
- **Login**: `etl_user`
- **Password**: `etl_pass`
- **Schema**: `source_db`

## 4. 添加Spark连接（可选）
点击 "+" 按钮添加新连接：
- **Connection Id**: `spark_default`
- **Connection Type**: `Spark`
- **Host**: `spark-master`
- **Port**: `7077`

## 5. 验证连接
- 在连接列表中可以看到新添加的连接
- 可以点击连接旁边的"Test"按钮测试连接

## 常见问题

### Q: 找不到MySQL连接类型？
A: 确保Airflow已安装mysql provider：
```bash
docker-compose exec airflow-webserver pip install apache-airflow-providers-mysql
```

### Q: 连接测试失败？
A: 检查以下几点：
1. 服务是否正在运行：`docker-compose ps`
2. 网络连接是否正常
3. 用户名密码是否正确

### Q: Spark连接类型不存在？
A: 安装Spark provider：
```bash
docker-compose exec airflow-webserver pip install apache-airflow-providers-apache-spark
```

## 快速验证脚本

运行以下命令验证连接是否正常：

```bash
# 测试MySQL连接
docker-compose exec airflow-webserver airflow connections test mysql_default

# 查看所有连接
docker-compose exec airflow-webserver airflow connections list
```