# 历史重复数据清理执行计划

## 📋 概述

本文档详细说明如何清理现有DWD层表中的重复数据，确保数据质量并为新的去重机制做准备。

## 🎯 清理目标

- **dwd_db.dwd_orders**: 基于 `OrderID` 去重
- **dwd_db.dwd_orderdetails**: 基于 `OrderDetailID` 去重

## 📅 执行计划

### 阶段1：分析和评估（1-2小时）

#### 1.1 部署清理脚本
```bash
# 1. 将脚本上传到服务器
scp scripts/cleanup_duplicate_data.py user@server:/opt/airflow/scripts/
scp scripts/run_cleanup.sh user@server:/opt/airflow/scripts/

# 2. 设置执行权限
chmod +x /opt/airflow/scripts/run_cleanup.sh
```

#### 1.2 执行数据分析
```bash
# 分析最近30天的重复数据情况
cd /opt/airflow/scripts
./run_cleanup.sh --analyze-only --days 30
```

#### 1.3 评估分析结果
检查日志输出，评估：
- 重复数据的分布情况
- 重复率的严重程度
- 需要清理的分区数量

### 阶段2：备份和准备（30分钟）

#### 2.1 创建备份目录
```bash
# 在HDFS上创建备份目录
hdfs dfs -mkdir -p /backup/duplicate_cleanup
hdfs dfs -chmod 755 /backup/duplicate_cleanup
```

#### 2.2 验证备份空间
```bash
# 检查HDFS可用空间
hdfs dfsadmin -report
```

### 阶段3：执行清理（2-4小时）

#### 3.1 小范围测试
```bash
# 先清理最近3天的数据进行测试
CLEANUP_DAYS=3 ./run_cleanup.sh
```

#### 3.2 验证测试结果
```bash
# 检查清理后的数据
./run_cleanup.sh --analyze-only --days 3
```

#### 3.3 全量清理
```bash
# 清理最近30天的所有重复数据
./run_cleanup.sh --days 30
```

### 阶段4：验证和监控（30分钟）

#### 4.1 数据验证
```sql
-- 验证 dwd_orders 表
SELECT 
    dt,
    COUNT(*) as total_count,
    COUNT(DISTINCT OrderID) as unique_count,
    COUNT(*) - COUNT(DISTINCT OrderID) as duplicate_count
FROM dwd_db.dwd_orders 
WHERE dt >= '2024-01-01'
GROUP BY dt
HAVING COUNT(*) != COUNT(DISTINCT OrderID)
ORDER BY dt;

-- 验证 dwd_orderdetails 表
SELECT 
    dt,
    COUNT(*) as total_count,
    COUNT(DISTINCT OrderDetailID) as unique_count,
    COUNT(*) - COUNT(DISTINCT OrderDetailID) as duplicate_count
FROM dwd_db.dwd_orderdetails 
WHERE dt >= '2024-01-01'
GROUP BY dt
HAVING COUNT(*) != COUNT(DISTINCT OrderDetailID)
ORDER BY dt;
```

#### 4.2 部署监控DAG
```bash
# 部署定期清理DAG
cp dags/cleanup_duplicate_data_dag.py /opt/airflow/dags/
```

## 🛡️ 安全措施

### 备份策略
1. **自动备份**: 清理脚本会自动备份原始数据到 `/backup/duplicate_cleanup/`
2. **保留期限**: 备份数据保留30天
3. **恢复机制**: 提供数据恢复脚本

### 回滚计划
如果清理出现问题，可以通过以下步骤回滚：

```bash
# 1. 停止相关ETL任务
# 2. 从备份恢复数据
hdfs dfs -cp /backup/duplicate_cleanup/dwd_db_dwd_orders/dt=2024-01-01/* \
    /user/hive/warehouse/dwd_db.db/dwd_orders/dt=2024-01-01/

# 3. 刷新表元数据
spark-sql -e "MSCK REPAIR TABLE dwd_db.dwd_orders"
```

## 📊 预期效果

### 清理前后对比
| 指标 | 清理前 | 清理后 | 改善 |
|------|--------|--------|------|
| 重复率 | 5-15% | 0% | 100%消除 |
| 存储空间 | 100% | 85-95% | 节省5-15% |
| 查询性能 | 基准 | 提升10-20% | 显著改善 |

### 数据质量提升
- ✅ 消除所有重复记录
- ✅ 确保主键唯一性
- ✅ 提高数据一致性
- ✅ 减少存储成本

## ⚠️ 注意事项

### 执行时机
- **建议时间**: 业务低峰期（凌晨2-6点）
- **避免时间**: 月末、季末报表生成期
- **通知范围**: 提前通知数据使用方

### 风险控制
1. **分批执行**: 按分区逐步清理，避免一次性处理过多数据
2. **监控资源**: 关注Spark集群资源使用情况
3. **验证数据**: 每个阶段都要验证数据正确性
4. **准备回滚**: 确保备份完整且可恢复

### 性能优化
```bash
# 设置Spark参数优化性能
export SPARK_CONF="
--conf spark.driver.memory=4g
--conf spark.executor.memory=4g
--conf spark.executor.cores=2
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
"
```

## 📞 联系信息

- **负责人**: 数据团队
- **紧急联系**: 值班电话
- **技术支持**: 运维团队

## 📝 执行记录

| 日期 | 操作 | 结果 | 备注 |
|------|------|------|------|
| 待填写 | 数据分析 | 待执行 | |
| 待填写 | 清理执行 | 待执行 | |
| 待填写 | 结果验证 | 待执行 | |

---

**重要提醒**: 执行清理前请务必完成数据分析和备份，确保操作安全性！

