"""
数据类型配置文件
统一管理Spark SQL中使用的数据类型，避免过度精度和类型不一致问题
"""

from pyspark.sql.types import DecimalType, DoubleType, IntegerType

class DataTypeConfig:
    """数据类型配置类"""
    
    # 金额相关字段 - 使用合理的精度
    AMOUNT_TYPE = DecimalType(15, 2)  # 最大999万亿，2位小数
    SMALL_AMOUNT_TYPE = DecimalType(10, 2)  # 最大9999万，2位小数
    
    # 平均值和比率 - 使用适中精度
    AVERAGE_TYPE = DecimalType(10, 2)  # 平均值
    RATE_TYPE = DoubleType()  # 百分比和比率
    FREQUENCY_TYPE = DecimalType(8, 4)  # 频率，如订单频率
    
    # 天数和时间相关
    DAYS_TYPE = DecimalType(5, 2)  # 天数，最大999.99天
    
    # 计数相关
    COUNT_TYPE = IntegerType()  # 计数字段
    
    @classmethod
    def get_amount_cast(cls, column_name):
        """获取金额字段的类型转换"""
        return cls.AMOUNT_TYPE
    
    @classmethod
    def get_average_cast(cls, column_name):
        """获取平均值字段的类型转换"""
        return cls.AVERAGE_TYPE
    
    @classmethod
    def get_rate_cast(cls, column_name):
        """获取比率字段的类型转换"""
        return cls.RATE_TYPE
    
    @classmethod
    def get_days_cast(cls, column_name):
        """获取天数字段的类型转换"""
        return cls.DAYS_TYPE
    
    @classmethod
    def get_frequency_cast(cls, column_name):
        """获取频率字段的类型转换"""
        return cls.FREQUENCY_TYPE

# 常用类型别名
AMOUNT = DataTypeConfig.AMOUNT_TYPE
SMALL_AMOUNT = DataTypeConfig.SMALL_AMOUNT_TYPE
AVERAGE = DataTypeConfig.AVERAGE_TYPE
RATE = DataTypeConfig.RATE_TYPE
DAYS = DataTypeConfig.DAYS_TYPE
FREQUENCY = DataTypeConfig.FREQUENCY_TYPE
COUNT = DataTypeConfig.COUNT_TYPE