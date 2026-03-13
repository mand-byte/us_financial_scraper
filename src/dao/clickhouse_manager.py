import clickhouse_connect,clickhouse_connect.driver.client
import os
from src.utils.logger import app_logger
from src.schema import MARKET_DATA_TABLES


class ClickHouseManager:
    def __init__(self):
        # 优先读取用户提供的环境变量
        self.host = os.getenv('CLICKHOST_HOST','localhost')
        self.port = int(os.getenv('CLICKHOST_PORT', 8123))
        self.username = os.getenv('CLICKHOST_USERNAME', 'default')
        self.password = os.getenv('CLICKHOST_PASSWORD', '')
        self.database = os.getenv('CLICKHOST_DATABASE', 'quant_data')
        
        # 建立连接
        try:
            self.client:clickhouse_connect.driver.client.Client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password
            )
        
            # 确保数据库存在
            self.client.command(f'CREATE DATABASE IF NOT EXISTS {self.database}')
            self.client.command(f'USE {self.database}')
        except Exception as e:
            app_logger.error(f"❌ 连接 ClickHouse 失败: {str(e)}")
            exit(1)  # 连接失败直接退出，避免后续操作报错
        # 初始化表
        self._init_all_tables()

    def _init_all_tables(self):
        
        # 循环执行建表语句
        for table_name, ddl in MARKET_DATA_TABLES.items():
            try:
                self.client.command(ddl)
                print(f"✅ 表 {table_name} 检查/创建成功")
            except Exception as e:
                print(f"❌ 表 {table_name} 创建失败: {e}")


    def close(self):
        self.client.close()

    
# ==========================================
# 🌟 Pythonic 单例模式 (懒加载)
# ==========================================
_global_db_manager = None

def get_db_manager() -> ClickHouseManager:
    """
    全局唯一获取 DB 连接的入口。
    如果是第一次调用，会建立连接；后续调用直接返回已有的连接。
    """
    global _global_db_manager
    if _global_db_manager is None:
        _global_db_manager = ClickHouseManager()
    return _global_db_manager        
if __name__ == "__main__":
    try:
        db = ClickHouseManager()
        app_logger.info("🚀 ClickHouse 连接测试成功！")
        db.close()
    except Exception as e:
        app_logger.error(f"❌ ClickHouse 连接失败: {str(e)}")
