import clickhouse_connect
import os
import pkgutil
import importlib
import pandas as pd
from src.utils.logger import app_logger

# 引入顶层包以供扫描
import src.model
from src.model.base_clickhouse_model import BaseClickHouseModel
from typing import Type


class ClickHouseManager:
    def __init__(self):
        # 优先读取用户提供的环境变量
        self.host = os.getenv("CLICKHOST_HOST", "localhost")
        self.port = int(os.getenv("CLICKHOST_PORT", 8123))
        self.username = os.getenv("CLICKHOST_USERNAME", "default")
        self.password = os.getenv("CLICKHOST_PASSWORD", "")
        self.database = os.getenv("CLICKHOST_DATABASE", "quant_data")

        # 建立连接
        try:
            self.client: clickhouse_connect.driver.client.Client = (
                clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                )
            )

            # 确保数据库存在
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.client.command(f"USE {self.database}")
        except Exception as e:
            app_logger.error(f"❌ 连接 ClickHouse 失败: {str(e)}")
            exit(1)  # 连接失败直接退出，避免后续操作报错
            
        # 激活所有模型并建表
        self._load_all_models()
        self._init_all_tables()

    def _load_all_models(self):
        """自动扫描 src.model 下所有的模块并导入，触发所有子类的 __init_subclass__ 注册"""
        package = src.model
        for _, module_name, is_pkg in pkgutil.walk_packages(package.__path__):
            if not is_pkg:
                try:
                    importlib.import_module(f"{package.__name__}.{module_name}")
                except Exception as e:
                    app_logger.warning(f"⚠️ 无法自动加载模型 {module_name}: {e}")

    def _init_all_tables(self):
        """从 BaseClickHouseModel 注册中心全自动化获取并执行 DDL"""
        models = BaseClickHouseModel._registry.values()

        for model in models:
            try:
                ddl = model.get_create_table_sql()
                self.client.command(ddl)
                app_logger.info(f"✅ 表 {model.table_name} 自动注册并初始化成功")
            except Exception as e:
                app_logger.error(f"❌ 表 {model.table_name} 初始化失败: {e}")

    def insert_model_df(self, model_cls: Type[BaseClickHouseModel], df: pd.DataFrame):
        """通用模型写入方法"""
        if df.empty:
            return
        self.client.insert_df(table=model_cls.table_name, df=df)

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
