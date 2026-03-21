import clickhouse_connect
import os
import pkgutil
import importlib
import pandas as pd
import threading
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
            # 首次尝试带数据库名连接
            self.client: clickhouse_connect.driver.client.Client = (
                clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    database=self.database,
                )
            )
        except Exception:
            # 如果数据库不存在，则连默认库并创建
            self.client: clickhouse_connect.driver.client.Client = (
                clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                )
            )
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.client.command(f"USE {self.database}")

        try:
            # Monkey Patch: 全局自动剔除值为全空的 update_time，交由 CH default now64(3) 计算，避免 None 序列化报错
            # 同时确保每次 insert 都带上正确的 database 参数，防止漂移到 default
            original_insert_df = self.client.insert_df

            def custom_insert_df(table, df, *args, **kwargs):
                if df is not None and not df.empty and "update_time" in df.columns:
                    if df["update_time"].isna().all():
                        df = df.drop(columns=["update_time"])
                if "column_names" not in kwargs and df is not None:
                    kwargs["column_names"] = tuple(df.columns)

                # 强行注入数据库上下文，防止 clickhouse-connect 默认回到 'default'
                if "database" not in kwargs:
                    kwargs["database"] = self.database

                return original_insert_df(table, df, *args, **kwargs)

            self.client.insert_df = custom_insert_df

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

    def query_dataframe(self, sql: str) -> pd.DataFrame:
        """执行 SQL 查询并返回 DataFrame"""
        result = self.client.query(sql)
        if isinstance(result.result_set, list):
            columns = result.column_names
            return pd.DataFrame(result.result_set, columns=columns)
        return result.result_set.to_pandas()

    def close(self):
        self.client.close()


# ==========================================
# 🌟 Pythonic 单例模式 (懒加载)
# ==========================================

_thread_local = threading.local()


def get_db_manager() -> ClickHouseManager:
    """
    全局唯一获取 DB 连接的入口。
    为了解决并发报错，改为基于 thread_local 的伪单例，每个线程持有一个专门的 Client。
    """
    if getattr(_thread_local, "db_manager", None) is None:
        _thread_local.db_manager = ClickHouseManager()
    return _thread_local.db_manager


if __name__ == "__main__":
    try:
        db = ClickHouseManager()
        app_logger.info("🚀 ClickHouse 连接测试成功！")
        db.close()
    except Exception as e:
        app_logger.error(f"❌ ClickHouse 连接失败: {str(e)}")
