import clickhouse_connect
import os
import pkgutil
import importlib
import pandas as pd
import threading
import time
from src.utils.logger import app_logger
from clickhouse_connect.driver.exceptions import DatabaseError
from src.config.settings import settings

# 引入顶层包以供扫描
import src.model
from src.model.base_clickhouse_model import BaseClickHouseModel
from typing import Type


class ClickHouseConnectionError(RuntimeError):
    """Raised when ClickHouse cannot be initialized for scraper runtime."""


class ClickHouseManager:
    _schema_init_lock = threading.Lock()
    _schema_initialized = False

    def __init__(self):
        # 优先读取用户提供的环境变量
        self.host = settings.db.clickhouse_host
        self.port = settings.db.clickhouse_port
        self.username = settings.db.clickhouse_username
        self.password = settings.db.clickhouse_password
        self.database = settings.db.clickhouse_database
        self.retry_attempts = settings.db.db_operation_retry_attempts
        self.retry_backoff_seconds = settings.db.db_retry_backoff_seconds
        self.circuit_open_seconds = settings.db.db_circuit_open_seconds
        self.write_fail_exit_threshold = settings.db.db_write_fail_exit_threshold
        self._consecutive_write_failures = 0
        self._circuit_open_until = 0.0
        self._state_lock = threading.Lock()

        self.client: clickhouse_connect.driver.client.Client = self._build_client()

        try:
            # Monkey Patch: 全局自动剔除值为全空的 update_time，交由 CH default now64(3) 计算，避免 None 序列化报错
            # 同时确保每次 insert 都带上正确的 database 参数，防止漂移到 default
            original_insert_df = self.client.insert_df

            def custom_insert_df(table, df, *args, **kwargs):
                self._assert_circuit_closed("insert")
                if df is not None and not df.empty and "update_time" in df.columns:
                    if df["update_time"].isna().all():
                        df = df.drop(columns=["update_time"])
                if "column_names" not in kwargs and df is not None:
                    kwargs["column_names"] = tuple(df.columns)

                # 强行注入数据库上下文，防止 clickhouse-connect 默认回到 'default'
                if "database" not in kwargs:
                    kwargs["database"] = self.database

                def _do_insert():
                    return original_insert_df(table, df, *args, **kwargs)

                try:
                    result = self._with_retry(_do_insert, op_name=f"insert {table}")
                    self._on_write_success()
                    return result
                except Exception as exc:
                    self._on_write_failure(table, exc)
                    raise

            original_query_df = self.client.query_df

            def custom_query_df(*args, **kwargs):
                self._assert_circuit_closed("query")

                def _do_query():
                    return original_query_df(*args, **kwargs)

                return self._with_retry(_do_query, op_name="query_df")

            self.client.insert_df = custom_insert_df
            self.client.query_df = custom_query_df

        except Exception as e:
            raise ClickHouseConnectionError(f"连接 ClickHouse 失败: {e}") from e

        # 激活所有模型并建表（进程内只执行一次）
        self._initialize_schema_once()

    def _build_client(self) -> clickhouse_connect.driver.client.Client:
        """Create a db-bound client, and only fallback when database is missing."""
        try:
            return clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
        except DatabaseError as exc:
            err_text = str(exc).upper()
            if "UNKNOWN_DATABASE" not in err_text and "CODE: 81" not in err_text:
                raise ClickHouseConnectionError(f"连接 ClickHouse 失败: {exc}") from exc

            try:
                bootstrap_client = clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                )
                bootstrap_client.command(
                    f"CREATE DATABASE IF NOT EXISTS {self.database}"
                )
                bootstrap_client.close()
                return clickhouse_connect.get_client(
                    host=self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    database=self.database,
                )
            except Exception as create_exc:
                raise ClickHouseConnectionError(
                    f"连接 ClickHouse 或创建数据库失败: {create_exc}"
                ) from create_exc
        except Exception as exc:
            raise ClickHouseConnectionError(f"连接 ClickHouse 失败: {exc}") from exc

    def _with_retry(self, func, op_name: str):
        last_error = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                return func()
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                app_logger.warning(
                    f"⚠️ ClickHouse {op_name} 失败，准备重试 {attempt}/{self.retry_attempts - 1}: {exc}"
                )
                time.sleep(self.retry_backoff_seconds * attempt)
        raise ClickHouseConnectionError(
            f"ClickHouse {op_name} 在 {self.retry_attempts} 次尝试后仍失败: {last_error}"
        ) from last_error

    def _assert_circuit_closed(self, op_name: str) -> None:
        with self._state_lock:
            if time.time() < self._circuit_open_until:
                raise ClickHouseConnectionError(
                    f"ClickHouse 熔断中，拒绝执行 {op_name}，恢复时间戳: {self._circuit_open_until:.0f}"
                )

    def _on_write_success(self) -> None:
        with self._state_lock:
            self._consecutive_write_failures = 0
            self._circuit_open_until = 0.0

    def _on_write_failure(self, table: str, exc: Exception) -> None:
        with self._state_lock:
            self._consecutive_write_failures += 1
            failures = self._consecutive_write_failures
            self._circuit_open_until = time.time() + self.circuit_open_seconds

        app_logger.error(
            f"❌ 写入 {table} 失败，连续失败 {failures} 次，已打开熔断 {self.circuit_open_seconds}s: {exc}"
        )

        if failures >= self.write_fail_exit_threshold:
            app_logger.error(
                f"🛑 连续写入失败达到阈值 {self.write_fail_exit_threshold}，进程将退出并等待外部拉起。"
            )
            os._exit(2)

    def _load_all_models(self):
        """自动扫描 src.model 下所有的模块并导入，触发所有子类的 __init_subclass__ 注册"""
        package = src.model
        for _, module_name, is_pkg in pkgutil.walk_packages(package.__path__):
            if not is_pkg:
                try:
                    importlib.import_module(f"{package.__name__}.{module_name}")
                except Exception as e:
                    app_logger.warning(f"⚠️ 无法自动加载模型 {module_name}: {e}")

    def _initialize_schema_once(self) -> None:
        if ClickHouseManager._schema_initialized:
            return

        with ClickHouseManager._schema_init_lock:
            if ClickHouseManager._schema_initialized:
                return
            self._load_all_models()
            self._init_all_tables()
            ClickHouseManager._schema_initialized = True

    def _init_all_tables(self):
        """从 BaseClickHouseModel 注册中心全自动化获取并执行 DDL"""
        models = BaseClickHouseModel._registry.values()
        failed_models: list[str] = []
        initialized_tables: list[str] = []

        for model in models:
            try:
                ddl = model.get_create_table_sql()
                self.client.command(ddl)
                initialized_tables.append(model.table_name)
                app_logger.debug(f"✅ 表 {model.table_name} 自动注册并初始化成功")
            except Exception as e:
                app_logger.error(f"❌ 表 {model.table_name} 初始化失败: {e}")
                failed_models.append(model.table_name)

        if failed_models:
            raise ClickHouseConnectionError(
                f"以下表初始化失败: {', '.join(failed_models)}"
            )

        app_logger.info(
            f"✅ ClickHouse Schema 已就绪: 本轮校验/初始化 {len(initialized_tables)} 张表"
        )

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
