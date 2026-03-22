from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any
import numpy as np


# ==========================================
# 1. 股票宇宙表
# ==========================================
class UsStockUniverseModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_universe"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_universe
        (
            ticker String,
            composite_figi FixedString(12),
            name                 String,
            cik                  FixedString(10),
            active               UInt8 DEFAULT 0,
            base_currency_name   Nullable(String),
            base_currency_symbol Nullable(FixedString(3)),
            currency_name        Nullable(String),
            currency_symbol      Nullable(FixedString(3)),
            delisted_utc         Nullable(DateTime64(3, 'UTC')),
            last_updated_utc     DateTime64(3, 'UTC'),
            locale               LowCardinality(String),
            market               LowCardinality(String),
            primary_exchange     Nullable(String),
            share_class_figi     Nullable(FixedString(12)),
            type                 Nullable(String),
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "ticker": {"type": "str", "nullable": False},
        "composite_figi": {"type": "str", "len": 12, "nullable": False},
        "name": {"type": "str"},
        "cik": {"type": "str", "len": 10},
        "active": {"type": "int", "default": 0},
        "base_currency_name": {"type": "str"},
        "base_currency_symbol": {"type": "str", "len": 3},
        "currency_name": {"type": "str"},
        "currency_symbol": {"type": "str", "len": 3},
        "delisted_utc": {"type": "datetime", "tz": "UTC"},
        "last_updated_utc": {"type": "datetime", "tz": "UTC"},
        "locale": {"type": "str"},
        "market": {"type": "str"},
        "primary_exchange": {"type": "str"},
        "share_class_figi": {"type": "str", "len": 12},
        "type": {"type": "str"},
        "update_time": {"type": "datetime", "tz": "US/Eastern"},
    }

    QUERY_ACTIVE_TICKERS_SQL: ClassVar[str] = "SELECT * FROM us_stock_universe WHERE active = 1"
    QUERY_DELISTED_TICKERS_SQL: ClassVar[str] = "SELECT * FROM us_stock_universe WHERE active = 0"
    QUERY_ALL_TICKERS_SQL: ClassVar[str] = "SELECT * FROM us_stock_universe"
    QUERY_CIK_TO_FIGI_MAPPING_SQL: ClassVar[str] = "SELECT cik, composite_figi FROM us_stock_universe WHERE length(composite_figi) > 0"
    QUERY_SYNC_TASKS_SQL: ClassVar[str] = (
        "SELECT "
        "    u.ticker, u.cik, u.composite_figi, u.active, u.delisted_utc, "
        "    ifNull(s.state, 0) as sync_state "
        "FROM us_stock_universe u "
        "LEFT JOIN {state_table} s ON u.{id_column} = s.{id_column}"
    )

    @classmethod
    def build_query_sync_tasks_sql(cls, state_table: str, id_column: str) -> str:
        safe_state_table = cls.sql_identifier(state_table)
        safe_id_column = cls.sql_identifier(id_column)
        return cls.QUERY_SYNC_TASKS_SQL.format(
            state_table=safe_state_table,
            id_column=safe_id_column,
        )

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        """格式化与数据对齐"""
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()

        # 1. 预处理：确保空值统一为 np.nan 方便后续处理
        if "composite_figi" in df.columns:
            df["composite_figi"] = df["composite_figi"].replace(
                {None: np.nan, "": np.nan, "nan": np.nan}
            )

        # 2. 字段填充与默认值处理
        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        # 3. 统一处理时间戳
        time_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "datetime"]
        for col in time_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.tz_localize(
                None
            )

        # 4. 类型转换与字符串截断 (注意：先填充空字符串再转 str，避免 NaN 变成 "nan")
        for col, meta in cls.SCHEMA_CLEAN.items():
            if meta["type"] == "str":
                length = meta.get("len")
                # 拦截 DB 查询返回的 FixedString(bytes) 格式，显式解码
                df[col] = df[col].apply(lambda x: x.decode('utf-8', 'ignore') if isinstance(x, bytes) else x)
                df[col] = df[col].fillna("").astype(str)
                df[col] = df[col].replace({"nan": "", "None": ""})
                if length:
                    df[col] = df[col].str.slice(0, length)
            elif meta["type"] == "int":
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce")
                    .fillna(meta.get("default", 0))
                    .astype(int)
                )

        return df[list(cls.SCHEMA_CLEAN.keys())]
