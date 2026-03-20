from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class StockDailyShortVolumeFactorsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_daily_short_volume_factors"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_daily_short_volume_factors
        (
            ticker String,
            composite_figi FixedString(12),

            date Date,   -- 交易日（核心）

            -- 总体
            total_volume Nullable(UInt64),
            short_volume Nullable(UInt64),
            exempt_volume Nullable(UInt64),
            non_exempt_volume Nullable(UInt64),
            short_volume_ratio Nullable(Float32),

            -- ADF
            adf_short_volume Nullable(UInt64),
            adf_short_volume_exempt Nullable(UInt64),

            -- NASDAQ Carteret
            nasdaq_carteret_short_volume Nullable(UInt64),
            nasdaq_carteret_short_volume_exempt Nullable(UInt64),

            -- NASDAQ Chicago
            nasdaq_chicago_short_volume Nullable(UInt64),
            nasdaq_chicago_short_volume_exempt Nullable(UInt64),

            -- NYSE
            nyse_short_volume Nullable(UInt64),
            nyse_short_volume_exempt Nullable(UInt64),

            -- 更新时间（用于Replacing）
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, date)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str", "len": 12},
        "trade_date": {"type": "date"},
        "short_volume": {"type": "uint64", "default": 0},
        "short_volume_ratio": {"type": "float32", "default": 0.0},
    }

    QUERY_LATEST_TRADE_DATE_BY_FIGI_SQL: ClassVar[str] = "SELECT max(trade_date) as last_ts FROM us_stock_daily_short_volume WHERE composite_figi = '{composite_figi}'"

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        elif "date" in df.columns:
            df["trade_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        str_cols = {
            k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"
        }
        for col, length in str_cols.items():
            df[col] = df[col].astype(str)
            if length:
                df[col] = df[col].str.slice(0, length)

        if "short_volume" in df.columns:
            df["short_volume"] = (
                pd.to_numeric(df["short_volume"], errors="coerce")
                .fillna(0)
                .astype("uint64")
            )
        if "short_volume_ratio" in df.columns:
            df["short_volume_ratio"] = (
                pd.to_numeric(df["short_volume_ratio"], errors="coerce")
                .fillna(0.0)
                .astype("float32")
            )

        return df[list(cls.SCHEMA_CLEAN.keys())]
