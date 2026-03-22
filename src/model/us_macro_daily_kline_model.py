from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any

class UsMacroDailyKlineModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_macro_daily_klines"

    __DDL__: ClassVar[str] = """
            CREATE TABLE IF NOT EXISTS us_macro_daily_klines
            (
                ticker LowCardinality(String),  -- 'US10Y', 'DXY', 'GOLD', VIX' (现货), 'VX1' (近月), 'VX2' (次月) 等
                trade_date Date,
                open Float32,
                high Float32,
                low Float32,
                close Float32,
                volume UInt64,                  --  Yahoo的宏观指标存入时自动补0
                open_interest UInt64,            -- 【核心】未平仓合约数，判断机构做空/做多恐慌盘真实资金量的关键, Yahoo数据为0, 只有VX期货有真实数值
                update_time DateTime64(3, 'UTC') DEFAULT now64(3)
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (ticker, trade_date)
        """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "ticker": {"type": "str"},
        "trade_date": {"type": "date"},
        "open": {"type": "float64", "default": 0.0},
        "high": {"type": "float64", "default": 0.0},
        "low": {"type": "float64", "default": 0.0},
        "close": {"type": "float64", "default": 0.0},
        "volume": {"type": "uint64", "default": 0},
        "open_interest": {"type": "uint64", "default": 0},
        "update_time": {"type": "datetime", "tz": "UTC"},
    }

    MAX_TRADE_DATE_QUERY_SQL: ClassVar[str] = (
        "SELECT max(trade_date) as ts FROM us_macro_daily_klines WHERE ticker IN ({symbols_str})"
    )

    @classmethod
    def build_max_trade_date_query_sql(cls, symbols: list[str]) -> str:
        return cls.MAX_TRADE_DATE_QUERY_SQL.format(symbols_str=cls.sql_in_clause(symbols))

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, default_ticker: str = '') -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        if len(default_ticker) > 0 and 'ticker' not in df.columns:
            df['ticker'] = default_ticker

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        date_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        time_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "datetime"]
        for col in time_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col], errors="coerce", utc=True
                ).dt.tz_localize(None)

        str_cols = {
            k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"
        }
        for col, length in str_cols.items():
            if col in df.columns:
                df[col] = df[col].astype(str)
                if length:
                    df[col] = df[col].str.slice(0, length)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        int_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if "int" in v["type"]]
        for col in int_cols:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce").fillna(0).astype("uint64")
                )

        return df[list(cls.SCHEMA_CLEAN.keys())]
