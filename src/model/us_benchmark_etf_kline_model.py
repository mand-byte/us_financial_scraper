from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


# ==========================================
# 4. 基准 ETF K线表
# ==========================================
class BenchmarkEtfKlineModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_benchmark_etf_klines"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_benchmark_etf_klines
        (
            ticker LowCardinality(String),
            timestamp        DateTime64(3, 'UTC'),
            open             Float64,
            high             Float64,
            low              Float64,
            close            Float64,
            vwap             Float64 DEFAULT 0,
            trades_count     UInt64,
            volume           Float64,
            otc              UInt8 DEFAULT 0
        ) ENGINE = ReplacingMergeTree(timestamp)
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (ticker, timestamp)
        SETTINGS index_granularity = 8192;
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "ticker": {"type": "str", "len": 12},
        "timestamp": {"type": "datetime", "tz": "UTC"},
        "open": {"type": "float64"},
        "high": {"type": "float64"},
        "low": {"type": "float64"},
        "close": {"type": "float64"},
        "vwap": {"type": "float64", "default": 0.0},
        "trades_count": {"type": "uint64", "default": 0},
        "volume": {"type": "float64", "default": 0.0},
        "otc": {"type": "uint8", "default": 0},
    }

    QUERY_LATEST_TS_BY_TICKER_SQL: ClassVar[str] = "SELECT max(timestamp) as last_ts FROM us_benchmark_etf_klines WHERE ticker = '{ticker}'"

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()

        rename_map = {
            "t": "timestamp",
            "o": "open",
            "h": "high",
            "l": "low",
            "c": "close",
            "v": "volume",
            "vw": "vwap",
            "n": "trades_count",
            "otc": "otc",
        }
        df = df.rename(columns=rename_map)

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        df["ticker"] = str(ticker)

        df["timestamp"] = pd.to_datetime(
            df["timestamp"], unit="ms", utc=True
        ).dt.tz_localize(None)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")
            )

        df["trades_count"] = (
            pd.to_numeric(df["trades_count"], errors="coerce")
            .fillna(0)
            .astype("uint64")
        )
        if "otc" in df.columns:
            df["otc"] = df["otc"].fillna(0).replace({True: 1, False: 0}).astype("uint8")

        return df[list(cls.SCHEMA_CLEAN.keys())]
