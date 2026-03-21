from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any

# ==========================================
# 2. 个股 K线表
# ==========================================
class UsStockMinutesKlineModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_minutes_klines"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_minutes_klines
        (
            composite_figi   FixedString(12),
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
        ORDER BY (composite_figi, timestamp)
        SETTINGS index_granularity = 8192;
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str", "len": 12},
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

    QUERY_LATEST_TS_BY_GROUP_SQL: ClassVar[str] = "SELECT composite_figi, MAX(timestamp) as last_ts FROM us_minutes_klines GROUP BY composite_figi"

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, composite_figi: str) -> pd.DataFrame:
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
        }
        df = df.rename(columns=rename_map)

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        df["composite_figi"] = df["composite_figi"].apply(
            lambda x: x.decode("utf-8") if isinstance(x, bytes) else str(x) if pd.notna(x) else ""
        )
        df["composite_figi"] = (
            df["composite_figi"]
            .fillna(str(composite_figi))
            .astype(str)
            .str.slice(0, 12)
        )

        df["timestamp"] = pd.to_datetime(
            df["timestamp"], unit="ms", utc=True
        ).dt.tz_localize(None)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")

        df["trades_count"] = pd.to_numeric(df["trades_count"], errors="coerce").fillna(0).astype("uint64")
        df["otc"] = df["otc"].fillna(0).replace({True: 1, False: 0}).astype("uint8")

        return df[list(cls.SCHEMA_CLEAN.keys())]
