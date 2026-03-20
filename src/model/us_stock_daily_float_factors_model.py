from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class StockDailyFloatFactorsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_daily_float_factors"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_daily_float_factors
        (
            composite_figi String,
            effective_date Date,
            free_float UInt64,
            free_float_percent Float32,
            ticker String,
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, effective_date)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str"},
        "effective_date": {"type": "date"},
        "free_float": {"type": "uint64", "default": 0},
        "free_float_percent": {"type": "float32", "default": 0.0},
    }

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        if "effective_date" in df.columns:
            df["effective_date"] = pd.to_datetime(df["effective_date"], errors="coerce").dt.date
        elif "date" in df.columns:
            df["effective_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        str_cols = {
            k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"
        }
        for col, length in str_cols.items():
            df[col] = df[col].astype(str)
            if length:
                df[col] = df[col].str.slice(0, length)

        if "free_float" in df.columns:
            df["free_float"] = (
                pd.to_numeric(df["free_float"], errors="coerce")
                .fillna(0)
                .astype("uint64")
            )
        if "free_float_percent" in df.columns:
            df["free_float_percent"] = (
                pd.to_numeric(df["free_float_percent"], errors="coerce")
                .fillna(0.0)
                .astype("float32")
            )

        return df[list(cls.SCHEMA_CLEAN.keys())]
