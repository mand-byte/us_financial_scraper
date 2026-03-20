from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class StockDailyShortInterestFactorsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_daily_short_interest_factors"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_daily_short_interest_factors
        (
            composite_figi FixedString(12),
            ticker              String,
            settlement_date     Date,
            avg_daily_volume    UInt64,
            days_to_cover       Float32,
            short_interest      Nullable(UInt64),
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, settlement_date)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str", "len": 12},
        "ticker": {"type": "str"},
        "settlement_date": {"type": "date"},
        "avg_daily_volume": {"type": "uint64", "default": 0},
        "days_to_cover": {"type": "float32", "default": 0.0},
        "short_interest": {"type": "uint64", "default": 0},
    }

    QUERY_LATEST_SETTLEMENT_DATE_BY_FIGI_SQL: ClassVar[str] = "SELECT max(settlement_date) as last_ts FROM us_stock_daily_short_interest WHERE composite_figi = '{composite_figi}'"

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        if "settlement_date" in df.columns:
            df["settlement_date"] = pd.to_datetime(
                df["settlement_date"], errors="coerce"
            ).dt.date
        elif "date" in df.columns:
            df["settlement_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        str_cols = {
            k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"
        }
        for col, length in str_cols.items():
            df[col] = df[col].astype(str)
            if length:
                df[col] = df[col].str.slice(0, length)

        if "avg_daily_volume" in df.columns:
            df["avg_daily_volume"] = (
                pd.to_numeric(df["avg_daily_volume"], errors="coerce")
                .fillna(0)
                .astype("uint64")
            )
        if "short_interest" in df.columns:
            df["short_interest"] = (
                pd.to_numeric(df["short_interest"], errors="coerce")
                .fillna(0)
                .astype("uint64")
            )
        if "days_to_cover" in df.columns:
            df["days_to_cover"] = (
                pd.to_numeric(df["days_to_cover"], errors="coerce")
                .fillna(0.0)
                .astype("float32")
            )

        return df[list(cls.SCHEMA_CLEAN.keys())]
