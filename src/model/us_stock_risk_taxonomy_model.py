from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStockRiskTaxonomyModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_risk_taxonomy"

    __DDL__: ClassVar[str] = """
            CREATE TABLE IF NOT EXISTS us_stock_risk_taxonomy
            (
                primary_category String,
                secondary_category String,
                tertiary_category String,
                description String,
                taxonomy Float32,
                update_time DateTime64(3, 'UTC') DEFAULT now64(3)
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (primary_category, secondary_category, tertiary_category)
        """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "primary_category": {"type": "str"},
        "secondary_category": {"type": "str"},
        "tertiary_category": {"type": "str"},
        "description": {"type": "str"},
        "taxonomy": {"type": "float64", "default": 0.0},
        "update_time": {"type": "datetime", "tz": "UTC"},
    }

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

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
