from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStock10kSectionsRawModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_10k_sections_raw"

    __DDL__: ClassVar[str] = """
            CREATE TABLE IF NOT EXISTS us_stock_10k_sections_raw
            (
                composite_figi FixedString(12),
                cik Nullable(String),
                filing_date Date,                         -- Date when the filing was submitted to the SEC (formatted as YYYY-MM-DD).
                period_end Date,                         -- Period end date that the filing relates to (formatted as YYYY-MM-DD).
                filing_url String CODEC(ZSTD(3)),          -- SEC URL source for the full filing.
                text String CODEC(ZSTD(3)),      -- Full raw text content of the section, including headers and formatting.
                section String CODEC(ZSTD(3)),        -- Standardized section identifier from the filing (e.g. 'business', 'risk_factors', etc.).
                update_time DateTime64(3, 'UTC') DEFAULT now64(3)
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi, filing_date, section)
        """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str"},
        "filing_date": {"type": "date"},
        "period_end": {"type": "date"},
        "filing_url": {"type": "str"},
        "text": {"type": "str"},
        "section": {"type": "str"},
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
