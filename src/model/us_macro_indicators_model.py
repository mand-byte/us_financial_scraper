from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any

class UsMacroIndicatorsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_macro_indicators"

    __DDL__: ClassVar[str] = """
                CREATE TABLE IF NOT EXISTS us_macro_indicators
                (
                    publish_timestamp DateTime64(3, 'UTC'),
                    indicator_code String,
                    actual_value Float32,
                    expected_value Nullable(Float32),
                    surprise_diff Float32 MATERIALIZED (actual_value - ifNull(expected_value, actual_value)),
                    update_time DateTime64(3, 'UTC') DEFAULT now64(3)
                ) ENGINE = ReplacingMergeTree(update_time)
                ORDER BY (indicator_code, publish_timestamp)
            """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "publish_timestamp": {"type": "datetime", "tz": "UTC"},
        "indicator_code": {"type": "str"},
        "actual_value": {"type": "float64", "default": 0.0},
        "expected_value": {"type": "float64", "default": None},
    }

    MAX_PUBLISHED_TIMESTAMP_QUERY_SQL: ClassVar[str] = (
        "SELECT max(publish_timestamp) as last_ts FROM us_macro_indicators WHERE indicator_code IN ({target_codes})"
    )

    @classmethod
    def build_max_published_timestamp_query_sql(cls, target_codes: list[str]) -> str:
        return cls.MAX_PUBLISHED_TIMESTAMP_QUERY_SQL.format(
            target_codes=cls.sql_in_clause(target_codes)
        )

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
