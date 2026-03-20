from typing import Any, ClassVar, Dict

import pandas as pd

from src.model.base_clickhouse_model import BaseClickHouseModel


class GdeltMacroSentimentModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "gdelt_macro_sentiment"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS gdelt_macro_sentiment
        (
            publish_timestamp DateTime64(3, 'UTC'),
            count_16 UInt32 DEFAULT 0,
            tone_16 Float64 DEFAULT 0,
            impact_16 Float64 DEFAULT 0,
            count_17 UInt32 DEFAULT 0,
            tone_17 Float64 DEFAULT 0,
            impact_17 Float64 DEFAULT 0,
            count_18 UInt32 DEFAULT 0,
            tone_18 Float64 DEFAULT 0,
            impact_18 Float64 DEFAULT 0,
            count_19 UInt32 DEFAULT 0,
            tone_19 Float64 DEFAULT 0,
            impact_19 Float64 DEFAULT 0,
            count_20 UInt32 DEFAULT 0,
            tone_20 Float64 DEFAULT 0,
            impact_20 Float64 DEFAULT 0,
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY publish_timestamp
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Dict[str, Any]]] = {
        "publish_timestamp": {"type": "datetime", "tz": "UTC"},
        "count_16": {"type": "uint32", "default": 0},
        "tone_16": {"type": "float64", "default": 0.0},
        "impact_16": {"type": "float64", "default": 0.0},
        "count_17": {"type": "uint32", "default": 0},
        "tone_17": {"type": "float64", "default": 0.0},
        "impact_17": {"type": "float64", "default": 0.0},
        "count_18": {"type": "uint32", "default": 0},
        "tone_18": {"type": "float64", "default": 0.0},
        "impact_18": {"type": "float64", "default": 0.0},
        "count_19": {"type": "uint32", "default": 0},
        "tone_19": {"type": "float64", "default": 0.0},
        "impact_19": {"type": "float64", "default": 0.0},
        "count_20": {"type": "uint32", "default": 0},
        "tone_20": {"type": "float64", "default": 0.0},
        "impact_20": {"type": "float64", "default": 0.0},
    }

    QUERY_GLOBAL_LATEST_PUBLISH_TS_SQL: ClassVar[str] = (
        "SELECT max(publish_timestamp) AS last_ts FROM gdelt_macro_sentiment"
    )

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        df = df.copy()
        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default")

        for col, meta in cls.SCHEMA_CLEAN.items():
            value_type = meta["type"]
            if value_type == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            elif value_type == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(meta.get("default", 0.0))
            elif value_type.startswith("uint"):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(meta.get("default", 0)).astype("uint32")

        return df[list(cls.SCHEMA_CLEAN.keys())]
