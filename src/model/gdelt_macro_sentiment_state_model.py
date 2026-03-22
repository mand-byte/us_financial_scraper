from typing import Any, ClassVar, Dict

import pandas as pd

from src.model.base_clickhouse_model import BaseClickHouseModel


class GdeltMacroSentimentStateModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "gdelt_macro_sentiment_state"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS gdelt_macro_sentiment_state
        (
            cursor_key String,
            last_file_ts DateTime64(3, 'UTC'),
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY cursor_key
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Dict[str, Any]]] = {
        "cursor_key": {"type": "str", "default": "gdelt_v2_15m"},
        "last_file_ts": {"type": "datetime", "tz": "UTC"},
    }

    QUERY_LATEST_CURSOR_SQL: ClassVar[str] = (
        "SELECT max(last_file_ts) AS last_ts "
        "FROM gdelt_macro_sentiment_state "
        "WHERE cursor_key = {cursor_key}"
    )

    @classmethod
    def build_query_latest_cursor_sql(cls, cursor_key: str) -> str:
        return cls.QUERY_LATEST_CURSOR_SQL.format(cursor_key=cls.sql_literal(cursor_key))

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        df = df.copy()
        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default")

        df["cursor_key"] = df["cursor_key"].fillna("gdelt_v2_15m").astype(str)
        df["last_file_ts"] = pd.to_datetime(df["last_file_ts"], errors="coerce", utc=True)

        return df[list(cls.SCHEMA_CLEAN.keys())]

