from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStockSplitsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_splits"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_splits
        (
            id String,
            adjustment_type LowCardinality(String),
            ticker String,
            execution_date Date,
            historical_adjustment_factor Float32,
            split_from Float32,
            split_to Float32,
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (ticker, execution_date, id)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "id": {"type": "str"},
        "adjustment_type": {"type": "str"},
        "ticker": {"type": "str"},
        "execution_date": {"type": "date"},
        "historical_adjustment_factor": {"type": "float64", "default": 0.0},
        "split_from": {"type": "float64", "default": 0.0},
        "split_to": {"type": "float64", "default": 0.0},
    }

    QUERY_LATEST_EX_DATE_BY_FIGI_SQL: ClassVar[str] = "SELECT max(execution_date) as last_date FROM us_stock_splits WHERE composite_figi = '{composite_figi}'"
    QUERY_GLOBAL_LATEST_EXECUTION_DATE_SQL: ClassVar[str] = "SELECT max(execution_date) as last_date FROM us_stock_splits"

    @classmethod
    def build_query_latest_execution_date_by_figi_sql(cls, composite_figi: str) -> str:
        return (
            "SELECT max(execution_date) as last_date "
            "FROM us_stock_splits "
            f"WHERE composite_figi = {cls.sql_literal(composite_figi)}"
        )

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        if "execution_date" in df.columns:
            df["execution_date"] = pd.to_datetime(
                df["execution_date"], errors="coerce"
            ).dt.date

        str_cols = {
            k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"
        }
        for col, length in str_cols.items():
            # 拦截 DB 查询返回的 FixedString(bytes) 格式，显式解码
            df[col] = df[col].apply(
                lambda x: x.decode("utf-8", "ignore") if isinstance(x, bytes) else x
            )
            df[col] = df[col].fillna("").astype(str)
            df[col] = df[col].replace({"nan": "", "None": ""})
            if length:
                df[col] = df[col].str.slice(0, length)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            df[col] = (
                pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")
            )

        return df[list(cls.SCHEMA_CLEAN.keys())]
