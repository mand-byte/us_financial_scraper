from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any

class UsStockDividendsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_dividends"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_dividends
        (
            id String,
            ticker String,
            ex_dividend_date Date,
            declaration_date Nullable(Date),
            record_date Nullable(Date),
            pay_date Nullable(Date),
            cash_amount Nullable(Float64),
            split_adjusted_cash_amount Nullable(Float64),
            currency LowCardinality(Nullable(String)),
            distribution_type LowCardinality(String),
            frequency Nullable(UInt16),
            historical_adjustment_factor Nullable(Float64),
            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (ticker, ex_dividend_date, id)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "id": {"type": "str"},
        "ticker": {"type": "str"},
        "ex_dividend_date": {"type": "date"},
        "declaration_date": {"type": "date"},
        "record_date": {"type": "date"},
        "pay_date": {"type": "date"},
        "cash_amount": {"type": "float64"},
        "split_adjusted_cash_amount": {"type": "float64"},
        "currency": {"type": "str"},
        "distribution_type": {"type": "str"},
        "frequency": {"type": "float64"},
        "historical_adjustment_factor": {"type": "float64"},
    }

    QUERY_LATEST_EX_DATE_BY_FIGI_SQL: ClassVar[str] = "SELECT max(ex_dividend_date) as last_date FROM us_stock_dividends WHERE composite_figi = '{composite_figi}'"
    QUERY_GLOBAL_LATEST_EX_DATE_SQL: ClassVar[str] = "SELECT max(ex_dividend_date) as last_date FROM us_stock_dividends"

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

        str_cols = {k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"}
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
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        return df[list(cls.SCHEMA_CLEAN.keys())]
