from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any

class StockDailyRatiosFactorsModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_daily_ratios_factors"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS us_stock_daily_ratios_factors
        (
            composite_figi FixedString(12),
            ticker                      String,
            date                        Date,
            cik                         Nullable(String),
            average_volume              Nullable(Float64),
            cash                        Nullable(Float64),
            current                     Nullable(Float64),
            debt_to_equity              Nullable(Float64),
            dividend_yield              Nullable(Float64),
            earnings_per_share          Nullable(Float64),
            enterprise_value            Nullable(Float64),
            ev_to_ebitda                Nullable(Float64),
            ev_to_sales                 Nullable(Float64),
            free_cash_flow              Nullable(Float64),
            market_cap                  Nullable(Float64),
            price                       Nullable(Float64),
            price_to_book               Nullable(Float64),
            price_to_cash_flow          Nullable(Float64),
            price_to_earnings           Nullable(Float64),
            price_to_free_cash_flow     Nullable(Float64),
            price_to_sales              Nullable(Float64),
            quick                       Nullable(Float64),
            return_on_assets            Nullable(Float64),
            return_on_equity            Nullable(Float64)
        ) ENGINE = ReplacingMergeTree(date)
        ORDER BY (composite_figi, date)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str", "len": 12},
        "ticker": {"type": "str"},
        "date": {"type": "date"},
        "cik": {"type": "str"},
        "average_volume": {"type": "float64"},
        "cash": {"type": "float64"},
        "current": {"type": "float64"},
        "debt_to_equity": {"type": "float64"},
        "dividend_yield": {"type": "float64"},
        "earnings_per_share": {"type": "float64"},
        "enterprise_value": {"type": "float64"},
        "ev_to_ebitda": {"type": "float64"},
        "ev_to_sales": {"type": "float64"},
        "free_cash_flow": {"type": "float64"},
        "market_cap": {"type": "float64"},
        "price": {"type": "float64"},
        "price_to_book": {"type": "float64"},
        "price_to_cash_flow": {"type": "float64"},
        "price_to_earnings": {"type": "float64"},
        "price_to_free_cash_flow": {"type": "float64"},
        "price_to_sales": {"type": "float64"},
        "quick": {"type": "float64"},
        "return_on_assets": {"type": "float64"},
        "return_on_equity": {"type": "float64"},
    }

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

        str_cols = {k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"}
        for col, length in str_cols.items():
            df[col] = df[col].astype(str)
            if length:
                df[col] = df[col].str.slice(0, length)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        return df[list(cls.SCHEMA_CLEAN.keys())]
