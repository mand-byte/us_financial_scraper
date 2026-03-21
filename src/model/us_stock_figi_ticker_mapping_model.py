from src.model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStockFigiTickerMappingModel(BaseClickHouseModel):
    table_name = "us_stock_figi_ticker_mapping"
    __DDL__ = """
    CREATE TABLE IF NOT EXISTS us_stock_figi_ticker_mapping (
        
        composite_figi FixedString(12),
        ticker String,
        date Date,
        update_time DateTime64(3, 'UTC') DEFAULT now64(3)
          )
    ENGINE = ReplacingMergeTree(update_time)
    ORDER BY (date, ticker)
    PRIMARY KEY (date, ticker)  
    """
    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "composite_figi": {"type": "str", "len": 12},
        "ticker": {"type": "str"},
        "date": {"type": "date"},
        "update_time": {"type": "datetime64", "tz": "UTC"},
    }

    QUERY_MAPPING_BY_FIGI_SQL: ClassVar[str] = "SELECT * FROM us_stock_figi_ticker_mapping WHERE composite_figi = '{figi}'"
    QUERY_MAPPING_BY_TICKERS_SQL: ClassVar[str] = (
        "SELECT composite_figi, ticker FROM us_stock_figi_ticker_mapping WHERE ticker IN ({tickers_str})"
    )
    QUERY_MAPPINGS_HISTORY_BY_TICKERS_SQL: ClassVar[str] = (
        "SELECT composite_figi, ticker, date FROM us_stock_figi_ticker_mapping WHERE ticker IN ({tickers_str})"
    )

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=list(cls.SCHEMA_CLEAN.keys()))
        df = df.copy()

        # 自动补全/对齐模式定义的所有列
        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"])
            # ClickHouse Date 类型支持范围为 [1970-01-01, 2149-06-06]
            # 过滤掉 1970 之前的日期（struct.pack('H', ...) 不接受负值）
            df = df[df["date"] >= pd.Timestamp("1970-01-01")]
            df["date"] = df["date"].dt.date
        
        if "update_time" in df.columns:
            # 必须保证 update_time 也不为空
            df["update_time"] = pd.to_datetime(df["update_time"]).fillna(pd.Timestamp.now(tz='UTC'))
        else:
            df["update_time"] = pd.Timestamp.now(tz='UTC')

        if "composite_figi" in df.columns:
            df["composite_figi"] = df["composite_figi"].apply(lambda x: x.decode('utf-8', 'ignore') if isinstance(x, bytes) else str(x) if pd.notna(x) else "")
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].apply(lambda x: x.decode('utf-8', 'ignore') if isinstance(x, bytes) else str(x) if pd.notna(x) else "")

        # 返回符合模式定义的列顺序
        return df[list(cls.SCHEMA_CLEAN.keys())]
