from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class UsStockNewsRawModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "us_stock_news_raw"

    __DDL__: ClassVar[str] = """
            CREATE TABLE IF NOT EXISTS us_stock_news_raw
            (
                news_id String,                          -- 建议用 URL 或 (标题+时间) 的 MD5 哈希
                composite_figi FixedString(12),
                published_utc DateTime64(3, 'UTC'),
                title String CODEC(ZSTD(3)),             -- 长文本开启 ZSTD 高级压缩
                description String CODEC(ZSTD(3)),
                update_time DateTime64(3, 'UTC') DEFAULT now64(3)
            ) ENGINE = ReplacingMergeTree(update_time)
            ORDER BY (composite_figi, published_utc, news_id)
        """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "news_id": {"type": "str"},
        "composite_figi": {"type": "str"},
        "published_utc": {"type": "datetime", "tz": "UTC"},
        "title": {"type": "str"},
        "description": {"type": "str"},
        "update_time": {"type": "datetime", "tz": "UTC"},
    }
    MAX_PUBLISHED_UTC_QUERY_SQL = (
        "SELECT max(published_utc) as last_ts FROM us_stock_news_raw"
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
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        int_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if "int" in v["type"]]
        for col in int_cols:
            if col in df.columns:
                df[col] = (
                    pd.to_numeric(df[col], errors="coerce").fillna(0).astype("uint64")
                )

        return df[list(cls.SCHEMA_CLEAN.keys())]
