import pandas as pd
import hashlib
from pydantic import  Field
from src.model import BaseClickHouseModel
from datetime import datetime
from typing import Optional, Dict


# ==========================================
# 10. 个股新闻原始表
# ==========================================
class UsStockNewsRawModel(BaseClickHouseModel):
    news_id: str = Field(...)
    composite_figi: str = Field(...)
    publish_timestamp: datetime = Field(...)
    title: str = Field(default="")
    description: str = Field(default="")
    update_time: Optional[datetime] = Field(default=None)

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, universe_map: Dict[str, str]) -> pd.DataFrame:
        """
        处理 Massive API 返回的新闻 DataFrame：
        1. 按 ticker 分割 (Explode)
        2. 映射 composite_figi
        3. 生成 news_id (title + timestamp 的 MD5)
        4. 对齐表结构
        """
        if df.empty:
            return df

        # 1. 🌟 按 ticker 展开
        # massive_api 返回的 tickers 是 "AAPL,MSFT" 格式的字符串
        df["ticker_list"] = df["tickers"].str.split(",")
        df = df.explode("ticker_list")
        df = df.rename(columns={"ticker_list": "ticker"})

        # 2. 🌟 映射 composite_figi
        df["composite_figi"] = df["ticker"].map(universe_map)
        # 丢弃无法映射 figi 的新闻（可能属于非美股或异常标的）
        df = df.dropna(subset=["composite_figi"])

        # 3. 🌟 时间戳对齐
        df["publish_timestamp"] = pd.to_datetime(df["published_utc"], utc=True)

        # 4. 🌟 生成 news_id
        # 使用 title + published_utc 作为唯一标识生成 MD5，保证 ReplacingMergeTree 幂等
        def generate_id(row):
            payload = f"{row['title']}_{row['published_utc']}_{row['composite_figi']}"
            return hashlib.md5(payload.encode("utf-8")).hexdigest()

        df["news_id"] = df.apply(generate_id, axis=1)

        # 5. 补全系统列
        df["update_time"] = pd.Timestamp.now(tz="UTC")

        # 6. 对齐模型定义的列
        cols = cls.get_columns()
        return df[cols].copy()