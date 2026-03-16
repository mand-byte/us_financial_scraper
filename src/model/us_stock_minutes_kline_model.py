from pydantic import Field
from datetime import datetime
from typing import List
import pandas as pd
import numpy as np
from src.model import BaseClickHouseModel


# ==========================================
# 2. 美股分钟 K 线表
# ==========================================
class UsStockMinutesKlineModel(BaseClickHouseModel):
    # 1. 定义严格的字段和类型
    composite_figi: str = Field(...)
    timestamp: datetime = Field(...)
    open: float = Field(default=0.0)
    high: float = Field(default=0.0)
    low: float = Field(default=0.0)
    close: float = Field(default=0.0)
    vwap: float = Field(default=0.0)
    trades_count: int = Field(default=0)
    volume: int = Field(default=0)

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, ticker: str, figi: str) -> pd.DataFrame:
        if df.empty:
            return df

        # 1. 基础映射与补全 (针对 API 返回的原始列表)
        # 你的 fetch_klines 已经把 o, c 映射成了 open, close，这里确保元数据到位
        df["ticker"] = ticker
        df["composite_figi"] = figi

        # 2. 🌟 核心：时间戳处理 (ms -> UTC DateTime)
        # unit='ms' 对应 API 的 "t" 字段，utc=True 确保时区正确
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)

        # 3. 类型强制转换 (防止 ClickHouse 插入时因 Float64/Float32 不匹配报错)
        # 根据你 DB 的定义，通常价格用 Float32 或 Decimal，成交量用 UInt64
        type_mapping = {
            "open": "float32",
            "high": "float32",
            "low": "float32",
            "close": "float32",
            "vwap": "float32",
            "volume": "uint64",
            "trades_count": "uint32",
        }

        for col, dtype in type_mapping.items():
            if col in df.columns:
                # 处理可能出现的 NaN，ClickHouse 非 Nullable 列不能接受 NaN
                if "float" in dtype:
                    df[col] = df[col].fillna(0.0).astype(dtype)
                else:
                    df[col] = df[col].fillna(0).astype(dtype)

        # 4. 增加系统列
        df["update_time"] = pd.Timestamp.now(tz="UTC")

        # 5. 按照模型定义的列顺序对齐
        cols = cls.get_columns()  # 假设你基类有这个方法获取字段列表
        return df[cols].copy()
