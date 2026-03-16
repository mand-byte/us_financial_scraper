from src.model import BaseClickHouseModel
from pydantic import Field
from datetime import date
from typing import Optional
from datetime import datetime
import pandas as pd


# ==========================================
# 1. 股票宇宙表
# ==========================================
class UsStockUniverseModel(BaseClickHouseModel):
    ticker: str = Field(...)
    composite_figi: str = Field(...)
    name: str = Field(...)
    cik: str = Field(...)
    active: int = Field(default=1)  # UInt8
    delisted_date: Optional[date] = Field(default=None)
    update_time: Optional[datetime] = Field(default=None)

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # 1. 强制对齐列（你原有的逻辑）
        cols = cls.get_columns()
        for col in cols:
            if col not in df.columns:
                df[col] = cls.model_fields[col].default

        # 2. 🌟 关键：处理 delisted_date 类型转换
        if "delisted_date" in df.columns:
            # 将 ISO 字符串转为 datetime 对象，再提取 date 部分
            # errors='coerce' 会将无法解析的列或 None 转为 NaT (对应 ClickHouse 的 Nullable)
            df["delisted_date"] = pd.to_datetime(
                df["delisted_date"], errors="coerce"
            ).dt.date

        return df[cols].copy()
