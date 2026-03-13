from pydantic import  Field
from datetime import date
from typing import List
import pandas as pd
import numpy as np
from src.model import BaseClickHouseModel
class UsMacroDailyKlineModel(BaseClickHouseModel):
    # 1. 定义严格的字段和类型
    ticker: str = Field(..., description="合约或标的名称, 如 VX1, ^TNX")
    trade_date: date = Field(..., description="交易日期")
    open: float = Field(default=0.0)
    high: float = Field(default=0.0)
    low: float = Field(default=0.0)
    close: float = Field(default=0.0)
    volume: int = Field(default=0)
    open_interest: int = Field(default=0)

    @classmethod
    def get_columns(cls) -> List[str]:
        return list(cls.model_fields.keys())

    # 🌟 修复: 变量名改为 default_ticker
    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, default_ticker: str = '') -> pd.DataFrame:
        if df.empty:
            return df

        # 🌟 修复: 字符串全部改成 'ticker'
        if len(default_ticker) > 0 and 'ticker' not in df.columns:
            df['ticker'] = default_ticker

        cols = cls.get_columns()
        
        for col in cols:
            if col not in df.columns:
                df[col] = cls.model_fields[col].default

        df = df[cols].copy()
        
        type_mapping = {
            'ticker': str,
            'trade_date': 'datetime64[ns]', 
            'open': np.float32,
            'high': np.float32,
            'low': np.float32,
            'close': np.float32,
            'volume': np.uint64,
            'open_interest': np.uint64
        }
        
        for col, dtype in type_mapping.items():
            if col == 'trade_date':
                df[col] = pd.to_datetime(df[col]).dt.date
            else:
                df[col] = df[col].astype(dtype)

        return df