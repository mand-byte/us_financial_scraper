import pandas as pd
import numpy as np
from pydantic import BaseModel
from typing import List

class BaseClickHouseModel(BaseModel):
    @classmethod
    def get_columns(cls) -> List[str]:
        return list(cls.model_fields.keys())

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        """通用清洗函数：强制对齐列名，丢弃多余列，补齐缺失列"""
        if df.empty:
            return df
            
        cols = cls.get_columns()
        for col in cols:
            if col not in df.columns:
                df[col] = cls.model_fields[col].default
        
        # 只保留模型定义的列
        return df[cols].copy()