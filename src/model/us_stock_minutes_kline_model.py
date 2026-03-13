from pydantic import  Field
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

  
    