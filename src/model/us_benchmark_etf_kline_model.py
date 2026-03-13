from pydantic import  Field
from datetime import datetime
from src.model import BaseClickHouseModel


# ==========================================
# 6. 基准 ETF K线表
# ==========================================
class BenchmarkEtfKlineModel(BaseClickHouseModel):
    ticker: str = Field(...)
    timestamp: datetime = Field(...)
    open: float = Field(default=0.0)
    high: float = Field(default=0.0)
    low: float = Field(default=0.0)
    close: float = Field(default=0.0)
    vwap: float = Field(default=0.0)
    trades_count: int = Field(default=0)
    volume: int = Field(default=0)