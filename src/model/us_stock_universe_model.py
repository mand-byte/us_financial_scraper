from src.model import BaseClickHouseModel
from pydantic import Field
from datetime import date
from typing import Optional
from datetime import datetime

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