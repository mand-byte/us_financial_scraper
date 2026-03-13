from src.model import BaseClickHouseModel
from pydantic import Field
from datetime import date
from typing import Optional
from datetime import datetime
# ==========================================
# 3. 纯财务基本面表
# ==========================================
class UsStockFundamentalsModel(BaseClickHouseModel):
    cik: str = Field(...)
    publish_timestamp: datetime = Field(...)
    period_end: date = Field(...)
    eps: float = Field(default=0.0)
    revenue_growth_yoy: float = Field(default=0.0)
    net_income_growth_yoy: float = Field(default=0.0)
    roe: float = Field(default=0.0)
    free_cash_flow: float = Field(default=0.0)
    debt_to_equity: float = Field(default=0.0)
    current_ratio: float = Field(default=0.0)
    update_time: Optional[datetime] = Field(default=None)