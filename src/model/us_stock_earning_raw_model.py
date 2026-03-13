from pydantic import  Field
from src.model import BaseClickHouseModel
from datetime import datetime,date
from typing import Optional

# ==========================================
# 9. 财报原始文本表
# ==========================================
class UsStockEarningsRawModel(BaseClickHouseModel):
    cik: str = Field(...)
    publish_timestamp: datetime = Field(...)
    period_end: date = Field(...)
    mda_txt: str = Field(default="")
    risk_qa_txt: str = Field(default="")
    next_quarter_revenue_low: float = Field(default=0.0)
    next_quarter_revenue_high: float = Field(default=0.0)
    update_time: Optional[datetime] = Field(default=None)