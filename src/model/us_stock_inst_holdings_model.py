
from src.model import BaseClickHouseModel
from pydantic import Field
from datetime import date
from typing import Optional
from datetime import datetime
# ==========================================
# 4. 机构持仓变动表 (13F)
# ==========================================
class UsStockInstHoldingsModel(BaseClickHouseModel):
    cik: str = Field(...)
    publish_timestamp: datetime = Field(...)
    period_end: date = Field(...)
    inst_hold_pct: float = Field(default=0.0)
    update_time: Optional[datetime] = Field(default=None)