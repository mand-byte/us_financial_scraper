from pydantic import  Field
from datetime import datetime
from typing import Optional
from src.model import BaseClickHouseModel
from datetime import datetime
# ==========================================
# 5. 内部人士持仓变动表 (Form 4)
# ==========================================
class UsStockInsiderHoldingsModel(BaseClickHouseModel):
    cik: str = Field(...)
    publish_timestamp: datetime = Field(...)
    insider_hold_pct: float = Field(default=0.0)
    update_time: Optional[datetime] = Field(default=None)