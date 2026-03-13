from pydantic import  Field
from src.model import BaseClickHouseModel
from datetime import datetime
from typing import Optional
# ==========================================
# 11. 宏观经济指标表 (注意: 没有 surprise_diff)
# ==========================================
class UsMacroIndicatorsModel(BaseClickHouseModel):
    publish_timestamp: datetime = Field(...)
    indicator_code: str = Field(...)
    actual_value: float = Field(default=0.0)
    expected_value: Optional[float] = Field(default=None)