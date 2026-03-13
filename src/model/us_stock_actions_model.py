from pydantic import  Field
from datetime import date
from src.model import BaseClickHouseModel

# ==========================================
# 8. 个股公司行动表
# ==========================================
class UsStockActionsModel(BaseClickHouseModel):
    composite_figi: str = Field(...)
    ex_date: date = Field(...)
    action_type: int = Field(...)  # Enum8: 1=split, 2=dividend
    split_ratio: float = Field(default=0.0)
    cash_amount: float = Field(default=0.0)