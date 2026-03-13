from pydantic import  Field
from src.model import BaseClickHouseModel
from datetime import datetime
from typing import Optional
# ==========================================
# 10. 个股新闻情绪表
# ==========================================
class UsStockNewsRawModel(BaseClickHouseModel):
    news_id: str = Field(...)
    composite_figi: str = Field(...)
    publish_timestamp: datetime = Field(...)
    title: str = Field(default="")
    description: str = Field(default="")
    update_time: Optional[datetime] = Field(default=None)