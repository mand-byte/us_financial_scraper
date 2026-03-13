from pydantic import  Field
from src.model import BaseClickHouseModel
from datetime import datetime
from typing import Optional
# ==========================================
# 13. 个股新闻情绪表 (派生特征)
# ==========================================
class UsStockNewsSentimentModel(BaseClickHouseModel):
    news_id: str = Field(...)
    composite_figi: str = Field(...)
    publish_timestamp: datetime = Field(...)
    llm_name: str = Field(...)
    sentiment_score: float = Field(default=0.0)
    event_category: str = Field(default="")
    update_time: Optional[datetime] = Field(default=None)