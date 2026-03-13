from pydantic import  Field
from datetime import date
from src.model import BaseClickHouseModel
from typing import List, Optional
from datetime import datetime

# ==========================================
# 14. 财报情感评分表
# ==========================================
class UsStockEarningsSentimentModel(BaseClickHouseModel):
    cik: str = Field(...)
    publish_timestamp: datetime = Field(...)
    period_end: date = Field(...)
    llm_name: str = Field(...)
    
    guidance_vs_consensus: int = Field(default=99)
    management_tone: int = Field(default=0)
    main_risks: List[str] = Field(default_factory=list)
    growth_drivers: List[str] = Field(default_factory=list)
    key_highlights: str = Field(default="")
    
    overall_score: float = Field(default=0.0)
    revenue_sentiment: int = Field(default=0)
    eps_sentiment: int = Field(default=0)
    red_flags: List[str] = Field(default_factory=list)
    update_time: Optional[datetime] = Field(default=None)