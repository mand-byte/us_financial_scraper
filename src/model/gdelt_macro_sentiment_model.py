from pydantic import  Field
from src.model import BaseClickHouseModel
from datetime import datetime

# ==========================================
# 12. GDELT 地缘风险表
# ==========================================
class GdeltMacroSentimentModel(BaseClickHouseModel):
    publish_timestamp: datetime = Field(...)
    count_16: int = Field(default=0)
    tone_16: float = Field(default=0.0)
    impact_16: float = Field(default=0.0)
    count_17: int = Field(default=0)
    tone_17: float = Field(default=0.0)
    impact_17: float = Field(default=0.0)
    count_18: int = Field(default=0)
    tone_18: float = Field(default=0.0)
    impact_18: float = Field(default=0.0)
    count_19: int = Field(default=0)
    tone_19: float = Field(default=0.0)
    impact_19: float = Field(default=0.0)
    count_20: int = Field(default=0)
    tone_20: float = Field(default=0.0)
    impact_20: float = Field(default=0.0)