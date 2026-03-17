from src.model import BaseClickHouseModel
from pydantic import Field
from datetime import date
from typing import Optional
from datetime import datetime
import pandas as pd


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

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, cik: str) -> pd.DataFrame:
        """
        Industrial-grade formatter for financial data.
        1. Injects CIK for all rows.
        2. Aligns and validates all columns defined in UsStockFundamentalsModel.
        3. Ensures data types (Float, Date) are correct for ClickHouse.
        """
        if df.empty:
            return pd.DataFrame()

        # Start with a copy to avoid side-effects
        processed_df = df.copy()
        
        # Injects metadata
        processed_df['cik'] = cik
        processed_df['update_time'] = datetime.now()
        
        # Standardize date conversion
        if 'period_end_dt' in processed_df.columns:
            processed_df['period_end'] = processed_df['period_end_dt'].dt.date
        elif 'period_end' in processed_df.columns:
            processed_df['period_end'] = pd.to_datetime(processed_df['period_end']).dt.date

        # Force align with model's expected columns
        cols = cls.get_columns()
        for col in cols:
            if col not in processed_df.columns:
                processed_df[col] = cls.model_fields[col].default
            
            # Specialized casting for financial metrics
            if col in ['eps', 'revenue_growth_yoy', 'net_income_growth_yoy', 'roe', 'free_cash_flow', 'debt_to_equity', 'current_ratio']:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0.0).astype(float)

        return processed_df[cols].copy()
