# -*- coding: utf-8 -*-
from src.model import BaseClickHouseModel
from pydantic import Field
from datetime import date, datetime
from typing import Optional
import pandas as pd

class UsStockDividendsModel(BaseClickHouseModel):
    id: str = Field(...)
    composite_figi: str = Field(...)
    ticker: str = Field(...)
    ex_date: date = Field(...)
    declaration_date: Optional[date] = Field(default=None)
    pay_date: Optional[date] = Field(default=None)
    cash_amount: float = Field(default=0.0)
    update_time: Optional[datetime] = Field(default=None)

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, composite_figi: str) -> pd.DataFrame:
        if df.empty: return pd.DataFrame()
        
        df = df.copy()
        df['composite_figi'] = composite_figi
        df['update_time'] = datetime.now()
        
        # Mapping from Massive API fields to DB fields
        # API: ex_dividend_date -> DB: ex_date
        if 'ex_dividend_date' in df.columns:
            df['ex_date'] = pd.to_datetime(df['ex_dividend_date']).dt.date
        
        # declaration_date, pay_date conversion
        for col in ['declaration_date', 'pay_date']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        
        cols = cls.get_columns()
        for col in cols:
            if col not in df.columns:
                df[col] = cls.model_fields[col].default
        
        return df[cols].copy()

class UsStockSplitsModel(BaseClickHouseModel):
    id: str = Field(...)
    composite_figi: str = Field(...)
    ticker: str = Field(...)
    ex_date: date = Field(...)
    split_from: float = Field(...)
    split_to: float = Field(...)
    update_time: Optional[datetime] = Field(default=None)

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, composite_figi: str) -> pd.DataFrame:
        if df.empty: return pd.DataFrame()
        
        df = df.copy()
        df['composite_figi'] = composite_figi
        df['update_time'] = datetime.now()
        
        # API: execution_date -> DB: ex_date
        if 'execution_date' in df.columns:
            df['ex_date'] = pd.to_datetime(df['execution_date']).dt.date
            
        cols = cls.get_columns()
        for col in cols:
            if col not in df.columns:
                df[col] = cls.model_fields[col].default
                
        return df[cols].copy()
