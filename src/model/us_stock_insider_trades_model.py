# -*- coding: utf-8 -*-
from src.model import BaseClickHouseModel
from pydantic import Field
from datetime import date, datetime
from typing import Optional
import pandas as pd
import numpy as np

class UsStockInsiderTradesModel(BaseClickHouseModel):
    composite_figi: str = Field(...)
    ticker: str = Field(...)
    filing_timestamp: datetime = Field(...)
    trade_date: date = Field(...)
    insider_name: str = Field(...)
    insider_title: str = Field(...)
    trade_type: str = Field(...)
    price: float = Field(default=0.0)
    qty: int = Field(default=0)
    trade_value: float = Field(default=0.0)
    update_time: Optional[datetime] = Field(default=None)

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame, composite_figi: str = None) -> pd.DataFrame:
        """
        Specialized cleaner for OpenInsider data.
        Handles:
        1. Stripping '$', ',', '+' from currency and quantity strings.
        2. Date parsing for Filing Date and Trade Date.
        3. Normalizing column names.
        """
        if df.empty: return pd.DataFrame()
        
        df = df.copy()
        
        # Clean non-breaking spaces and redundant columns if any
        df.columns = [c.replace('\xa0', ' ') if isinstance(c, str) else c for c in df.columns]
        
        # Mapping OpenInsider -> ClickHouse
        col_map = {
            'Filing Date': 'filing_timestamp',
            'Trade Date': 'trade_date',
            'Ticker': 'ticker',
            'Insider Name': 'insider_name',
            'Title': 'insider_title',
            'Trade Type': 'trade_type',
            'Price': 'price',
            'Qty': 'qty',
            'Value': 'trade_value'
        }
        df = df.rename(columns=col_map)
        
        # Data Cleaning: Strings to Numbers
        def clean_numeric(val):
            if pd.isna(val) or not isinstance(val, (str, int, float)): return 0.0
            if isinstance(val, (int, float)): return float(val)
            # Remove $, commas, +, and handle parentheses for negative numbers
            s = val.replace('$', '').replace(',', '').replace('+', '').replace(' ', '').strip()
            if s.startswith('(') and s.endswith(')'):
                s = '-' + s[1:-1]
            try:
                return float(s)
            except ValueError:
                return 0.0

        for col in ['price', 'qty', 'trade_value']:
            if col in df.columns:
                df[col] = df[col].apply(clean_numeric)

        # Date Parsing
        df['filing_timestamp'] = pd.to_datetime(df['filing_timestamp'])
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        
        if composite_figi:
            df['composite_figi'] = composite_figi
            
        df['update_time'] = datetime.now()
        
        # Final Alignment
        cols = cls.get_columns()
        for col in cols:
            if col not in df.columns:
                df[col] = cls.model_fields[col].default
        
        return df[cols].copy()
