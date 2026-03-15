# -*- coding: utf-8 -*-
# 负责 表1,2,6,7,11 (行情与宏观)

from .clickhouse_manager import get_db_manager
import pandas as pd
from src.utils.logger import app_logger
import os
from datetime import datetime
import pytz 
class MarketDataRepo:
    def __init__(self):
        self.db = get_db_manager()  # 初始化客户端

    def insert_us_stock_universe(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_universe', df)
        except Exception as e:
            app_logger.error(f"插入股票列表数据失败: {e}")
            raise e

    def get_active_tickers(self) -> pd.DataFrame:
        query = "SELECT * FROM us_stock_universe WHERE active = 1"
        try:
            res = self.db.client.query_df(query)
            return res
        except Exception as e:
            app_logger.error(f"查询活跃股票列表失败: {e}")
            return pd.DataFrame()
    def get_delisted_tickers(self) -> pd.DataFrame:
        query = "SELECT * FROM us_stock_universe WHERE WHERE active = 1"
        try:
            res =self.db.client.query_df(query)
            return res
        except Exception as e:
            app_logger.error(f"查询退市股票列表失败: {e}")
            return pd.DataFrame()

    def get_universe_tickers(self) -> pd.DataFrame:
        query = "SELECT * FROM us_stock_universe"
        try:
            res = self.db.client.query_df(query)
            return res
        except Exception as e:
            app_logger.error(f"查询股票列表失败: {e}")
            return pd.DataFrame()
        
    def insert_stock_minutes_klines(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_minutes_klines', df)
        except Exception as e:
            app_logger.error(f"插入分钟K线数据失败: {e}")
            raise e
    
    def get_latest_stock_minutes_klines(self, ticker: str) -> datetime:
        query = f"SELECT max(timestamp) as last_ts FROM us_minutes_klines WHERE ticker = '{ticker}'"
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts).replace(tzinfo=pytz.UTC)
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询最新分钟K线时间戳失败: {e}")
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    
    

    def insert_benchmark_etf_klines(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_benchmark_etf_klines', df)
        except Exception as e:
            app_logger.error(f"插入基准ETF K线数据失败: {e}")
            raise e

    def get_latest_benchmark_etf_klines(self, ticker: str) -> datetime:
        query = f"SELECT max(timestamp) as last_ts FROM us_benchmark_etf_klines WHERE ticker = '{ticker}'"
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts).replace(tzinfo=pytz.UTC)
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询最新基准ETF K线时间戳失败: {e}")
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
        
    def insert_macro_daily_klines(self, df: pd.DataFrame):
        # 此时传进来的 df 已经是被 Model 洗干净的了
        self.db.client.insert_df('us_macro_daily_klines', df)

    def get_latest_trade_date_in_macro_daily_klines(self, symbols: list|dict|str) -> str:
        if isinstance(symbols, dict):
            target_symbols = list(symbols.values())
        elif isinstance(symbols, str):
            target_symbols = [symbols]
        elif isinstance(symbols, list):
            target_symbols = symbols
        else:
            raise ValueError("symbols 参数必须是 str, list 或 dict 类型")    
        
        if not target_symbols:
            app_logger.error(f"symbols 参数非法: {symbols}")
            return os.getenv("SCRAPING_START_DATE", "2014-01-01")

        symbols_str = "','".join(target_symbols)   
        
        # SQL 里 AS ts
        query = f"SELECT max(trade_date) as ts FROM us_macro_daily_klines WHERE ticker IN ('{symbols_str}')"
        
        try:
            res = self.db.client.query_df(query) # 假设你单例调用是这样
            
            # 🌟 修复: 提取的列名必须是 'ts'
            last_date = res.iloc[0]['ts']
            
            if pd.isna(last_date):
                return os.getenv("SCRAPING_START_DATE", "2014-01-01")  
                
            # 🌟 安全保障: 强制转为 datetime 再提取字符串，防止 ClickHouse 原生 date 类型报错
            return pd.to_datetime(last_date).strftime('%Y-%m-%d')
            
        except Exception as e:
            app_logger.error(f"查询最新交易日期失败: {e}")
            return os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def insert_marco_indicators(self, df: pd.DataFrame):
        self.db.client.insert_df('us_macro_indicators', df)
    
    def get_latest_macro_indicators(self, indicator_code: list|dict|str) -> datetime:
        if isinstance(indicator_code, dict):
            target_codes = list(indicator_code.values())
        elif isinstance(indicator_code, str):
            target_codes = [indicator_code]
        elif isinstance(indicator_code, list):
            target_codes = indicator_code
        else:
            raise ValueError("indicator_code 参数必须是 str, list 或 dict 类型")    
        
        if not target_codes:
            app_logger.error(f"indicator_code 参数非法: {indicator_code}")
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
        query = f"SELECT max(publish_timestamp) as last_ts FROM us_macro_indicators WHERE indicator_code IN ('{target_codes}')"
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts).replace(tzinfo=pytz.UTC)
           
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询最新宏观指标时间戳失败: {e}")
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)






