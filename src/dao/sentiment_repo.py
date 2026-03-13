# -*- coding: utf-8 -*-
# 负责 表12 ,13,14(情绪与替代数据)
import os
from .clickhouse_manager import get_db_manager
from src.utils.logger import app_logger
import pandas as pd
import pytz
from datetime import datetime

class SentimentRepo:
    def __init__(self):
        self.db = get_db_manager()  # 初始化客户端
   

    def insert_gdelt_macro_sentiment(self, df: pd.DataFrame):
        self.db.client.insert_df('gdelt_macro_sentiment', df)

    def get_latest_gdelt_macro_sentiment(self) -> datetime:
        query = "SELECT max(publish_timestamp) as last_ts FROM gdelt_macro_sentiment"
        
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts).replace(tzinfo=pytz.UTC)
           
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
          
        except Exception as e:
            app_logger.error(f"查询最新 GDELT 宏观情绪时间戳失败: {e}")
            start_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    

    def insert_us_stock_news_sentiment(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_news_sentiment', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['ticker']} 插入新闻情绪数据失败: {e}")
            raise e

    def get_stock_news_sentiment(self, figi: str,start_ts: datetime, end_ts: datetime,llm_name: str) -> pd.DataFrame:
        query = f"""
            SELECT * FROM us_stock_news_sentiment FINAL
            WHERE composite_figi = '{figi}' 
            AND publish_timestamp >= {start_ts.strftime("%Y-%m-%d %H:%M:%S")}
            AND publish_timestamp <= {end_ts.strftime("%Y-%m-%d %H:%M:%S")}
            AND llm_name = '{llm_name}'
        """
        try:
            res = self.db.client.query_df(query)
            return res
        except Exception as e:
            app_logger.error(f"查询 {figi} 新闻情绪数据失败: {e}")
            return pd.DataFrame()

    def insert_stock_earnings_sentiment(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_earnings_sentiment', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入财报情绪数据失败: {e}")
            raise e

    def get_stock_earnings_sentiment(self, cik: str,start_ts: datetime, end_ts: datetime,llm_name: str) -> pd.DataFrame:
        query = f"""
            SELECT * FROM us_stock_earnings_sentiment FINAL
            WHERE cik = '{cik}' 
            AND publish_timestamp >= {start_ts.strftime("%Y-%m-%d %H:%M:%S")}
            AND publish_timestamp <= {end_ts.strftime("%Y-%m-%d %H:%M:%S")}
            AND llm_name = '{llm_name}'
        """
        try:
            res = self.db.client.query_df(query)
            return res
        except Exception as e:
            app_logger.error(f"查询 {cik} 财报情绪数据失败: {e}")
            return pd.DataFrame()