# -*- coding: utf-8 -*-
# 负责 表3,4,5,8,9 ,10(基本面,资金,个股新闻)
from .clickhouse_manager import get_db_manager
from src.utils.logger import app_logger
import pandas as pd
from datetime import datetime
import os
import pytz
class FundamentalRepo:
    def __init__(self):
        self.db = get_db_manager()

    def insert_stock_fundamentals(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_fundamentals', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入 us_stock_fundamentals 失败: {e}")
            raise e

    def get_latest_fundamental_timestamp(self, cik: str) -> datetime:
        query = f"SELECT max(publish_timestamp) as last_ts FROM us_stock_fundamentals WHERE cik = '{cik}'"
        
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            app_logger.warning(f"{cik} 在 us_stock_fundamentals 中没有数据，返回默认开始时间")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询{cik}最新基本面时间戳失败: {e}")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

    def insert_stock_inst_holdings(self,df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_inst_holdings', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入 us_stock_inst_holdings 失败: {e}")
            raise e
    def get_latest_inst_holdings_timestamp(self, cik: str) -> datetime:
        query = f"SELECT max(publish_timestamp) as last_ts FROM us_stock_inst_holdings WHERE cik = '{cik}'"
        
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            app_logger.warning(f"{cik} 在 us_stock_inst_holdings 中没有数据，返回默认开始时间")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询{cik}最新机构持仓时间戳失败: {e}")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

    def insert_stock_insider_holdings(self,df: pd.DataFrame): 
        try:
            self.db.client.insert_df('us_stock_insider_holdings', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入 us_stock_insider_holdings 失败: {e}")
            raise e
    def get_latest_insider_holdings_timestamp(self, cik: str) -> datetime:
        query = f"SELECT max(publish_timestamp) as last_ts FROM us_stock_insider_holdings WHERE cik = '{cik}'"
        
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            app_logger.warning(f"{cik} 在 us_stock_insider_holdings 中没有数据，返回默认开始时间")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询{cik}最新内部人士持仓时间戳失败: {e}")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    
    def insert_stock_actions(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_actions', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入 us_stock_actions 失败: {e}")
            raise e 
    def get_latest_stock_actions_timestamp(self, cik: str) -> datetime:
        query = f"SELECT max(publish_timestamp) as last_ts FROM us_stock_actions WHERE cik = '{cik}'"
        
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            app_logger.warning(f"{cik} 在 us_stock_actions 中没有数据，返回默认开始时间")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询{cik}最新公司行动时间戳失败: {e}")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    
    def insert_stock_earnings_raw(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_earnings_raw', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入 us_stock_earnings_raw 失败: {e}")
            raise e

    def get_latest_stock_earnings_raw_timestamp(self, cik: str) -> datetime:
        query = f"SELECT max(publish_timestamp) as last_ts FROM us_stock_earnings_raw WHERE cik = '{cik}'"
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            app_logger.warning(f"{cik} 在 us_stock_earnings_raw 中没有数据，返回默认开始时间")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询{cik}最新财报原文时间戳失败: {e}")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

    def get_global_latest_news_timestamp(self) -> datetime:
        """获取全市场新闻原文表中的最新时间戳"""
        query = "SELECT max(published_utc) as last_ts FROM us_stock_news_raw"
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            time_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
        except Exception as e:
            app_logger.error(f"查询全市场新闻最新时间戳失败: {e}")
            time_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

    def insert_stock_news_raw(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df('us_stock_news_raw', df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入 us_stock_news_raw 失败: {e}")
            raise e

    def get_latest_stock_news_raw_timestamp(self, cik: str) -> datetime:
        query = f"SELECT max(publish_timestamp) as last_ts FROM us_stock_news_raw WHERE cik = '{cik}'"
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]['last_ts']
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            app_logger.warning(f"{cik} 在 us_stock_news_raw 中没有数据，返回默认开始时间")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC) 
        except Exception as e:
            app_logger.error(f"查询{cik}最新个股新闻原文时间戳失败: {e}")
            time=os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").replace(tzinfo=pytz.UTC)