# -*- coding: utf-8 -*-
# 负责 表3,4,5,8,9 ,10(基本面,资金,个股新闻)
from .clickhouse_manager import get_db_manager
from src.utils.logger import app_logger
import pandas as pd
from datetime import datetime, date
import os
from zoneinfo import ZoneInfo
from src.model import UsStockNewsRawModel


class FundamentalRepo:
    SCRAPING_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def __init__(self):
        self.db = get_db_manager()



    def insert_stock_dividends(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df("us_stock_dividends", df)
        except Exception as e:
            app_logger.error(f"插入 us_stock_dividends 失败: {e}")
            raise e

    def get_latest_stock_dividends_date(self, composite_figi: str) -> date:
        from src.model.us_stock_dividends_model import UsStockDividendsModel

        query = UsStockDividendsModel.QUERY_LATEST_EX_DATE_BY_FIGI_SQL.format(
            composite_figi=composite_figi
        )
        try:
            res = self.db.client.query_df(query)
            last_date = res.iloc[0]["last_date"]
            if pd.notna(last_date):
                return pd.to_datetime(last_date).date()
            return datetime.strptime(self.SCRAPING_START_DATE, "%Y-%m-%d").date()
        except Exception:
            return datetime.strptime(self.SCRAPING_START_DATE, "%Y-%m-%d").date()

    def insert_stock_splits(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df("us_stock_splits", df)
        except Exception as e:
            app_logger.error(f"插入 us_stock_splits 失败: {e}")
            raise e

    def get_latest_stock_splits_date(self, composite_figi: str) -> date:
        from src.model.us_stock_splits_model import UsStockSplitsModel

        query = UsStockSplitsModel.QUERY_LATEST_EX_DATE_BY_FIGI_SQL.format(
            composite_figi=composite_figi
        )
        try:
            res = self.db.client.query_df(query)
            last_date = res.iloc[0]["last_date"]
            if pd.notna(last_date):
                return pd.to_datetime(last_date).date()
            time = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").date()
        except Exception as e:
            app_logger.error(f"查询{composite_figi}最新股票拆分时间失败: {e}")
            time = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").date()

    def get_global_latest_stock_dividends_date(self) -> date:
        from src.model.us_stock_dividends_model import UsStockDividendsModel

        query = UsStockDividendsModel.QUERY_GLOBAL_LATEST_EX_DATE_SQL
        try:
            res = self.db.client.query_df(query)
            last_date = res.iloc[0]["last_date"]
            if pd.notna(last_date):
                return pd.to_datetime(last_date).date()
            time = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").date()
        except Exception as e:
            app_logger.error(f"全局派息时间查询失败: {e}")
            time = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").date()

    def get_global_latest_stock_splits_date(self) -> date:
        from src.model.us_stock_splits_model import UsStockSplitsModel

        query = UsStockSplitsModel.QUERY_GLOBAL_LATEST_EXECUTION_DATE_SQL
        try:
            res = self.db.client.query_df(query)
            last_date = res.iloc[0]["last_date"]
            if pd.notna(last_date):
                return pd.to_datetime(last_date).date()
            time = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").date()
        except Exception as e:
            app_logger.error(f"全局股票拆分时间查询失败: {e}")
            time = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            return datetime.strptime(time, "%Y-%m-%d").date()

    def insert_stock_10k_sections_raw(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df("us_stock_10k_sections_raw", df)
        except Exception as e:
            app_logger.error(
                f"{df.iloc[0]['cik']} 插入 us_stock_10k_sections_raw 失败: {e}"
            )
            raise e

    def get_latest_stock_earnings_raw_timestamp(self, cik: str) -> datetime:
        from src.model.us_stock_earnings_raw_model import UsStockEarningsRawModel

        query = UsStockEarningsRawModel.QUERY_LATEST_PUBLISH_TS_BY_CIK_SQL.format(
            cik=cik
        )
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]["last_ts"]
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            app_logger.warning(
                f"{cik} 在 us_stock_earnings_raw 中没有数据，返回默认开始时间"
            )
            return datetime.strptime(self.SCRAPING_START_DATE, "%Y-%m-%d").replace(
                tzinfo=ZoneInfo("UTC")
            )
        except Exception as e:
            app_logger.error(f"查询{cik}最新财报原文时间戳失败: {e}")
            return datetime.strptime(self.SCRAPING_START_DATE, "%Y-%m-%d").replace(
                tzinfo=ZoneInfo("UTC")
            )

    def get_global_latest_news_timestamp(self) -> datetime:
        """获取全市场新闻原文表中的最新时间戳"""
        try:
            res = self.db.client.query_df(
                UsStockNewsRawModel.MAX_PUBLISHED_UTC_QUERY_SQL
            )
            last_ts = res.iloc[0]["last_ts"]
            if pd.notna(last_ts):
                return pd.to_datetime(last_ts)
            return datetime.strptime(self.SCRAPING_START_DATE, "%Y-%m-%d").replace(
                tzinfo=ZoneInfo("UTC")
            )
        except Exception as e:
            app_logger.error(f"查询全市场新闻最新时间戳失败: {e}")
            return datetime.strptime(self.SCRAPING_START_DATE, "%Y-%m-%d").replace(
                tzinfo=ZoneInfo("UTC")
            )

    def insert_stock_news_raw(self, df: pd.DataFrame):
        try:
            self.db.client.insert_df("us_stock_news_raw", df)
        except Exception as e:
            app_logger.error(f"{df.iloc[0]['cik']} 插入 us_stock_news_raw 失败: {e}")
        except Exception as e:
            app_logger.error(f"插入 us_stock_daily_float 失败: {e}")
            raise e
