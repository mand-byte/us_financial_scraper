# -*- coding: utf-8 -*-
# 负责基本面、风险因子、新闻原文等数据的入库与查询。
from datetime import date, datetime
from typing import Optional

import pandas as pd

from src.model import UsStockNewsRawModel
from src.utils.logger import app_logger


class FundamentalRepo:
    @property
    def db(self):
        from src.dao.clickhouse_manager import get_db_manager

        return get_db_manager()

    def __init__(self) -> None:
        pass

    @staticmethod
    def _extract_valid_date(df: pd.DataFrame, column: str = "last_date") -> Optional[date]:
        if df.empty or column not in df.columns:
            return None
        value = df.iloc[0][column]
        if pd.isna(value):
            return None
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt) or dt.year <= 1970:
            return None
        return dt.date()

    @staticmethod
    def _extract_valid_datetime(
        df: pd.DataFrame, column: str = "last_ts"
    ) -> Optional[datetime]:
        if df.empty or column not in df.columns:
            return None
        value = df.iloc[0][column]
        if pd.isna(value):
            return None
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt) or dt.year <= 1970:
            return None
        return dt.to_pydatetime()

    def insert_stock_dividends(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_stock_dividends", df)
        except Exception as e:
            app_logger.error(f"插入 us_stock_dividends 失败: {e}")
            raise

    def get_latest_stock_dividends_date(self, composite_figi: str) -> Optional[date]:
        from src.model.us_stock_dividends_model import UsStockDividendsModel

        query = UsStockDividendsModel.build_query_latest_ex_date_by_figi_sql(
            composite_figi=composite_figi
        )
        try:
            return self._extract_valid_date(self.db.client.query_df(query))
        except Exception as e:
            app_logger.error(f"查询 {composite_figi} 最新派息时间失败: {e}")
            return None

    def insert_stock_splits(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_stock_splits", df)
        except Exception as e:
            app_logger.error(f"插入 us_stock_splits 失败: {e}")
            raise

    def get_latest_stock_splits_date(self, composite_figi: str) -> Optional[date]:
        from src.model.us_stock_splits_model import UsStockSplitsModel

        query = UsStockSplitsModel.build_query_latest_execution_date_by_figi_sql(
            composite_figi=composite_figi
        )
        try:
            return self._extract_valid_date(self.db.client.query_df(query))
        except Exception as e:
            app_logger.error(f"查询 {composite_figi} 最新股票拆分时间失败: {e}")
            return None

    def get_global_latest_stock_dividends_date(self) -> Optional[date]:
        from src.model.us_stock_dividends_model import UsStockDividendsModel

        try:
            return self._extract_valid_date(
                self.db.client.query_df(UsStockDividendsModel.QUERY_GLOBAL_LATEST_EX_DATE_SQL)
            )
        except Exception as e:
            app_logger.error(f"全局派息时间查询失败: {e}")
            return None

    def get_global_latest_stock_splits_date(self) -> Optional[date]:
        from src.model.us_stock_splits_model import UsStockSplitsModel

        try:
            return self._extract_valid_date(
                self.db.client.query_df(
                    UsStockSplitsModel.QUERY_GLOBAL_LATEST_EXECUTION_DATE_SQL
                )
            )
        except Exception as e:
            app_logger.error(f"全局股票拆分时间查询失败: {e}")
            return None


    def insert_stock_risk_factors(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_stock_risk_factors", df)
        except Exception as e:
            app_logger.error(f"插入 us_stock_risk_factors 失败: {e}")
            raise

    def insert_stock_risk_taxonomy(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_stock_risk_taxonomy", df)
        except Exception as e:
            app_logger.error(f"插入 us_stock_risk_taxonomy 失败: {e}")
            raise

    def get_global_latest_risk_factors_date(self) -> Optional[date]:
        from src.model.us_stock_risk_factors_model import UsStockRiskFactorsModel

        try:
            return self._extract_valid_date(
                self.db.client.query_df(
                    UsStockRiskFactorsModel.QUERY_GLOBAL_LATEST_FILING_DATE_SQL
                )
            )
        except Exception as e:
            app_logger.error(f"全局风险因素时间查询失败: {e}")
            return None

    def get_latest_stock_earnings_raw_timestamp(self, cik: str) -> Optional[datetime]:
        # 当前模型未定义，保留接口但不让运行时抛 ImportError。
        app_logger.warning(
            f"未找到 Earnings Raw Model，跳过 {cik} 的最新财报原文时间查询。"
        )
        return None

    def get_global_latest_news_timestamp(self) -> Optional[datetime]:
        """获取全市场新闻原文表中的最新时间戳。"""
        try:
            return self._extract_valid_datetime(
                self.db.client.query_df(UsStockNewsRawModel.MAX_PUBLISHED_UTC_QUERY_SQL)
            )
        except Exception as e:
            app_logger.error(f"查询全市场新闻最新时间戳失败: {e}")
            return None

    def insert_stock_news_raw(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_stock_news_raw", df)
        except Exception as e:
            cik = ""
            if not df.empty and "cik" in df.columns:
                cik = str(df.iloc[0]["cik"])
            app_logger.error(f"{cik} 插入 us_stock_news_raw 失败: {e}")
            raise
