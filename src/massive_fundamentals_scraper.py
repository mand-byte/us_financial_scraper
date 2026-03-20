# -*- coding: utf-8 -*-
"""
================================================================================
Massive 财务基本面搜刮器 (MassiveFundamentalScraper) - 重构版
================================================================================

[核心需求 - 已实现]
1. 数据源:
   - 全部 Fundamental 端点 (Balance Sheets, Cash Flow, Income Statements)
   - 全部 Factors 端点 (Short Interest, Short Volume, Float, Ratios)
2. 获取策略:
   - 全局扫描 (不按 ticker 逐个查询)，从底层数据库记录的最大时间段增量往前拉。
   - 完全利用 Point-in-Time mapping 对齐到 composite_figi 实体。
================================================================================
"""

import pandas as pd
from datetime import datetime
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from zoneinfo import ZoneInfo
import os

from src.api.massive_api import MassiveApi
from src.dao.fundamental_repo import FundamentalRepo
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger

# Import all independent models
from src.model.us_stock_balance_sheets_model import UsStockBalanceSheetsModel
from src.model.us_stock_cash_flow_statements_model import UsStockCashFlowStatementsModel
from src.model.us_stock_income_statements_model import UsStockIncomeStatementsModel

try:
    from src.model.us_stock_daily_ratios_factors_model import StockDailyRatiosFactorsModel
except ModuleNotFoundError:
    StockDailyRatiosFactorsModel = None

try:
    from src.model.us_stock_daily_short_interest_factors_model import StockDailyShortInterestFactorsModel
except ModuleNotFoundError:
    StockDailyShortInterestFactorsModel = None

try:
    from src.model.us_stock_daily_short_volume_factors_model import StockDailyShortVolumeFactorsModel
except ModuleNotFoundError:
    StockDailyShortVolumeFactorsModel = None

try:
    from src.model.us_stock_daily_float_factors_model import StockDailyFloatFactorsModel
except ModuleNotFoundError:
    StockDailyFloatFactorsModel = None

class MassiveFundamentalsScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: Optional[BlockingScheduler] = None):
        self.api = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler

    def start(self):
        if self.scheduler:
            self.scheduler.add_job(
                self.run,
                "cron",
                hour=3,
                minute=0,
                timezone=self.NYC,
                id="daily_fundamental_sync",
                next_run_time=datetime.now(self.NYC),
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )
            app_logger.info("✅ Massive Fundamental Scraper started.")

    def stop(self):
        app_logger.info("🛑 Massive Fundamental Scraper stopping...")

    def _fetch_and_store(
        self,
        api_func,
        model_cls,
        date_col: str,
        api_date_param: Optional[str] = None,
        query_history: bool = True
    ):
        """
        Generic helper:
        1. Query latest time from ClickHouse (if query_history=True)
        2. Fetch Massive API globally (no ticker filtering)
        3. Match historical composite_figi using merge_asof backwards
        4. Ingest to DB via ReplacingMergeTree
        """
        date_str = None
        if query_history:
            query = f"SELECT max({date_col}) as last_ts FROM {model_cls.table_name}"
            try:
                res = self.fundamental_repo.db.client.query_df(query)
                last_ts = res.iloc[0]["last_ts"]
                if pd.notna(last_ts):
                    date_str = pd.to_datetime(last_ts).strftime("%Y-%m-%d")
            except Exception as e:
                app_logger.warning(f"无法查询 {model_cls.table_name} 时间: {e}, 采用默认.")
            
            if not date_str:
                date_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")
                
        app_logger.info(f"🚀 获取 {model_cls.table_name} (起: {date_str or 'LATEST'})...")
        
        kwargs = {"limit": 50000}
        if date_str and api_date_param:
            kwargs[api_date_param] = date_str
            
        df_raw = api_func(**kwargs)
        
        if df_raw is None or df_raw.empty:
            app_logger.info(f"{model_cls.table_name} 无新增数据。")
            return
            
        if "ticker" not in df_raw.columns:
            app_logger.info(f"{model_cls.table_name} API 未返回 ticker 字段。")
            return
            
        df_raw = df_raw.dropna(subset=["ticker"])
        if df_raw.empty:
            return
            
        unique_tickers = df_raw["ticker"].unique().tolist()
        mappings_df = self.market_repo.get_figi_mapping_history_by_tickers(unique_tickers)
        
        if not mappings_df.empty:
            mappings_df["date_map"] = pd.to_datetime(mappings_df["date"]).dt.tz_localize(None)
            mappings_df = mappings_df.sort_values("date_map")
            
            if date_col in df_raw.columns:
                df_raw[f"{date_col}_dt"] = pd.to_datetime(df_raw[date_col], errors='coerce').dt.tz_localize(None)
                df_raw = df_raw.dropna(subset=[f"{date_col}_dt"])
                df_raw = df_raw.sort_values(f"{date_col}_dt")
                
                df_raw = pd.merge_asof(
                    df_raw,
                    mappings_df[["ticker", "date_map", "composite_figi"]],
                    left_on=f"{date_col}_dt",
                    right_on="date_map",
                    by="ticker",
                    direction="backward"
                )
                
                missing_mask = df_raw["composite_figi"].isna()
                if missing_mask.any():
                    fallback_df = pd.merge_asof(
                        df_raw[missing_mask].drop(columns=["composite_figi", "date_map"]),
                        mappings_df[["ticker", "date_map", "composite_figi"]],
                        left_on=f"{date_col}_dt",
                        right_on="date_map",
                        by="ticker",
                        direction="forward"
                    )
                    df_raw.loc[missing_mask, "composite_figi"] = fallback_df["composite_figi"].values
                
        if "composite_figi" in df_raw.columns:
            df_raw = df_raw.dropna(subset=["composite_figi"])
            
        if df_raw.empty:
            app_logger.info(f"{model_cls.table_name} 匹配历史 FIGI 后为空。")
            return
            
        clean_df = model_cls.format_dataframe(df_raw)
        if not clean_df.empty:
            try:
                self.fundamental_repo.db.client.insert_df(model_cls.table_name, clean_df)
                app_logger.info(f"✅ {model_cls.table_name} 成功插入 {len(clean_df)} 条记录。")
            except Exception as e:
                app_logger.error(f"❌ {model_cls.table_name} 插入失败: {e}")

    def run(self):
        # 1. 有时间参数的增量拉取
        self._fetch_and_store(
            api_func=self.api.get_balance_sheets,
            model_cls=UsStockBalanceSheetsModel,
            date_col="period_end",
            api_date_param="date",
            query_history=True
        )
        self._fetch_and_store(
            api_func=self.api.get_cashflow_statements,
            model_cls=UsStockCashFlowStatementsModel,
            date_col="period_end",
            api_date_param="date",
            query_history=True
        )
        self._fetch_and_store(
            api_func=self.api.get_income_statements,
            model_cls=UsStockIncomeStatementsModel,
            date_col="period_end",
            api_date_param="date",
            query_history=True
        )
        factor_jobs = [
            (
                StockDailyShortInterestFactorsModel,
                self.api.get_short_interest,
                "settlement_date",
                "date",
                True,
            ),
            (
                StockDailyShortVolumeFactorsModel,
                self.api.get_short_volume,
                "date",
                "date",
                True,
            ),
            (
                StockDailyFloatFactorsModel,
                self.api.get_float,
                "effective_date",
                None,
                False,
            ),
            (
                StockDailyRatiosFactorsModel,
                self.api.get_ratios,
                "date",
                None,
                False,
            ),
        ]

        for model_cls, api_func, date_col, api_date_param, query_history in factor_jobs:
            if model_cls is None:
                app_logger.warning(f"跳过缺失模型对应的 fundamental factor 任务: {date_col}")
                continue
            self._fetch_and_store(
                api_func=api_func,
                model_cls=model_cls,
                date_col=date_col,
                api_date_param=api_date_param,
                query_history=query_history,
            )

if __name__ == "__main__":
    scraper = MassiveFundamentalsScraper()
    scraper.run()
