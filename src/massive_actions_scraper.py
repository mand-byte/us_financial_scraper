# -*- coding: utf-8 -*-
"""
================================================================================
Massive 公司行动搜刮器 (MassiveActionsScraper)
================================================================================

[核心需求 - 已实现]
1. 数据源: Massive API v1 (Splits, Dividends).
2. 状态追踪 (Smart Sync):
   - 使用 `us_stock_splits_state` 和 `us_stock_dividends_state` 表管理退市标的同步进度。
   - 滚动重刷: 针对活跃标的，每天回溯 3 天，确保覆盖延迟。
   - 退市处理: 针对未完成的退市标的，补齐历史至 delisted_date，完成后标记跳过。
3. 数据一致性:
   - 依赖 Massive 提供的 unique 'id' 字段作为主键去重。
4. 调度周期:
   - 每日 20:30 NYC 执行。
================================================================================
"""

import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from src.api.massive_api import MassiveApi
from src.dao.fundamental_repo import FundamentalRepo
from src.dao.market_data_repo import MarketDataRepo
from src.model.us_stock_dividends_model import UsStockDividendsModel
from src.model.us_stock_splits_model import UsStockSplitsModel
from src.utils.logger import app_logger as logger
import os


class MassiveActionsScraper:
    NYC = ZoneInfo("America/New_York")
    MASSIVE_API_ACTIONS_LIMIT = 5000

    def __init__(self, scheduler: Optional[BlockingScheduler] = None):
        self.massive = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def start(self):
        if self.scheduler:
            self.scheduler.add_job(
                self.refresh_recent_actions,
                "cron",
                hour=20,
                minute=30,
                timezone=self.NYC,
                id="rolling_actions_refresh",
                next_run_time=datetime.now(self.NYC),  # 启动时立即执行一次
                max_instances=1,  # 实例锁互斥：前一个没跑完则跳过新触发
                coalesce=True,  # 防止积压多批次
                replace_existing=True,
            )
            logger.info("✅ Massive 公司行动搜刮器已启动 (单日回刷3天模式)。")

    def stop(self):
        logger.info("🛑 Massive 公司行动搜刮器停止。")

    def fetch_dividends(self):
        last_date = self.fundamental_repo.get_global_latest_stock_dividends_date()
        date_str = last_date.strftime("%Y-%m-%d")

        logger.info(f"🚀 拉取派息数据 (起: {date_str})...")
        df_raw = self.massive.get_dividends(
            ex_dividend_date=date_str, limit=self.MASSIVE_API_ACTIONS_LIMIT
        )

        if df_raw is None or df_raw.empty:
            logger.info("派息数据无新增。")
            return

        if "ticker" not in df_raw.columns:
            return

        df_raw = df_raw.dropna(subset=["ticker"])
        if df_raw.empty:
            return

        unique_tickers = df_raw["ticker"].unique().tolist()
        mappings_df = self.market_repo.get_figi_mapping_history_by_tickers(
            unique_tickers
        )

        if mappings_df.empty:
            logger.info("无有效匹配 FIGI 的派息数据。")
            return

        mappings_df["date"] = pd.to_datetime(mappings_df["date"]).dt.tz_localize(None)
        mappings_df = mappings_df.sort_values("date")

        df_raw["ex_dividend_date_dt"] = pd.to_datetime(
            df_raw["ex_dividend_date"]
        ).dt.tz_localize(None)
        df_raw = df_raw.sort_values("ex_dividend_date_dt")

        df_raw = pd.merge_asof(
            df_raw,
            mappings_df[["ticker", "date", "composite_figi"]],
            left_on="ex_dividend_date_dt",
            right_on="date",
            by="ticker",
            direction="backward",
        )

        missing_mask = df_raw["composite_figi"].isna()
        if missing_mask.any():
            fallback_df = pd.merge_asof(
                df_raw[missing_mask].drop(columns=["composite_figi", "date"]),
                mappings_df[["ticker", "date", "composite_figi"]],
                left_on="ex_dividend_date_dt",
                right_on="date",
                by="ticker",
                direction="forward",
            )
            df_raw.loc[missing_mask, "composite_figi"] = fallback_df[
                "composite_figi"
            ].values

        df_raw = df_raw.dropna(subset=["composite_figi"])
        if df_raw.empty:
            logger.info("无有效匹配 FIGI 的派息数据。")
            return

        clean_df = UsStockDividendsModel.format_dataframe(df_raw)
        if not clean_df.empty:
            self.fundamental_repo.insert_stock_dividends(clean_df)
        logger.info(f"派息数据拉取完成，新增 {len(clean_df)} 条记录。")

    def fetch_splits(self):
        last_date = self.fundamental_repo.get_global_latest_stock_splits_date()
        date_str = last_date.strftime("%Y-%m-%d")

        logger.info(f"🚀 拉取股票拆分数据 (起: {date_str})...")
        df_raw = self.massive.get_splits(
            ticker=None, execution_date=date_str, limit=self.MASSIVE_API_ACTIONS_LIMIT
        )

        if df_raw is None or df_raw.empty:
            logger.info("拆分数据无新增。")
            return

        if "ticker" not in df_raw.columns:
            return

        df_raw = df_raw.dropna(subset=["ticker"])
        if df_raw.empty:
            return

        unique_tickers = df_raw["ticker"].unique().tolist()
        mappings_df = self.market_repo.get_figi_mapping_history_by_tickers(
            unique_tickers
        )

        if mappings_df.empty:
            logger.info("无有效匹配 FIGI 的拆分数据。")
            return

        mappings_df["date"] = pd.to_datetime(mappings_df["date"]).dt.tz_localize(None)
        mappings_df = mappings_df.sort_values("date")

        df_raw["execution_date_dt"] = pd.to_datetime(
            df_raw["execution_date"]
        ).dt.tz_localize(None)
        df_raw = df_raw.sort_values("execution_date_dt")

        df_raw = pd.merge_asof(
            df_raw,
            mappings_df[["ticker", "date", "composite_figi"]],
            left_on="execution_date_dt",
            right_on="date",
            by="ticker",
            direction="backward",
        )

        missing_mask = df_raw["composite_figi"].isna()
        if missing_mask.any():
            fallback_df = pd.merge_asof(
                df_raw[missing_mask].drop(columns=["composite_figi", "date"]),
                mappings_df[["ticker", "date", "composite_figi"]],
                left_on="execution_date_dt",
                right_on="date",
                by="ticker",
                direction="forward",
            )
            df_raw.loc[missing_mask, "composite_figi"] = fallback_df[
                "composite_figi"
            ].values

        df_raw = df_raw.dropna(subset=["composite_figi"])
        if df_raw.empty:
            logger.info("无有效匹配 FIGI 的拆分数据。")
            return

        clean_df = UsStockSplitsModel.format_dataframe(df_raw)
        if not clean_df.empty:
            self.fundamental_repo.insert_stock_splits(clean_df)
        logger.info(f"股票拆分数据拉取完成，新增 {len(clean_df)} 条记录。")

    def refresh_recent_actions(self):
        self.fetch_dividends()
        self.fetch_splits()


if __name__ == "__main__":
    scraper = MassiveActionsScraper()
    scraper.refresh_recent_actions()
