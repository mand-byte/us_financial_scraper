# -*- coding: utf-8 -*-
"""
================================================================================
Massive 公司行动搜刮器 (MassiveActionsFetcher)
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
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pytz
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from src.api.massive_api import MassiveApi
from src.dao.fundamental_repo import FundamentalRepo
from src.dao.market_data_repo import MarketDataRepo
from src.model.us_stock_actions_model import UsStockDividendsModel, UsStockSplitsModel
from src.utils.logger import app_logger as logger
import os
import concurrent.futures

class MassiveActionsFetcher:
    NYC = ZoneInfo("America/New_York")

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
                'cron', 
                hour=20, 
                minute=30, 
                timezone=self.NYC, 
                id="rolling_actions_refresh"
            )

            self.scheduler.add_job(
                self.refresh_recent_actions, 
                next_run_time=datetime.now(self.NYC), 
                id="rolling_actions_refresh",
                replace_existing=True
            )
            logger.info("✅ Massive 公司行动搜刮器已启动 (单日回刷3天模式)。")

    def stop(self):
        logger.info("🛑 Massive 公司行动搜刮器停止。")

    def _sync_dividend_for_ticker(self, task_data: tuple):
        ticker, composite_figi, delisted_date, active, sync_state, backfill_days = task_data
        try:
            # 1. 判定边界
            if not active and sync_state == 1: return
            
            last_date = self.fundamental_repo.get_latest_stock_dividends_date(composite_figi)
            
            if not active and pd.notna(delisted_date):
                delisted_dt = pd.to_datetime(delisted_date).replace(tzinfo=pytz.UTC).date()
                # 数据时间 + 1天(对于date来说) >= 下市时间 就改 state 标记并跳过
                if last_date + timedelta(days=1) >= delisted_dt:
                    self.market_repo.update_sync_status('us_stock_dividends', composite_figi, 'composite_figi', 1)
                    return
            else:
                delisted_dt = None

            # 活跃标的：如果不缺数据（距今3天内），说明每日增量已覆盖
            if active and last_date >= (datetime.now(self.NYC).date() - timedelta(days=3)):
                return

            if active and backfill_days:
                start_dt = (datetime.now(self.NYC) - timedelta(days=backfill_days)).date()
                start_date_str = max(start_dt, last_date).strftime('%Y-%m-%d')
            else:
                start_date_str = last_date.strftime('%Y-%m-%d')

            df_raw = self.massive.get_dividends(ticker=ticker, ex_dividend_date=start_date_str, limit=5000)
            
            if df_raw.empty:
                if not active: self.market_repo.update_sync_status('us_stock_dividends', composite_figi, 'composite_figi', 1)
                return

            if delisted_dt:
                df_raw['ex_dividend_date'] = pd.to_datetime(df_raw['ex_dividend_date']).dt.date
                df_raw = df_raw[df_raw['ex_dividend_date'] <= delisted_dt]
                if df_raw.empty:
                    if not active: self.market_repo.update_sync_status('us_stock_dividends', composite_figi, 'composite_figi', 1)
                    return

            clean_df = UsStockDividendsModel.format_dataframe(df_raw, composite_figi)
            if not clean_df.empty:
                self.fundamental_repo.insert_stock_dividends(clean_df)

            if not active:
                self.market_repo.update_sync_status('us_stock_dividends', composite_figi, 'composite_figi', 1)
                
        except Exception as e:
            logger.error(f"[{ticker}] 派息同步异常: {e}")

    def _sync_split_for_ticker(self, task_data: tuple):
        ticker, composite_figi, delisted_date, active, sync_state, backfill_days = task_data
        try:
            # 1. 判定边界
            if not active and sync_state == 1: return
            
            last_date = self.fundamental_repo.get_latest_stock_splits_date(composite_figi)
            
            if not active and pd.notna(delisted_date):
                delisted_dt = pd.to_datetime(delisted_date).replace(tzinfo=pytz.UTC).date()
                if last_date + timedelta(days=1) >= delisted_dt:
                    self.market_repo.update_sync_status('us_stock_splits', composite_figi, 'composite_figi', 1)
                    return
            else:
                delisted_dt = None

            # 活跃标的：如果不缺数据（距今3天内），跳过
            if active and last_date >= (datetime.now(self.NYC).date() - timedelta(days=3)):
                return

            if active and backfill_days:
                start_dt = (datetime.now(self.NYC) - timedelta(days=backfill_days)).date()
                start_date_str = max(start_dt, last_date).strftime('%Y-%m-%d')
            else:
                start_date_str = last_date.strftime('%Y-%m-%d')

            df_raw = self.massive.get_splits(ticker=ticker, execution_date=start_date_str, limit=5000)
            
            if df_raw.empty:
                if not active: self.market_repo.update_sync_status('us_stock_splits', composite_figi, 'composite_figi', 1)
                return

            if delisted_dt:
                df_raw['execution_date'] = pd.to_datetime(df_raw['execution_date']).dt.date
                df_raw = df_raw[df_raw['execution_date'] <= delisted_dt]
                if df_raw.empty:
                    if not active: self.market_repo.update_sync_status('us_stock_splits', composite_figi, 'composite_figi', 1)
                    return

            clean_df = UsStockSplitsModel.format_dataframe(df_raw, composite_figi)
            if not clean_df.empty:
                self.fundamental_repo.insert_stock_splits(clean_df)

            if not active:
                self.market_repo.update_sync_status('us_stock_splits', composite_figi, 'composite_figi', 1)
                
        except Exception as e:
            logger.error(f"[{ticker}] 拆分同步异常: {e}")

    def sync_actions(self, backfill_days: Optional[int] = None):
        max_workers = 10
        logger.info(f"🚀 启动公司行动智能同步 (模式: {backfill_days or '增量'})...")
        
        # 1. 派息任务
        tasks_div = self.market_repo.get_sync_tasks('us_stock_dividends', id_column='composite_figi')
        if not tasks_div.empty:
            sync_mask = (tasks_div['active'] == 1) | ((tasks_div['active'] == 0) & (tasks_div['sync_state'] == 0))
            filtered_div = tasks_div[sync_mask].dropna(subset=['ticker'])
            
            task_list_div = [
                (r.ticker, r.composite_figi, r.delisted_date, r.active, r.sync_state, backfill_days) 
                for r in filtered_div.itertuples(index=False)
            ]
            
            logger.info(f"派息任务: 准备处理 {len(task_list_div)} 个标的。")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(self._sync_dividend_for_ticker, task_list_div)

        # 2. 拆分任务
        tasks_split = self.market_repo.get_sync_tasks('us_stock_splits', id_column='composite_figi')
        if not tasks_split.empty:
            sync_mask = (tasks_split['active'] == 1) | ((tasks_split['active'] == 0) & (tasks_split['sync_state'] == 0))
            filtered_split = tasks_split[sync_mask].dropna(subset=['ticker'])
            
            task_list_split = [
                (r.ticker, r.composite_figi, r.delisted_date, r.active, r.sync_state, backfill_days) 
                for r in filtered_split.itertuples(index=False)
            ]
            
            logger.info(f"拆分任务: 准备处理 {len(task_list_split)} 个标的。")
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                executor.map(self._sync_split_for_ticker, task_list_split)

        logger.info("✅ 公司行动同步完毕。")

    def refresh_recent_actions(self):
        self.sync_actions(backfill_days=3)

if __name__ == "__main__":
    scraper = MassiveActionsFetcher()
    scraper.refresh_recent_actions()
