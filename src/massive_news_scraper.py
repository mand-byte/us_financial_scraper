# -*- coding: utf-8 -*-
"""
================================================================================
Massive 新闻原始搜刮器 (MassiveNewsFetcher) - 需求与逻辑文档
================================================================================

[核心需求 - 已实现]
1. 数据源: 
   - Massive API v2 News (/v2/reference/news).
   - 模式: 流式抓取 (Generator)，按页入库。
2. 状态追踪 (Smart Sync):
   - 垂直解耦: 使用 `us_stock_news_raw_state` 表管理退市标的同步进度。
   - 滚动重刷: 针对活跃标的，每天回溯 3 天新闻，确保覆盖修正与延迟。
   - 退市处理: 针对未完成的退市标的，补齐从 COLD_START 到 delisted_date 的历史。
3. 数据一致性:
   - 始终通过 UsStockNewsRawModel 进行数据清洗与 figi 对齐。
   - 对退市标的新闻执行严格时间截断。
4. 调度周期:
   - 每日 20:00 NYC 执行。
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
from src.model.us_stock_news_raw_model import UsStockNewsRawModel
from src.utils.logger import app_logger as logger
import os

class MassiveNewsFetcher:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.massive = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def start(self):
        """异步注册任务并触发初始化补数"""
        # 1. 🌟 每日滚动重刷 (每天一次，活跃标的回溯 3 天)
        self.scheduler.add_job(
            self.refresh_recent_news, 
            'cron', 
            hour=20, 
            minute=0, 
            timezone=self.NYC, 
            id="rolling_news_refresh"
        )

        # 2. 启动时立即执行一次
        self.scheduler.add_job(
            self.refresh_recent_news, 
            next_run_time=datetime.now(self.NYC), 
            id="rolling_news_refresh",
            replace_existing=True
        )
        logger.info("✅ Massive 新闻搜刮器已启动 (单日回刷3天模式)。")

    def stop(self):
        logger.info("🛑 Massive 新闻搜刮器停止。")

    def sync_latest_news(self, backfill_days: Optional[int] = None):
        """
        全市场新闻同步逻辑：
        1. 活跃标的：如果 backfill_days 有值，从回溯点开始同步；否则从全局最大时间戳增量同步。
        2. 退市标的：如果状态为 0，则执行该标的历史追溯，完成后标记状态表。
        """
        logger.info(f"🚀 启动新闻智能同步任务 (模式: {backfill_days or '增量'})...")
        
        try:
            # 1. 获取任务清单
            tasks_df = self.market_repo.get_sync_tasks('us_stock_news_raw', id_column='composite_figi')
            if tasks_df.empty: return

            universe_map = dict(zip(tasks_df["ticker"], tasks_df["composite_figi"]))
            
            # A. 活跃标的：增量或滚动逻辑
            active_tasks = tasks_df[tasks_df['active'] == 1]
            if not active_tasks.empty:
                if backfill_days:
                    # 滚动重刷：从 backfill_days 前开始
                    start_dt = datetime.now(pytz.UTC) - timedelta(days=backfill_days)
                else:
                    # 普通增量：从库中最大时间开始
                    start_dt = self.fundamental_repo.get_global_latest_news_timestamp()
                
                start_date_str = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                
                for page_df in self.massive.get_stock_news(
                    ticker=None,
                    published_utc_type="published_utc.gt",
                    date=start_date_str,
                    order="asc",
                    limit=1000
                ):
                    if page_df.empty: continue
                    clean_df = UsStockNewsRawModel.format_dataframe(page_df, universe_map)
                    if not clean_df.empty:
                        self.fundamental_repo.insert_stock_news_raw(clean_df)

            # B. 退市标的：补齐历史逻辑 (仅针对 sync_state == 0)
            delisted_pending = tasks_df[(tasks_df['active'] == 0) & (tasks_df['sync_state'] == 0)]
            for row in delisted_pending.itertuples():
                try:
                    last_dt = self.fundamental_repo.get_latest_stock_news_raw_timestamp(row.cik)
                    delisted_dt = pd.to_datetime(row.delisted_date, utc=True)
                    
                    if last_dt + timedelta(hours=8) >= delisted_dt:
                        self.market_repo.update_sync_status('us_stock_news_raw', row.composite_figi, 'composite_figi', 1)
                        continue
                        
                    start_str = last_dt.strftime('%Y-%m-%d')
                    
                    for page_df in self.massive.get_stock_news(
                        ticker=row.ticker,
                        published_utc_type="published_utc.gt",
                        date=start_str,
                        order="asc",
                        limit=1000
                    ):
                        if page_df.empty: break
                        page_df['published_utc_dt'] = pd.to_datetime(page_df['published_utc'])
                        page_df = page_df[page_df['published_utc_dt'] <= delisted_dt]
                        
                        if page_df.empty: break
                        
                        clean_df = UsStockNewsRawModel.format_dataframe(page_df, universe_map)
                        if not clean_df.empty:
                            self.fundamental_repo.insert_stock_news_raw(clean_df)
                    
                    self.market_repo.update_sync_status('us_stock_news_raw', row.composite_figi, 'composite_figi', 1)
                    logger.info(f"退市标的 {row.ticker} 新闻历史补齐。")
                except Exception as e:
                    logger.error(f"退市标的 {row.ticker} 新闻同步异常: {e}")

            logger.info(f"全市场新闻同步任务执行完毕。")

        except Exception as e:
            logger.error(f"全市场新闻同步失败: {e}")

    def refresh_recent_news(self):
        """滚动重刷活跃标的 3 天新闻"""
        self.sync_latest_news(backfill_days=3)
