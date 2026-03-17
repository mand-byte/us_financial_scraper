# -*- coding: utf-8 -*-
"""
================================================================================
Massive 基准 ETF 搜刮器 (MassiveBenchmarkScraper) - 需求与逻辑文档
================================================================================

[核心需求 - 已实现]
1. 数据源: Massive API v2 Aggs.
2. 标的范围: SPY (S&P 500), QQQ (Nasdaq 100), IWM (Russell 2000), DIA (Dow Jones).
3. 存储表: `us_benchmark_etf_klines`.
4. 逻辑特性:
   - 滚动重刷: 每天执行一次，强制回溯 3 天数据，确保数据一致性与修正。
   - 原始存储: 强制使用 `adjusted=false`，确保底层数据的物理纯洁性。
   - 调度: 每日 20:00 NYC 执行同步。
================================================================================
"""

import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from src.api.massive_api import MassiveApi
from src.model.us_benchmark_etf_kline_model import BenchmarkEtfKlineModel
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger as logger
import os

class MassiveBenchmarkScraper:
    NYC = ZoneInfo("America/New_York")
    BENCHMARKS = ["SPY", "QQQ", "IWM", "DIA"]

    def __init__(self, scheduler: Optional[BlockingScheduler] = None):
        self.massive = MassiveApi()
        self.repo = MarketDataRepo()
        self.scheduler = scheduler
        self.KLINE_SPAN = int(os.getenv("KLINE_SPAN", 5))
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def start(self):
        if self.scheduler:
            # 1. 🌟 每日滚动重刷 (每天一次，每次回溯 3 天)
            self.scheduler.add_job(
                self.refresh_recent_benchmarks, 
                'cron', 
                hour=20, 
                minute=0, 
                timezone=self.NYC, 
                id="rolling_benchmark_refresh"
            )

            # 2. 启动时立即执行一次
            self.scheduler.add_job(
                self.refresh_recent_benchmarks, 
                next_run_time=datetime.now(self.NYC), 
                id="rolling_benchmark_refresh",
                replace_existing=True
            )
            logger.info(f"✅ Massive 基准 ETF 搜刮器已启动 (单日回刷3天模式)。")

    def stop(self):
        logger.info("🛑 Massive 基准 ETF 搜刮器停止。")

    def sync_benchmarks(self, backfill_days: Optional[int] = None):
        """同步核心基准 ETF 历史与增量"""
        now_nyc = datetime.now(self.NYC)
        for ticker in self.BENCHMARKS:
            try:
                # 1. 判定起点
                if backfill_days:
                    start_dt = now_nyc - timedelta(days=backfill_days)
                    last_ms = int(start_dt.timestamp() * 1000)
                else:
                    last_dt = self.repo.get_latest_benchmark_etf_klines(ticker)
                    last_ms = int(last_dt.timestamp() * 1000)
                
                # 2. 确定目标结束时间
                end_ms = int(now_nyc.timestamp() * 1000)
                
                if last_ms >= (end_ms - 60000):
                    continue

                logger.info(f"🚀 正在同步基准 {ticker} (回溯: {backfill_days or '增量'}): {datetime.fromtimestamp(last_ms/1000, tz=self.NYC)} -> Now")
                
                # 3. 流式抓取并存入
                for page_df in self.massive.get_historical_klines(
                    ticker=ticker, 
                    multiplier=self.KLINE_SPAN, 
                    start=str(last_ms + 1), 
                    end=str(end_ms),
                    adjusted=False  # 🌟 核心量化原则：底层永远存原始数据
                ):
                    if page_df.empty: continue
                    
                    clean_df = BenchmarkEtfKlineModel.format_dataframe(page_df, ticker)
                    self.repo.insert_benchmark_etf_klines(clean_df)
                    
                logger.info(f"✅ 基准 {ticker} 同步完成。")

            except Exception as e:
                logger.error(f"同步基准 {ticker} 失败: {e}")

    def refresh_recent_benchmarks(self):
        """强制回溯最近 3 天基准数据"""
        logger.info("📅 执行最近 3 天基准 ETF 滚动重刷...")
        self.sync_benchmarks(backfill_days=3)

if __name__ == "__main__":
    scraper = MassiveBenchmarkScraper()
    scraper.refresh_recent_benchmarks()
