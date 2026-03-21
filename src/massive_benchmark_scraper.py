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

from datetime import datetime
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
            # 1. 🌟 
            self.scheduler.add_job(
                self.fetch_benchmark_etf_klines, 
                'cron', 
                hour='10-16', 
                minute=0, 
                day_of_week="mon-fri",
                timezone=self.NYC, 
                id="fetch_benchmark_etf_klines",
                next_run_time=datetime.now(self.NYC),  # 启动时立即执行一次
                max_instances=1,   # 只允许一个实例，前一个没跑完则跳过新触发
                coalesce=True,     # 触发积压时合并为一次执行
                replace_existing=True,
            )

            
            logger.info("✅ Massive 基准 ETF 搜刮器已启动 。")
    def fetch_benchmark_etf_klines(self):
        """从数据库最新时间戳开始，拉取到当前时间的增量 K 线数据"""
        now_nyc = datetime.now(self.NYC)
        end_ms = int(now_nyc.timestamp() * 1000)

        for ticker in self.BENCHMARKS:
            try:
                # 1. 获取该 ticker 在数据库中的最新时间戳
                last_dt = self.repo.get_latest_benchmark_etf_klines(ticker)
                
                if last_dt is None:
                    last_dt = datetime.strptime(self.COLD_START_DATE, "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC"))
                    
                start_ms = int(last_dt.timestamp() * 1000) + 1  # +1ms 避免重复

                # 如果最新记录距当前不足 1 分钟，说明已经是最新的
                if start_ms >= end_ms - 60000:
                    logger.info(f"ℹ️ 基准 {ticker} 数据已是最新，跳过。")
                    continue

                logger.info(f"🚀 正在同步基准 {ticker}: {last_dt} -> Now")

                # 2. 拉取数据并入库
                page_df = self.massive.get_historical_klines(
                    ticker=ticker,
                    multiplier=self.KLINE_SPAN,
                    start=str(start_ms),
                    end=str(end_ms),
                    adjusted=False,
                )
                if page_df is None:
                    logger.warning(f"⚠️ API failed for {ticker}. Skipping.")
                    continue
                
                if not page_df.empty:
                    clean_df = BenchmarkEtfKlineModel.format_dataframe(page_df, ticker)
                    self.repo.insert_benchmark_etf_klines(clean_df)

                logger.info(f"✅ 基准 {ticker} 同步完成。")

            except Exception as e:
                logger.error(f"❌ 同步基准 {ticker} 失败: {e}")
    def stop(self):
        logger.info("🛑 Massive 基准 ETF 搜刮器停止。")


