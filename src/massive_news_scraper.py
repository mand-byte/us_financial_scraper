"""
================================================================================
Massive 新闻搜刮器 (MassiveNewsFetcher) - 需求与逻辑文档
================================================================================

[核心需求 - 已实现]
1. 功能定位: 仅负责数据摄取 (Scraping)，不涉及处理。
2. 数据源: 
   - 模式: 流式抓取 (Generator)，逐页入库，支持 10 年海量数据补齐。
   - 内容: 自动排除无 ticker 关联的杂讯新闻。
3. 调度逻辑:
   - 异步启动: start() 时触发 initial_news_backfill。
   - 周期同步: APScheduler 每 5 分钟增量同步全市场。
================================================================================
"""

import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
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
        self.scheduler.add_job(
            self.sync_latest_news, 'interval', minutes=5, id="news_incremental_sync"
        )
        self.scheduler.add_job(
            self.sync_latest_news, 
            next_run_time=datetime.now(self.NYC), 
            id="initial_news_backfill"
        )
        logger.info("✅ Massive 新闻搜刮器已启动 (流式处理模式)。")

    def stop(self):
        logger.info("🛑 Massive 新闻搜刮器停止。")

    def sync_latest_news(self):
        """全市场新闻增量同步逻辑 (流式模式)"""
        logger.info("🚀 启动全市场新闻流同步...")
        
        try:
            last_ts = self.fundamental_repo.get_global_latest_news_timestamp()
            start_date_str = last_ts.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            # 🌟 核心：流式产出
            # 获取全宇宙映射用于 figi 对齐
            universe = self.market_repo.get_universe_tickers()
            universe_map = dict(zip(universe["ticker"], universe["composite_figi"]))

            for page_df in self.massive.get_stock_news(
                ticker=None,
                published_utc_type="published_utc.gt",
                date=start_date_str,
                order="asc",
                limit=1000
            ):
                if page_df.empty: continue

                # 格式化并按标的展开 (Explode)
                clean_df = UsStockNewsRawModel.format_dataframe(page_df, universe_map)
                if not clean_df.empty:
                    self.fundamental_repo.insert_stock_news_raw(clean_df)
                    # 每一页数据存入后即从内存释放
                    
            logger.info(f"全市场新闻同步任务执行完毕。")

        except Exception as e:
            logger.error(f"全市场新闻同步失败: {e}")
