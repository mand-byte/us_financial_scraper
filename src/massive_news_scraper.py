# -*- coding: utf-8 -*-
"""
================================================================================
Massive 新闻原始搜刮器 (MassiveNewsScraper) - 需求与逻辑文档
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
   - 当前: 每 5 分钟执行一次（启动立即触发）。
   - 建议: 新闻类事件 24/7 到达，保持 5 分钟频率；若需降本可改为盘中 5 分钟 + 非盘中 15 分钟。
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
from src.config.settings import settings


class MassiveNewsScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.massive = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler
        self.COLD_START_DATE = settings.scraper.scraping_start_date
        self.API_MAX_LIMIT = 1000

    def start(self):
        """启动时拉一次，之后每5分钟拉1000条，从最近的新闻条数开始拉取"""
        self.scheduler.add_job(
            self.fectch_news,
            "cron",
            minute="*/5",
            timezone=self.NYC,
            id="fectch_news",
            next_run_time=datetime.now(self.NYC),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        logger.info("✅ Massive 新闻搜刮器已启动")

    def stop(self):
        logger.info("🛑 Massive 新闻搜刮器停止。")

    def fectch_news(self):
        last_ts = self.fundamental_repo.get_global_latest_news_timestamp()

        if last_ts is None:
            # 冷启动：使用环境变量定义的起点
            last_ts = datetime.strptime(self.COLD_START_DATE, "%Y-%m-%d")
            logger.info(f"新闻初次同步，使用冷启动存量日期: {self.COLD_START_DATE}")

        logger.debug(f"新闻增量起点: {last_ts}")
        if not isinstance(last_ts, pd.Timestamp):
            last_ts = pd.Timestamp(last_ts)
        ts = last_ts.tz_convert("UTC") if last_ts.tzinfo else last_ts.tz_localize("UTC")
        # 必须使用 gte：当单次返回不足覆盖同一时间戳下的所有新闻时，gt 会造成漏数。
        date_raw = self.massive.get_stock_news(
            published_utc_type="published_utc.gte", date=ts.isoformat()
        )
        if date_raw is None or date_raw.empty:
            logger.info("无新增新闻。")
            return

        # API 返回的 news 数据中包含 tickers (列表)，id (新闻ID)，我们需要先展开 tickers
        if "tickers" in date_raw.columns:
            # 过滤掉关联股票超过 5 只的大型盘点/宏观噪音新闻，保留高价值的个股强相关新闻
            date_raw = date_raw[
                date_raw["tickers"].apply(lambda x: isinstance(x, list) and len(x) <= 5)
            ]

            date_raw = (
                date_raw.explode("tickers")
                .rename(columns={"tickers": "ticker"})
                .dropna(subset=["ticker"])
            )
        else:
            logger.info("未发现 tickers 字段。")
            return

        if date_raw.empty:
            logger.info("无有效 tickers 数据。")
            return

        # 将 id 重命名为 news_id 用于数据库存储
        if "id" in date_raw.columns:
            date_raw = date_raw.rename(columns={"id": "news_id"})

        # 解包 publisher 字典提取 name
        if "publisher" in date_raw.columns:
            date_raw["publisher"] = date_raw["publisher"].apply(
                lambda x: x.get("name", "") if isinstance(x, dict) else str(x)
            )

        data = UsStockNewsRawModel.format_dataframe(date_raw)
        if not data.empty:
            before = len(data)
            data = data.dropna(subset=["published_utc"])
            data = data[
                (data["news_id"].astype(str).str.len() > 0)
                & (data["ticker"].astype(str).str.len() > 0)
            ]
            data = data.drop_duplicates(
                subset=["news_id", "ticker", "published_utc"], keep="last"
            )
            dropped = before - len(data)
            if dropped > 0:
                logger.warning(f"⚠️ 新闻数据清洗后丢弃 {dropped} 条脏/重复记录。")
        if not data.empty:
            self.fundamental_repo.insert_stock_news_raw(data)
            logger.info(f"✅ 新闻增量入库完成，新增 {len(data)} 条。")
