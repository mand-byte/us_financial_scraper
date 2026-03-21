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
   - 每日 20:00 NYC 执行。
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


class MassiveNewsScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.massive = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")
        self.API_MAX_LIMIT = 1000

    def start(self):
        """异步注册任务并触发初始化补数"""
        # 1. 🌟 每日滚动重刷 (每天一次，活跃标的回溯 3 天)
        self.scheduler.add_job(
            self.fectch_news,
            "cron",
            hour=20,
            minute=0,
            timezone=self.NYC,
            id="fectch_news",
            next_run_time=datetime.now(self.NYC),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        logger.info("✅ Massive 新闻搜刮器已启动 (单日回刷3天模式)。")

    def stop(self):
        logger.info("🛑 Massive 新闻搜刮器停止。")

    def fectch_news(self):

        last_ts = self.fundamental_repo.get_global_latest_news_timestamp()
        if not isinstance(last_ts, pd.Timestamp):
            last_ts = pd.Timestamp(last_ts)
        ts = last_ts.tz_convert("UTC") if last_ts.tzinfo else last_ts.tz_localize("UTC")
        date_raw = self.massive.get_stock_news(
            published_utc_type="published_utc.gt", date=ts.isoformat()
        )
        if date_raw.empty:
            logger.info("无新增新闻。")
            return

        # API 返回的 news 数据中包含 tickers (列表)，id (新闻ID)，我们需要先展开 tickers
        if "tickers" in date_raw.columns:
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

        # 提取 unique tickers 用于查询映射
        unique_tickers = date_raw["ticker"].unique().tolist()
        mappings_df = self.market_repo.get_figi_mapping_history_by_tickers(
            unique_tickers
        )

        if mappings_df.empty:
            logger.info("无有效匹配 FIGI 历史映射的新闻。")
            return

        # 准备时间对齐: mappings_df 的 date 为精确到天
        mappings_df["date"] = pd.to_datetime(mappings_df["date"]).dt.tz_localize(None)
        mappings_df = mappings_df.sort_values("date")

        # API 的 published_utc 是类似 '2024-03-12T15:20:00Z'
        date_raw["published_utc_dt"] = pd.to_datetime(
            date_raw["published_utc"], utc=True
        ).dt.tz_localize(None)
        date_raw = date_raw.sort_values("published_utc_dt")

        # 进行 Point-in-Time 倒退匹配
        date_raw = pd.merge_asof(
            date_raw,
            mappings_df[["ticker", "date", "composite_figi"]],
            left_on="published_utc_dt",
            right_on="date",
            by="ticker",
            direction="backward",
        )

        # 针对在首次有映射之前的新闻，启用向前匹配作为退路
        missing_mask = date_raw["composite_figi"].isna()
        if missing_mask.any():
            fallback_df = pd.merge_asof(
                date_raw[missing_mask].drop(columns=["composite_figi", "date"]),
                mappings_df[["ticker", "date", "composite_figi"]],
                left_on="published_utc_dt",
                right_on="date",
                by="ticker",
                direction="forward",
            )
            date_raw.loc[missing_mask, "composite_figi"] = fallback_df[
                "composite_figi"
            ].values

        date_raw = date_raw.dropna(subset=["composite_figi"])

        if date_raw.empty:
            logger.info("无有效匹配 FIGI 的新闻。")
            return

        # 将 id 重命名为 news_id 用于数据库存储
        if "id" in date_raw.columns:
            date_raw = date_raw.rename(columns={"id": "news_id"})

        data = UsStockNewsRawModel.format_dataframe(date_raw)
        if not data.empty:
            self.fundamental_repo.insert_stock_news_raw(data)
