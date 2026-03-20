# -*- coding: utf-8 -*-
"""
================================================================================
Massive Ratios 搜刮器 (MassiveRatiosFetcher) - 需求与逻辑文档
================================================================================

[核心需求]
1. 数据源:
   - Massive API v1 Ratios (/stocks/financials/v1/ratios).
   - 模式: 流式抓取，按页入库。
2. 同步逻辑:
   - 活跃标的 (active=1): 每次都增量拉取，state 表不记录
   - 退市标的 (active=0): 拉取数据，如果没有新数据则插入 state=1
3. 调度周期:
   - 每日 19:00 NYC 执行。
================================================================================
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from src.api.massive_api import MassiveApi
from src.dao.fundamental_repo import FundamentalRepo
from src.dao.market_data_repo import MarketDataRepo
from src.model.us_stock_factor_model import (
    StockDailyRatiosFactorsModel,
    StockDailyShortInterestfactorModel,
    StockDailyShortVolumefactorModel,
    StockDailyFloatFactorModel,
)
from src.utils.logger import app_logger as logger
import os


class MassiveFinancialFactorScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.massive = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def start(self):
        self.scheduler.add_job(
            self.fetch_ratios_factors,
            "cron",
            hour="20,22",
            minute=0,
            day_of_week="mon-fri",
            timezone=self.NYC,
            id="massive_ratios_fetcher_schedule",
            next_run_time=datetime.now(self.NYC),  # 启动时立即执行一次
            max_instances=1,  # 只允许一个实例，前一个没跑完则跳过新触发
            coalesce=True,  # 触发积压时合并为一次执行
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.fetch_short_interest_factors,
            "cron",
            hour="20,22",
            minute=0,
            day_of_week="mon-fri",
            timezone=self.NYC,
            id="massive_short_interest_factors_fetcher_schedule",
            next_run_time=datetime.now(self.NYC),  # 启动时立即执行一次
            max_instances=1,  # 只允许一个实例，前一个没跑完则跳过新触发
            coalesce=True,  # 触发积压时合并为一次执行
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.fetch_short_volume_factors,
            "cron",
            hour="20,22",
            minute=0,
            day_of_week="mon-fri",
            timezone=self.NYC,
            id="massive_short_volume_factors_fetcher_schedule",
            next_run_time=datetime.now(self.NYC),  # 启动时立即执行一次
            max_instances=1,  # 只允许一个实例，前一个没跑完则跳过新触发
            coalesce=True,  # 触发积压时合并为一次执行
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.fetch_float_factors,
            "cron",
            hour="20,22",
            minute=0,
            day_of_week="mon-fri",
            timezone=self.NYC,
            id="massive_float_factors_fetcher_schedule",
            next_run_time=datetime.now(self.NYC),  # 启动时立即执行一次
            max_instances=1,  # 只允许一个实例，前一个没跑完则跳过新触发
            coalesce=True,  # 触发积压时合并为一次执行
            replace_existing=True,
        )
        logger.info("✅ Massive Ratios 搜刮器已启动。")

    def stop(self):
        logger.info("🛑 Massive Ratios 搜刮器停止。")

    def fetch_ratios_factors(self):
        # ratios 同样为截面因子，不记录各自状态，每日定时抓取所有活跃标的
        active_tasks = self.market_repo.get_active_tickers()
        if active_tasks.empty:
            logger.info("No tasks found for ratios factors.")
            return

        for _, row in active_tasks.iterrows():
            try:
                ticker = row["ticker"]
                composite_figi = row["composite_figi"]

                data_raw = self.massive.get_ratios(ticker=ticker)

                if data_raw is None:
                    logger.warning(
                        f"⚠️ API Request failed for {ticker} (Ratios). Skipping."
                    )
                    continue

                if data_raw.empty:
                    logger.info(f"ℹ️ {ticker} has no ratios data.")
                    continue

                # Transform using Model
                df = StockDailyRatiosFactorsModel.format_dataframe(data_raw)
                df["composite_figi"] = composite_figi

                # Insert into Database
                self.fundamental_repo.insert_ratios_factors(df)
                logger.info(f"✅ Successfully inserted ratios factors for {ticker}.")

            except Exception as e:
                logger.error(
                    f"❌ Processing ratios factors for {row.get('ticker', 'Unknown')} failed: {e}"
                )
                continue

    def fetch_short_interest_factors(self):
        # 从数据库中获取所有股票代码
        df_tasks = self.market_repo.get_sync_tasks("us_stock_short_interest_state")
        if df_tasks.empty:
            logger.info("No tasks found.")
            return
        for _, row in df_tasks.iterrows():
            try:
                state = row["sync_state"]
                if state == 1:
                    # 已经是 delisted 并且数据已经抓完了，不需要再抓取了
                    continue

                ticker = row["ticker"]
                composite_figi = row["composite_figi"]
                active = row["active"]

                latest_ts = self.fundamental_repo.get_latest_short_interest_ts(
                    composite_figi
                )
                df_raw = self.massive.get_short_interest(
                    ticker, settlement_date=latest_ts.strftime("%Y-%m-%d")
                )

                # 情况 1: API 请求异常（网络中断、超时、供应商报错），返回了 None
                if df_raw is None:
                    logger.warning(
                        f"⚠️ API Request failed for {ticker}. Skipping to avoid data loss."
                    )
                    continue  # 直接跳过，坚决不更新 state，明天再试

                # 情况 2: API 正常返回，但明确告知没有数据
                if df_raw.empty:
                    logger.info(f"ℹ️ {ticker} has no short interest data.")
                    # 如果股票已退市且明确无数据，说明这是真的拉到底了
                    if active == 0:
                        self.market_repo.update_sync_status(
                            "us_stock_short_interest_state", composite_figi
                        )
                    continue

                # 格式化数据，此时列名对齐为 date（根据最新 DDL）
                df = StockDailyShortInterestfactorModel.format_dataframe(df_raw)

                # 数据插入：不需要在内存中做时间的精准拦截，抛给 ClickHouse 的 ReplacingMergeTree 自行覆盖更新
                self.fundamental_repo.insert_stock_daily_short_interest(df)
                logger.info(
                    f"✅ Successfully inserted {len(df)} short interest data for {ticker}."
                )

                # 完结判定：如果返回条数短于 50000 证明没分页/没后续了，并且该股票已退市，光荣标记完结！
                # 注意：使用 len(df_raw) 而不是 df_raw.count()
                if active == 0 and len(df_raw) < 50000:
                    self.market_repo.update_sync_status(
                        "us_stock_short_interest_state", composite_figi
                    )
                    logger.info(
                        f"🏁 {ticker} is delisted and data is exhausted. Marked state=1."
                    )

            except Exception as e:
                logger.error(
                    f"❌ Processing short interest for {row.get('ticker', 'Unknown')} failed: {e}"
                )
                continue

    def fetch_short_volume_factors(self):
        # 从任务表中拉取同步队列
        df_tasks = self.market_repo.get_sync_tasks("us_stock_short_volume")
        if df_tasks.empty:
            logger.info("No short volume tasks found.")
            return

        for _, row in df_tasks.iterrows():
            try:
                state = row["sync_state"]
                if state == 1:
                    continue

                ticker = row["ticker"]
                composite_figi = row["composite_figi"]
                active = row["active"]

                latest_ts = self.fundamental_repo.get_latest_short_volume_ts(
                    composite_figi
                )
                df_raw = self.massive.get_short_volume(
                    ticker, date=latest_ts.strftime("%Y-%m-%d")
                )

                # 情况 1: API 请求异常，返回了 None
                if df_raw is None:
                    logger.warning(
                        f"⚠️ API Request failed for {ticker}. Skipping to avoid data loss."
                    )
                    continue

                # 情况 2: API 正常返回，空数据
                if df_raw.empty:
                    logger.info(f"ℹ️ {ticker} has no short volume data.")
                    if active == 0:
                        self.market_repo.update_sync_status(
                            "us_stock_short_volume_state", composite_figi
                        )
                    continue

                # 格式化数据入库
                df = StockDailyShortVolumefactorModel.format_dataframe(df_raw)
                df["composite_figi"] = composite_figi

                self.fundamental_repo.insert_stock_daily_short_volume(df)
                logger.info(
                    f"✅ Successfully inserted {len(df)} short volume data for {ticker}."
                )

                # 完结判定：返回条数 < API limit(50000)，退市结案
                if active == 0 and len(df_raw) < 50000:
                    self.market_repo.update_sync_status(
                        "us_stock_short_volume_state", composite_figi
                    )
                    logger.info(
                        f"🏁 {ticker} is delisted and short volume data is exhausted. Marked state=1."
                    )

            except Exception as e:
                logger.error(
                    f"❌ Processing short volume for {row.get('ticker', 'Unknown')} failed: {e}"
                )
                continue

    def fetch_float_factors(self):
        # 自由流通股 (Float) 作为截面因子，不记录状态，每日定时抓取所有活跃标的
        active_tasks = self.market_repo.get_active_tickers()
        if active_tasks.empty:
            logger.info("No float factor tasks found.")
            return

        for _, row in active_tasks.iterrows():
            try:
                ticker = row["ticker"]
                composite_figi = row["composite_figi"]

                # 利用你添加的 get_float 接口获取最新自由流通股数
                df_raw = self.massive.get_float(ticker)

                # 容灾：网络超时返回 None，不造成任何假跳过
                if df_raw is None:
                    logger.warning(
                        f"⚠️ API Request failed for {ticker} (Float). Skipping."
                    )
                    continue

                if df_raw.empty:
                    logger.info(f"ℹ️ {ticker} has no float data.")
                    continue

                # 数据清洗重组为每日因子
                df = StockDailyFloatFactorModel.format_dataframe(df_raw)
                df["composite_figi"] = composite_figi

                # 追加到数据库
                self.fundamental_repo.insert_stock_daily_float(df)
                logger.info(f"✅ Successfully inserted float factor data for {ticker}.")

            except Exception as e:
                logger.error(
                    f"❌ Processing float factors for {row.get('ticker', 'Unknown')} failed: {e}"
                )
                continue
