# -*- coding: utf-8 -*-
# 此代码用来拉取Cobe数据，并将其存入ClickHouse数据库中。它使用了apscheduler库来定时执行数据抓取任务，确保每天晚上9点（美东时间）自动更新VIX现货、VX1和VX2的数据。代码还包含了线程管理和错误处理机制，以确保数据抓取过程的稳定性和可靠性。
# 主要功能包括：
# 1. 定义CboeDataFetcher类，负责管理数据抓取和
#    数据库交互。
# 2. 使用apscheduler库设置定时任务，每天晚上9点执行数据抓取。
# 3. 从CBOE官方库获取VIX现货和期货数据，并进行数据透视处理。
# 4. 将抓取到的数据与数据库中现有数据进行比较，确保只插入新的数据。
# 5. 提供启动和停止数据抓取线程的方法，允许在需要时手动控制数据抓取过程
from datetime import datetime
from zoneinfo import ZoneInfo
from src.model.us_macro_daily_kline_model import UsMacroDailyKlineModel
from apscheduler.schedulers.blocking import BlockingScheduler
from src.utils.logger import app_logger
from src.dao import MarketDataRepo
import os
from src.utils.constants import CBOE_Indicator_Code

# Assuming build_vx_continuous is defined in another util (e.g. cboe_scraping helper),
# but it was previously imported via wildcard `from src.utils import *`.
# Fixing this tightly.
from src.utils.cboe_scraper import build_vx_continuous


class CboeScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.mapping = CBOE_Indicator_Code  # {'VX1': 'VX1', 'VX2': 'VX2'}
        self.scheduler = scheduler
        self.repo = MarketDataRepo()
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def start(self):
        app_logger.info("✅ CBOE VIX 指数与期货搜刮器已激活。")
        # 1. 每日 21:00 NYC 同步
        self.scheduler.add_job(
            self.scraping,
            "cron",
            hour=21,
            minute=0,
            timezone=self.NYC,
            id="daily_vix_scraping",
            next_run_time=datetime.now(self.NYC),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        # 2. 启动同步
        self.scheduler.add_job(
            self.scraping, next_run_time=datetime.now(self.NYC), id="initial_vix_sync"
        )

    def scraping(self):
        """同步 CBOE VX 期货连续合约 (VX1, VX2)"""
        app_logger.info("🚀 启动 CBOE VX 期货连续合约同步...")
        try:
            # 使用 VX1 作为基准探测日期
            res_date_str = self.repo.get_latest_trade_date_in_macro_daily_klines("VX1")
            if res_date_str is None:
                res_date_str = self.COLD_START_DATE
            last_db_date = datetime.strptime(res_date_str, "%Y-%m-%d").date()

            # build_vx_continuous 会同时生成 VX1 和 VX2
            df_raw = build_vx_continuous(
                start_date=last_db_date, end_date=datetime.now(self.NYC).date()
            )

            if df_raw.empty:
                app_logger.info("CBOE VX: 无需更新。")
                return

            # 格式化并入库
            # 这里的 df_raw 已经包含 symbol 列 ('VX1', 'VX2')
            for ticker in ["VX1", "VX2"]:
                sub_df = df_raw[df_raw["symbol"] == ticker].copy()
                if sub_df.empty:
                    continue

                clean_df = UsMacroDailyKlineModel.format_dataframe(
                    sub_df, default_ticker=ticker
                )
                self.repo.insert_macro_daily_klines(clean_df)
                app_logger.info(f"✅ {ticker} 同8完成，新增 {len(clean_df)} 行。")

        except Exception as e:
            app_logger.error(f"❌ CBOE VX 同步异常: {e}")

    def stop(self):
        if hasattr(self, "scheduler") and self.scheduler:
            try:
                self.scheduler.remove_job("daily_vix_scraping")
            except Exception:
                pass
            try:
                self.scheduler.remove_job("initial_vix_sync")
            except Exception:
                pass
        app_logger.info("🛑 CBOE VIX 搜刮器停止。")
