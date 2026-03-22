from src.config.settings import settings
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from fredapi import Fred

from src.dao.market_data_repo import MarketDataRepo
from src.model.us_macro_indicators_model import UsMacroIndicatorsModel
from src.utils.logger import app_logger
from src.utils.constants import Fred_Indicator_Code
from apscheduler.schedulers.blocking import BlockingScheduler


class FredScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.api_key = settings.api.fred_api_key
        if not self.api_key:
            app_logger.warning("❌ 未设置 FRED_API_KEY，FRED 同步将跳过。")

        self.fred = Fred(api_key=self.api_key) if self.api_key else None
        self.scheduler = scheduler
        self.indicators = Fred_Indicator_Code
        self.repo = MarketDataRepo()
        self.COLD_START_DATE = settings.scraper.scraping_start_date

    def sync_all(self):
        """同步所有 FRED 定义的宏观指标"""
        if not self.fred:
            return

        app_logger.info("🚀 启动 FRED 宏观指标增量同步...")
        for fred_ticker, internal_code in self.indicators.items():
            try:
                # 1. 获取库中最新时间戳
                last_ts = self.repo.get_latest_macro_indicators(internal_code)
                start_date = (
                    last_ts.astimezone(self.NYC).strftime("%Y-%m-%d")
                    if last_ts
                    else self.COLD_START_DATE
                )

                # 2. 抓取数据
                series = self.fred.get_series(fred_ticker, observation_start=start_date)
                if series.empty:
                    continue

                df = pd.DataFrame(series, columns=["actual_value"]).reset_index()
                df.rename(columns={"index": "date"}, inplace=True)

                # 3. PIT 模拟：设定为披露日美东 17:00，防止回测偷看
                df["publish_timestamp"] = pd.to_datetime(df["date"]).apply(
                    lambda x: (
                        x.replace(hour=17, minute=0, second=0)
                        .replace(tzinfo=self.NYC)
                        .astimezone(ZoneInfo("UTC"))
                    )
                )
                df["indicator_code"] = internal_code
                df["expected_value"] = None  # FRED 原始数据通常无预期值

                # 4. 严格增量过滤
                if last_ts:
                    df = df[df["publish_timestamp"] > last_ts]

                if not df.empty:
                    # 格式化并入库
                    clean_df = UsMacroIndicatorsModel.format_dataframe(pd.DataFrame(df))
                    self.repo.insert_macro_indicators(clean_df)
                    app_logger.info(
                        f"✅ FRED: {internal_code} 同步完成 ({len(clean_df)} 条)。"
                    )

            except Exception as e:
                app_logger.error(f"❌ FRED 同步 {internal_code} 失败: {e}")

    def start(self):
        if not self.fred:
            return

        # 2. 每日 17:15 NYC 执行 (确保当日收盘后的指标已发布)
        self.scheduler.add_job(
            self.sync_all,
            "cron",
            hour=[8, 18, 20],
            minute=[5, 15, 30, 45],
            timezone=self.NYC,
            id="daily_fred_sync",
            coalesce=True,
            replace_existing=True,
            misfire_grace_time=24 * 3600,
            next_run_time=datetime.now(self.NYC),
            max_instances=1,
        )
        app_logger.info("✅ FRED 搜刮器激活。")

    def stop(self):
        if self.scheduler:
            try:
                self.scheduler.remove_job("daily_fred_sync")
            except Exception:
                pass
        app_logger.info("🛑 FRED 搜刮器停止。")
