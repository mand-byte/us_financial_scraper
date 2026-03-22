import time
import os
import pandas as pd
from zoneinfo import ZoneInfo
from datetime import datetime

from src.utils.logger import app_logger
from src.utils.constants import ForexFactory_Indicator_Title_Map
from src.utils.forexfactory_scraper.scraper import scrape_month
from src.dao.market_data_repo import MarketDataRepo
from src.model.us_macro_indicators_model import UsMacroIndicatorsModel
from apscheduler.schedulers.blocking import BlockingScheduler


class ForexFactoryScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.indicators_map = ForexFactory_Indicator_Title_Map
        self.scheduler = scheduler
        self.repo = MarketDataRepo()
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def _clean_value(self, val_str):
        """清洗数值字符串 (如 <0.25%, 2.5%, 450K, 1.2M -> 0.25, 2.5, 450000, 1200000)"""
        if not val_str or str(val_str).strip() == "":
            return None
        clean_val = str(val_str).replace("%", "").replace(",", "").replace("<", "").replace(">", "").strip()
        try:
            if clean_val.endswith("K"):
                return float(clean_val[:-1]) * 1000
            if clean_val.endswith("M"):
                return float(clean_val[:-1]) * 1000000
            if clean_val.endswith("B"):
                return float(clean_val[:-1]) * 1000000000
            return float(clean_val)
        except ValueError:
            return None

    def process_scraped_data(self, df_scraped):
        """清洗并准备入库 (基于 Title 映射)"""
        if df_scraped.empty:
            return pd.DataFrame()

        processed_list = []
        for _, row in df_scraped.iterrows():
            if row.get("Currency") != "USD":
                continue
            actual_val = self._clean_value(row["Actual"])
            forecast_val = self._clean_value(row["Forecast"])
            title = str(row["Title"]).strip() if row.get("Title") else None
            
            if not title or title not in self.indicators_map:
                continue
            
            
            processed_list.append(
                {
                    "publish_timestamp": row["DateTime"],
                    "indicator_code": self.indicators_map[title],
                    "actual_value": actual_val,
                    "expected_value": forecast_val,
                }
            )

        return pd.DataFrame(processed_list)

    def sync_history(self):
        """同步历史数据：从数据库记录开始逐月追溯"""
        last_ts = self.repo.get_latest_macro_indicators(
            list(self.indicators_map.values())
        )
        start_dt = (
            last_ts
            if last_ts
            else datetime.strptime(self.COLD_START_DATE, "%Y-%m-%d").replace(
                tzinfo=ZoneInfo("UTC")
            )
        )

        now = datetime.now(ZoneInfo("UTC"))
        current = start_dt.replace(day=1)  # 从该月 1 号开始补

        while current <= now:
            month_label = current.strftime("%b").lower()
            year = current.year
            app_logger.info(f"📅 ForexFactory: 正在补齐月份: {year}-{month_label}")

            try:
                df_raw = scrape_month(month_label, year)
                
                if not df_raw.empty:
                    df_processed = self.process_scraped_data(df_raw)
                    if not df_processed.empty:
                        # Model -> DF -> Repo
                        clean_df = UsMacroIndicatorsModel.format_dataframe(df_processed)
                        self.repo.insert_marco_indicators(clean_df)
            except Exception as e:
                app_logger.error(f"抓取 {year}-{month_label} 失败: {e}")

            # 步进
            if current.month == 12:
                current = current.replace(year=year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
            time.sleep(1)

    def start(self):
        app_logger.info("✅ ForexFactory 宏观日历搜刮器激活。")

        # 1. 启动时补齐历史
        self.sync_history()

        # 2. 每日 21:00 NYC 更新当月最新数值
        self.scheduler.add_job(
            self.sync_history,  # 直接复用历史同步逻辑即可补齐当月
            "cron",
            hour=21,
            minute=0,
            timezone=self.NYC,
            id="daily_forexfactory_sync",
        )

    def stop(self):
        if self.scheduler:
            self.scheduler.remove_job("daily_forexfactory_sync")
        app_logger.info("🛑 ForexFactory 搜刮器停止。")
