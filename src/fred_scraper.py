import os
import time
import pandas as pd
import pytz
import threading
from datetime import datetime, timedelta
from fredapi import Fred

from src.dao.market_data_repo import MarketDataRepo
from src.model.us_macro_indicators_model import UsMacroIndicatorsModel
from src.utils.logger import app_logger
from src.utils.constants import Fred_Indicator_Code
from apscheduler.schedulers.blocking import BlockingScheduler


class FredScraper:
    def __init__(self,scheduler:BlockingScheduler):
        self.api_key = os.getenv('FRED_API_KEY')
        if not self.api_key:
            app_logger.warning("❌ 警告：未在 .env 中设置 FRED_API_KEY，FRED 同步将失败。")
        self.fred = Fred(api_key=self.api_key)
        self.tz_et = pytz.timezone('US/Eastern')
        self.scheduler=scheduler
        self.indicators = Fred_Indicator_Code
        self.start_date_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")


    def sync_all(self):
        
        for fred_ticker, internal_code in self.indicators.items():
            self._sync_single_indicator(fred_ticker, internal_code)
           

    def _sync_single_indicator(self, fred_ticker, internal_code):
        """同步单个指标的逻辑"""
        
        last_ts=MarketDataRepo().get_latest_macro_indicators(internal_code)
        
        if last_ts :
            # 增量抓取起点：最后一条记录日期
            start_date = last_ts.astimezone(self.tz_et).strftime('%Y-%m-%d')
        else:
            start_date = self.start_date_str

        try:
            # 获取数据 (FRED 仅返回日期)
            series = self.fred.get_series(fred_ticker, observation_start=start_date)
            if series.empty: return

            df = pd.DataFrame(series, columns=['actual_value']).reset_index()
            df.rename(columns={'index': 'date'}, inplace=True)
            
            # 💡 策略：将 FRED 只有日期的记录，统一设定为美东 17:00 (5:00 PM) 
            # 这样可以确保在该时间点后抓取时，数据已经由官方发布
            df['publish_timestamp'] = pd.to_datetime(df['date']).apply(
                lambda x: self.tz_et.localize(x.replace(hour=17, minute=0, second=0)).astimezone(pytz.UTC)
            )
            df['indicator_code'] = internal_code
            df['expected_value'] = None 

            # 过滤掉重复
            if last_ts:
                df = df[df['publish_timestamp'] > last_ts]

            if not df.empty:
                UsMacroIndicatorsModel.format_dataframe(df)
                MarketDataRepo().insert_marco_indicators(df)
                app_logger.info(f"✅ FRED 指标 {internal_code} ({fred_ticker}) 同步完成，新增 {len(df)} 条。")

        except Exception as e:
            app_logger.error(f"❌ FRED 抓取 {fred_ticker} 失败: {str(e)}")

    def _main_loop(self):
        app_logger.info("🛡️ FRED 搜刮子线程启动。")
        self.scheduler.add_job(
            self.sync_all, 
            'cron', 
            hour=17, 
            minute=15, 
            id='daily_fred_scraping'
        )
       

    def start(self):
        if not self.fred:
            app_logger.error("❌ FRED API Key 缺失，无法启动同步。")
            return
        self._main_loop()
        app_logger.info("✅ FRED 后台搜刮器已激活。")

    def stop(self):
        if self.scheduler:
            self.scheduler.remove_job('daily_fred_scraping')
        app_logger.info("🛑 FRED 已退出。")
