import os
import time
import pandas as pd
import pytz
import threading
from datetime import datetime, timedelta
from fredapi import Fred
from dotenv import load_dotenv
from src.utils.db_manager import ClickHouseManager
from src.utils.logger import app_logger
from src.utils.constants import Fred_Indicator_Code

load_dotenv()

class FredScraper:
    def __init__(self):
        self.api_key = os.getenv('FRED_API_KEY')
        if not self.api_key:
            app_logger.warning("❌ 警告：未在 .env 中设置 FRED_API_KEY，FRED 同步将失败。")
        self.fred = Fred(api_key=self.api_key) if self.api_key else None
        self.db = None
        self.tz_et = pytz.timezone('US/Eastern')
        self._stop_event = threading.Event()
        self._thread = None
        self.indicators = Fred_Indicator_Code
        self.start_date_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def _init_db(self):
        if self.db is None:
            self.db = ClickHouseManager()

    def sync_all(self):
        """同步所有 FRED 指标"""
        self._init_db()
        for fred_ticker, internal_code in self.indicators.items():
            if self._stop_event.is_set(): break
            self._sync_single_indicator(fred_ticker, internal_code)
            time.sleep(1) # 礼貌延迟

    def _sync_single_indicator(self, fred_ticker, internal_code):
        """同步单个指标的逻辑"""
        query = f"SELECT max(publish_timestamp) as last_ts FROM macro_indicators WHERE indicator_code = '{internal_code}'"
        res = self.db.client.query_df(query)
        last_ts = res.iloc[0]['last_ts']
        
        if last_ts and not pd.isna(last_ts):
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
            if last_ts and not pd.isna(last_ts):
                df = df[df['publish_timestamp'] > last_ts]

            if not df.empty:
                self.db.save_macro(df)
                app_logger.info(f"✅ FRED 指标 {internal_code} ({fred_ticker}) 同步完成，新增 {len(df)} 条。")

        except Exception as e:
            app_logger.error(f"❌ FRED 抓取 {fred_ticker} 失败: {str(e)}")

    def _main_loop(self):
        app_logger.info("🛡️ FRED 搜刮子线程启动。")
        
        while not self._stop_event.is_set():
            try:
                # 1. 执行同步
                self.sync_all()
                
                # 2. 计算下一次唤醒时间 (美东 17:15，即官方更新后 15 分钟)
                now_et = datetime.now(self.tz_et)
                wake_up_et = now_et.replace(hour=17, minute=15, second=0, microsecond=0)
                
                if now_et >= wake_up_et:
                    # 如果今天已经过了 17:15，则设为明天
                    wake_up_et += timedelta(days=1)
                
                wake_up_utc = wake_up_et.astimezone(pytz.UTC)
                wait_seconds = (wake_up_utc - datetime.now(pytz.UTC)).total_seconds()
                
                app_logger.info(f"😴 FRED 指标已扫描。下次唤醒 (美东 17:15): {wake_up_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                
                # 分段休眠
                while wait_seconds > 0:
                    if self._stop_event.is_set(): break
                    sleep_time = min(60, wait_seconds)
                    time.sleep(sleep_time)
                    wait_seconds -= sleep_time
                    
            except Exception as e:
                app_logger.error(f"🧨 FRED 调度异常: {str(e)}")
                time.sleep(60)

    def start(self):
        if not self.fred:
            app_logger.error("❌ FRED API Key 缺失，无法启动同步。")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        app_logger.info("✅ FRED 后台搜刮器已激活。")

    def stop(self):
        self._stop_event.set()
        if self._thread: self._thread.join()

if __name__ == "__main__":
    scraper = FredScraper()
    scraper.sync_all()
