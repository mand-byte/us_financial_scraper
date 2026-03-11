import yfinance as yf
import pandas as pd
import time
import pytz
import threading
from datetime import datetime, timedelta
from src.utils.db_manager import ClickHouseManager
from src.utils.logger import app_logger
from src.utils.constants import Yahoo_Indicator_Code

class YahooMacroScraper:
    def __init__(self):
        self.db = None
        self.tz_et = pytz.timezone('US/Eastern')
        self._stop_event = threading.Event()
        self._thread = None
        
        # 使用 constants.py 中的 Yahoo_Indicator_Code
        self.tickers = Yahoo_Indicator_Code

    def _init_db(self):
        if self.db is None:
            self.db = ClickHouseManager()

    def fetch_and_save(self, start_date="2014-01-01"):
        app_logger.info(f"📡 正在从 Yahoo Finance 拉取宏观日线数据 (起点: {start_date})...")
        
        try:
            df_raw = yf.download(list(self.tickers.keys()), start=start_date, interval="1d", progress=False)
            if df_raw.empty: return 0

            df_close = df_raw['Close'].copy()
            
            if '^TNX' in df_close.columns:
                df_close['^TNX'] = df_close['^TNX'] / 10.0
            
            total_records = 0
            for yf_ticker, indicator_code in self.tickers.items():
                if yf_ticker not in df_close.columns: continue
                
                series = df_close[yf_ticker].dropna()
                if series.empty: continue
                
                df_single = series.reset_index()
                df_single.columns = ['date', 'actual_value']
                
                # 🌟 修复 2: 解决时区冲突问题
                publish_ts = pd.to_datetime(df_single['date']).dt.tz_localize(None) + pd.Timedelta(hours=23, minutes=59, seconds=59)
                df_single['publish_timestamp'] = publish_ts.dt.tz_localize('US/Eastern').dt.tz_convert('UTC')
                
                df_single['expected_value'] = df_single['actual_value'].shift(1)
                df_single['indicator_code'] = indicator_code
                
                df_to_save = df_single.dropna(subset=['expected_value']).drop(columns=['date'])
                
                if not df_to_save.empty:
                    # 使用 db 封装的方法插入
                    self.db.save_macro(df_to_save)
                    total_records += len(df_to_save)
            
            return total_records

        except Exception as e:
            app_logger.error(f"❌ Yahoo 宏观日线同步异常: {str(e)}")
            return 0

    def _main_loop(self):
        self._init_db()
        app_logger.info("🛡️ Yahoo 宏观“慢变量”后台子线程已启动 (日线精度)。")
        
        while not self._stop_event.is_set():
            try:
                # 🌟 优化：从 Yahoo_Indicator_Code 动态获取需要增量检查的内部代码
                target_codes = "','".join(set(Yahoo_Indicator_Code.values()))
                query = f"SELECT max(publish_timestamp) as last_ts FROM macro_indicators WHERE indicator_code IN ('{target_codes}')"
                res = self.db.client.query_df(query)
                
                start_date = "2014-01-01"
                if not res.empty and res.iloc[0]['last_ts'] is not None and not pd.isna(res.iloc[0]['last_ts']):
                    last_ts = res.iloc[0]['last_ts']
                    # 🌟 修复 3: 往前多推 7 天，确保 shift(1) 就算跨越春节/圣诞节长假也能拿到上一个交易日
                    start_date = (last_ts - timedelta(days=7)).strftime('%Y-%m-%d')
                
                count = self.fetch_and_save(start_date)
                if count > 0:
                    app_logger.info(f"✅ 宏观日线同步完成：新增/更新 {count} 条记录。")
            except Exception as e:
                app_logger.warning(f"⚠️ 轮询查询异常 (可能表还未建): {e}")

            app_logger.info("😴 宏观日线同步结束，进入 12 小时长睡眠...")
            for _ in range(12 * 60):
                if self._stop_event.is_set(): break
                time.sleep(60)

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        app_logger.info("✅ Yahoo 宏观日线搜刮器已激活。")

    def stop(self):
        self._stop_event.set()
        if self._thread: self._thread.join()

if __name__ == "__main__":
    scraper = YahooMacroScraper()
    scraper.start()
    try:
        while True: time.sleep(10)
    except KeyboardInterrupt:
        scraper.stop()
        app_logger.info("🛑 服务已安全退出。")
