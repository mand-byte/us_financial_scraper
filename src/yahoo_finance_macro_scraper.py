import yfinance as yf
import pandas as pd
import time
import pytz
import threading
from datetime import datetime, timedelta
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger
from src.utils.constants import Yahoo_Indicator_Code
from src.utils.us_trading_calendar import get_trading_calendar
from src.model.us_macro_daily_kline_model import UsMacroDailyKlineModel
import os
from apscheduler.schedulers.blocking import BlockingScheduler

class YahooMacroScraper:
    def __init__(self,scheduler:BlockingScheduler):
        
        self.tz_et = pytz.timezone('US/Eastern')
        self._thread = None
        self.scheduler=scheduler
        # 使用 constants.py 中的 Yahoo_Indicator_Code
        self.tickers = Yahoo_Indicator_Code
       
    

    def fetch_and_save(self, start_date="2014-01-01"):
        app_logger.info(f"📡 正在从 Yahoo Finance 拉取宏观日线数据 (起点: {start_date})...")
        now_et = datetime.now(self.tz_et).strftime('%Y-%m-%d')
        try:
            df_raw = yf.download(list(self.tickers.keys()), start=start_date,end=now_et, interval="1d", progress=False)
            if df_raw is None or "Close" not in df_raw.columns: 
                return 
            df_close = pd.DataFrame(df_raw['Close']).copy()
            #与cboe的vx统一，全部转小写    
            df_close.columns = df_close.columns.str.strip().str.lower()
            
            if '^tnx' in df_close.columns: # 注意这里也要用小写匹配
                df_close['^tnx'] = df_close['^tnx'] / 10.0
                
            for yf_ticker, indicator_code in self.tickers.items():
                yf_ticker_lower = yf_ticker.lower()
                if yf_ticker_lower not in df_close.columns: continue
                
                series = df_close[yf_ticker_lower].dropna()
                if series.empty: continue
                
                # 3. reset_index 后，原来的日期索引会变成一列，通常默认叫 'Date'
                df_single = series.reset_index()
                df_single.rename(columns={
                    'Date': 'trade_date', 
                    'date': 'trade_date',
                    yf_ticker_lower: 'close'  
                }, inplace=True)
                # 🌟 关键赋值：把大写的标签（如 'US10Y'）填入内容中
                df_single['ticker'] = indicator_code
                df_to_save = df_single.dropna()
                
                # 🌟 魔法生效：一键对齐标准模型！
                # 无论 Yahoo 返回什么神仙格式，出来必定是 [ticker, trade_date, open, high, low, close, volume, oi] 的完美队形
                df_to_save = UsMacroDailyKlineModel.format_dataframe(df_single, default_ticker=indicator_code)
                if not df_to_save.empty:
                    # 使用 db 封装的方法插入
                    MarketDataRepo().insert_macro_daily_klines(df_to_save)
        except Exception as e:
            app_logger.error(f"❌ Yahoo 宏观日线同步异常: {str(e)}")
            return 0

    def _checking_data_complementation(self):
        try:
            # 🌟 优化：从 Yahoo_Indicator_Code 动态获取需要增量检查的内部代码
            res=MarketDataRepo().get_latest_trade_date_in_macro_daily_klines(Yahoo_Indicator_Code)
            self.fetch_and_save(res)
            
        except Exception as e:
            app_logger.warning(f"⚠️ 轮询查询异常 (可能表还未建): {e}")
    
    def hourly_scraping(self):
        
        #有些指标非美股时间也会更新，所以不区分交易日，无脑强制覆盖当天数据，确保数据完整性和时效性
        self.fetch_and_save(start_date=datetime.now(self.tz_et).strftime('%Y-%m-%d'))

    def _main_loop(self):
        app_logger.info("🛡️ Yahoo 宏观“慢变量”后台子线程已启动 (日线精度)。")
        self._checking_data_complementation()
        
        self.scheduler.add_job(
            self.hourly_scraping, 
            'cron', 
            minute=0, 
            id='hourly_yahoo_marco_scraping',
            coalesce=True            # 如果多次错过只运行一次
        )
    def start(self):
        self._main_loop()
        app_logger.info("✅ Yahoo 宏观日线搜刮器已激活。")

    def stop(self):
        if self.scheduler:
            self.scheduler.remove_job('hourly_yahoo_marco_scraping')
             
        app_logger.info("🛑 yahoo marco 已退出。")

