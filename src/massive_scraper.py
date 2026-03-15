import requests
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from src.model.us_stock_universe_model import UsStockUniverseModel
from src.dao.market_data_repo import MarketDataRepo
import os

from src.utils.logger import app_logger
from src.api import MassiveApi
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
class MassiveDataFetcher:
    def __init__(self,scheduler:BlockingScheduler):
        self.pytz=pytz.timezone('us/eastern')
        self.massive=MassiveApi()
        self.scheduler=scheduler

    def fetch_klines(self, ticker: str, start_date: str, end_date: str):
        pass

    def fetch_historical_news(self, ticker: str):
        pass

    def fetch_financial_statements(self, ticker: str, statement_type: str = "income-statements"):
        pass
    
    def fetch_universe_ticker(self):
        active_raw=MassiveApi().get_all_tickers()
        if not active_raw.empty:
            active_data=UsStockUniverseModel.format_dataframe(active_raw)
            MarketDataRepo().insert_us_stock_universe(active_data)
            app_logger.info("全宇宙表active list 已刷新到数据库")
        delisted_raw=MassiveApi().get_all_tickers(active=False)
        if not delisted_raw.empty:
            delisted_data=UsStockUniverseModel.format_dataframe(delisted_raw)
            MarketDataRepo().insert_us_stock_universe(delisted_data)
            app_logger.info("全宇宙表delisted list 已刷新到数据库")
    
    
    def _main_loop(self):
        self.scheduler.add_job(
            self.fetch_universe_ticker,
            'cron',
            hour=8,
            minute=0,
            id='massive_scraping'
        )
    def start(self):
        self.load_stock_universe()
        self._main_loop()
        app_logger.info("✅ massive 搜刮器激活。")
        
    def stop(self):
       
        if self.scheduler:
            self.scheduler.remove_job('massive_scraping')
            
        app_logger.info("🛑 massive搜刮器 线程已退出。")    
    #本地获取美股全宇宙表采用无状态方式，启动时拉取并保存，之后美东早上8点定时拉取保存。    
    
    def load_stock_universe(self):
        app_logger.info("massive 开始同步全美股宇宙表")
        active_raw=MassiveApi().get_all_tickers()
        delisted_raw=MassiveApi().get_all_tickers(active=False)
        active_in_db=MarketDataRepo().get_active_tickers()
        delisted_in_db=MarketDataRepo().get_delisted_tickers()
        if (active_raw.empty or delisted_raw.empty) and ( active_in_db.empty or delisted_in_db.empty):
            app_logger.error("美股全宇宙表 api 无法查询，本地也无数据，程序退出")
            exit(1)
        active_data=UsStockUniverseModel.format_dataframe(active_raw)
        delisted_data=UsStockUniverseModel.format_dataframe(delisted_raw)
        MarketDataRepo().insert_us_stock_universe(active_data)
        MarketDataRepo().insert_us_stock_universe(delisted_data)
