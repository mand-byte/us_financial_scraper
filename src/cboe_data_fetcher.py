# -*- coding: utf-8 -*-
#此代码用来拉取Cobe数据，并将其存入ClickHouse数据库中。它使用了apscheduler库来定时执行数据抓取任务，确保每天晚上9点（美东时间）自动更新VIX现货、VX1和VX2的数据。代码还包含了线程管理和错误处理机制，以确保数据抓取过程的稳定性和可靠性。
# 主要功能包括：
# 1. 定义CboeDataFetcher类，负责管理数据抓取和
#    数据库交互。
# 2. 使用apscheduler库设置定时任务，每天晚上9点执行数据抓取。
# 3. 从CBOE官方库获取VIX现货和期货数据，并进行数据透视处理。
# 4. 将抓取到的数据与数据库中现有数据进行比较，确保只插入新的数据。
# 5. 提供启动和停止数据抓取线程的方法，允许在需要时手动控制数据抓取过程
import pandas as pd
from datetime import datetime
from src.utils import *
import pytz
import threading
import time
from src.model.us_macro_daily_kline_model import UsMacroDailyKlineModel
from apscheduler.schedulers.blocking import BlockingScheduler
from src.utils.logger import app_logger
from src.dao import MarketDataRepo
from src.utils.constants import CBOE_Indicator_Code
class CboeDataFetcher:
    def __init__(self):
        self.mapping = CBOE_Indicator_Code
        self.tz_utc = pytz.UTC
        self._stop_event = threading.Event()
        self._thread = None
        self.scheduler = None

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        app_logger.info("✅ Cobe 数据拉取线程已启动。")

    def stop(self):
        self._stop_event.set()
        if self.scheduler:
            try:
                self.scheduler.shutdown(wait=False)
                app_logger.info("🛑 Cobe 调度器已关闭。")
            except Exception as e:
                app_logger.error(f"⚠️ 关闭 Cobe 调度器时出错: {e}")
        if self._thread:
            self._thread.join(timeout=5)
            app_logger.info("🛑 Cobe 子线程已退出。")

    def _main_loop(self):
        app_logger.info("🛡️ Cobe 数据拉取线程子线程启动。")
        # 使用美东时间
        self.scheduler = BlockingScheduler(timezone='US/Eastern')
        
        # 设置 cron 任务：美东时间晚上 9 点 (21:00)
        # 初始运行一次以补齐数据
        self.scheduler.add_job(
            self.scraping, 
            'cron', 
            hour=21, 
            minute=0, 
            id='daily_vix_scraping',
            coalesce=True            # 如果多次错过只运行一次
        )
        self.scraping()
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass
        except Exception as e:
            app_logger.error(f"❌ Cobe 调度器运行崩溃: {e}")

    def scraping(self):
        """调度器执行的具体抓取任务"""
        app_logger.info("🚀 开始执行 CBOE VIX 数据抓取任务...")
        try:
            df = self.fetch_vix_data()
            if not df.empty:
                app_logger.info(f"📊 成功抓取 VIX 数据，共 {len(df)} 行，准备入库...")
                MarketDataRepo().insert_macro_daily_klines(df)
                app_logger.info("✅ VIX 数据入库成功。")
            else:
                app_logger.info("ℹ️ 未发现需要更新的 VIX 数据。")
        except Exception as e:
            app_logger.error(f"❌ Cobe 抓取任务执行失败: {e}", exc_info=True)

    def fetch_vix_data(self) -> pd.DataFrame:
        """从 CBOE 获取数据并根据数据库最后日期进行增量对比"""
        app_logger.info("📡 正在调用 cobe_scraper 接口获取数据...")
        
        # 1. 获取数据库中已有的最后日期 (针对 VIX 相关 symbol)
        # 注意：这里假设你的数据库表里 symbol 存的是 'VX1', 'VX2'

        res=MarketDataRepo().get_latest_trade_date_in_macro_daily_klines(self.mapping)
        last_db_date = datetime.strptime(res, "%Y-%m-%d").date()
            
        futures_df=build_vx_continuous(start_date=last_db_date, end_date=datetime.now(pytz.timezone('US/Eastern')).date())
        futures_df = UsMacroDailyKlineModel.format_dataframe(futures_df)
        return futures_df
    
if __name__ == "__main__":
    
    
    fetcher = CboeDataFetcher()                                                             
    fetcher.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        fetcher.stop()
