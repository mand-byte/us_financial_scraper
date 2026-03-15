import time
import os
import pandas as pd
import pytz
import threading
from datetime import datetime, timedelta

from src.utils.logger import app_logger
from src.utils.constants import ForexFactory_Indicator_Code
from src.utils.forexfactory_scraper.scraper import scrape_month
from src.dao.market_data_repo import MarketDataRepo
from src.model.us_macro_indicators_model import UsMacroIndicatorsModel
from apscheduler.schedulers.blocking import BlockingScheduler
class ForexFactoryScraper:
    def __init__(self,scheduler:BlockingScheduler):
        self.db = None
        self.indicators_map = ForexFactory_Indicator_Code
        self.scheduler=scheduler
      
        self.et_tz = pytz.timezone("America/New_York")



    def _clean_value(self, val_str):
        """清洗数值字符串 (如 2.5%, 450K, 1.2M -> 2.5, 450000, 1200000)"""
        if not val_str or val_str.strip() == "":
            return None
        clean_val = str(val_str).replace('%', '').replace(',', '').strip()
        try:
            if clean_val.endswith('K'): return float(clean_val[:-1]) * 1000
            if clean_val.endswith('M'): return float(clean_val[:-1]) * 1000000
            if clean_val.endswith('B'): return float(clean_val[:-1]) * 1000000000
            return float(clean_val)
        except ValueError:
            return None

    def process_scraped_data(self, df_scraped):
        """清洗并准备入库 (基于 EventID 映射)"""
        if df_scraped.empty: return pd.DataFrame()
        
        processed_list = []
        for _, row in df_scraped.iterrows():
            eid = str(row['EventID']) if row['EventID'] else None
            if not eid or eid not in self.indicators_map:
                continue
            
            actual_val = self._clean_value(row['Actual'])
            forecast_val = self._clean_value(row['Forecast'])
            
            processed_list.append({
                "publish_timestamp": row['DateTime'], 
                "indicator_code": self.indicators_map[eid],
                "actual_value": actual_val,
                "expected_value": forecast_val
            })
            
        return pd.DataFrame(processed_list)

    def sync_history(self):
        """同步历史数据：从数据库最后一次记录到上个月底"""
        start_dt=MarketDataRepo().get_latest_macro_indicators(self.indicators_map)

        now = datetime.now(pytz.UTC)
        # 补齐到上个月底（为了简单起见，这里按月步进）
        current = start_dt
        while current.year < now.year or current.month < now.month:
            
            month_label = current.strftime('%b').lower()
            app_logger.info(f"📅 正在同步历史月份: {current.year}-{month_label}")
            df_raw = scrape_month(month_label, current.year)
            
            if not df_raw.empty:
                df_final = self.process_scraped_data(df_raw)
                if not df_final.empty:
                    MarketDataRepo().insert_marco_indicators(df_final)
            # 步进到下个月
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)
            time.sleep(2)

    def sync_current_month(self):
        """同步当月数据并返回下一次公布时间"""
     
        now = datetime.now(pytz.UTC)
        month_label = now.strftime('%b').lower()
        
        app_logger.info(f"🔎 正在扫描/更新本月数据 ({now.year}-{month_label})...")
        df_raw = scrape_month(month_label, now.year)
        
        if df_raw.empty:
            return None

        # 清洗并入库 (ReplacingMergeTree 会处理冲突/更新)
        df_final = self.process_scraped_data(df_raw)
        if not df_final.empty:
            df=UsMacroIndicatorsModel.format_dataframe(df_final)  # 验证数据结构正确性
            MarketDataRepo().insert_marco_indicators(df)
       


    def _main_loop(self):
        app_logger.info("🛡️ ForexFactory 业务调度启动。")
        self.sync_history()
        self.scheduler.add_job(
            self.sync_current_month, 
            'cron', 
            hour=21, 
            minute=0,
            id='daily_forexfactory_scraping'
        )
    def start(self):
        self._main_loop()
        app_logger.info("✅ ForexFactory 生产级搜刮器已激活。")

    def stop(self):
        if self.scheduler:
            self.scheduler.remove_job('daily_forexfactory_scraping')

        app_logger.info("🛑 ForexFactory 生产级搜刮器 已退出。")


