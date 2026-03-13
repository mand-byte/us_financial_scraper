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

class ForexFactoryScraper:
    def __init__(self):
        self.db = None
        self._stop_event = threading.Event()
        self._thread = None
        self.indicators_map = ForexFactory_Indicator_Code
  
      
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
            if self._stop_event.is_set(): break
            
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
            
        # 计算下一次需要等待的公布时间 (Actual 为空的最近一条记录)
        # 过滤出监控列表内、且在当前时间之后的记录
        future_events = df_final[
            (df_final['publish_timestamp'] > now) | 
            (df_final['actual_value'].isna())
        ].sort_values('publish_timestamp')

        if not future_events.empty:
            next_event_time = future_events.iloc[0]['publish_timestamp']
            return next_event_time
        
        return None

    def get_next_et_midnight(self):
        """获取下一个美东时间 00:00 的 UTC 时间"""
        now_et = datetime.now(self.et_tz)
        tomorrow_et = (now_et + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return tomorrow_et.astimezone(pytz.UTC)

    def _main_loop(self):
        app_logger.info("🛡️ ForexFactory 业务调度子线程启动。")
        
        # 1. 先补全历史
        try:
            self.sync_history()
        except Exception as e:
            app_logger.error(f"🧨 历史同步异常: {e}")

        # 2. 进入监控循环
        while not self._stop_event.is_set():
            try:
                # 同步当月并获取下次唤醒时间
                next_event_time = self.sync_current_month()
                
                # 保底唤醒时间：美东午夜
                next_midnight = self.get_next_et_midnight()
                
                # 确定最终唤醒时间
                now = datetime.now(pytz.UTC)
                wake_up_time = next_midnight
                
                if next_event_time:
                    # 如果有即将到来的公布，则在公布后 3 秒唤醒
                    potential_wake_up = next_event_time + timedelta(seconds=3)
                    if now < potential_wake_up < next_midnight:
                        wake_up_time = potential_wake_up
                        app_logger.info(f"⏳ 预定下次公布抓取: {wake_up_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                else:
                    app_logger.info(f"😴 今日无剩余指标，保底美东午夜唤醒: {wake_up_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

                # 计算等待秒数
                wait_seconds = (wake_up_time - datetime.now(pytz.UTC)).total_seconds()
                if wait_seconds > 0:
                    # 分段休眠以响应停止事件
                    for _ in range(int(wait_seconds / 60) + 1):
                        if self._stop_event.is_set(): break
                        time.sleep(min(60, wait_seconds))
                        wait_seconds -= 60
                
            except Exception as e:
                app_logger.error(f"🧨 监控循环异常: {str(e)}")
                time.sleep(60) # 报错后等待一分钟重试

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        app_logger.info("✅ ForexFactory 生产级搜刮器已激活。")

    def stop(self):
        self._stop_event.set()
        if self._thread: self._thread.join()

if __name__ == "__main__":
    scraper = ForexFactoryScraper()
    scraper.sync_current_month()
