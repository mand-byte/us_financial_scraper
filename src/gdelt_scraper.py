import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import time
from datetime import datetime
import pytz
import threading
from src.dao import SentimentRepo
from src.model import GdeltMacroSentimentModel
from apscheduler.schedulers.blocking import BlockingScheduler
from src.utils.logger import app_logger


# 每小时的0分，15分，30分，45分拉取一次。
class GDELTScraper:
    def __init__(self):
        self.tz_utc = pytz.UTC
        self._stop_event = threading.Event()
        self._thread = None
        self.master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
        
        # 核心关注的 CAMEO 根代码 (系统性风险)
        self.target_codes = ['16', '17', '18', '19', '20']



    def fetch_and_process_v2(self, file_url, timestamp_str):
        """处理 GDELT 2.0 15分钟增量文件并转换为智能加权聚合宽表记录"""
        filename = file_url.split('/')[-1]
        dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S").replace(tzinfo=pytz.UTC)
        
        try:
            r = requests.get(file_url, timeout=30)
            if r.status_code != 200: return 0
            
            # 1. 极速读取内存中的 ZIP 内容
            z = zipfile.ZipFile(io.BytesIO(r.content))
            csv_filename = z.namelist()[0]
            
            # 读取：26:EventRootCode, 30:GoldsteinScale, 31:NumMentions, 33:Confidence, 34:AvgTone
            df = pd.read_csv(z.open(csv_filename), sep='\t', header=None, 
                             usecols=[26, 30, 31, 33, 34], 
                             names=['EventRootCode', 'GoldsteinScale', 'NumMentions', 'Confidence', 'AvgTone'],
                             dtype={'EventRootCode': str})
            
            df_filtered = df[df['EventRootCode'].isin(self.target_codes)].copy()
            
            # 2. 准备宽表字典，初始值全为 0 (和平心跳)
            wide_record:dict = {'publish_timestamp': dt}
            for code in self.target_codes:
                wide_record[f'count_{code}'] = 0
                wide_record[f'tone_{code}'] = 0.0
                wide_record[f'impact_{code}'] = 0.0

            # 3. 👑 终极降噪加权魔法
            if not df_filtered.empty:
                # 计算智能权重：Log(1 + 报道数) * (置信度 / 100)
                df_filtered['Smart_Weight'] = np.log1p(df_filtered['NumMentions']) * (df_filtered['Confidence'] / 100.0)
                
                # 计算分子 (分数 × 智能权重)
                df_filtered['Weighted_Tone'] = df_filtered['AvgTone'] * df_filtered['Smart_Weight']
                df_filtered['Weighted_Impact'] = df_filtered['GoldsteinScale'] * df_filtered['Smart_Weight']
                
                # 分组聚合：对分子和分母（总智能权重）分别求和
                agg_df = df_filtered.groupby('EventRootCode').agg(
                    count=('EventRootCode', 'count'),                   # 发生的独立事件数
                    total_smart_weight=('Smart_Weight', 'sum'),         # 分母：总智能权重
                    sum_weighted_tone=('Weighted_Tone', 'sum'),         # 分子：加权情绪总和
                    sum_weighted_impact=('Weighted_Impact', 'sum')      # 分子：加权破坏力总和
                )
                
                # 遍历结果填入宽表，计算最终智能平均值
                for code, row in agg_df.iterrows():
                    total_sw = row['total_smart_weight']
                    wide_record[f'count_{code}'] = int(row['count'])
                    
                    if total_sw > 0:
                        # 算出真正的加权平均值：总加权得分 / 总智能权重
                        wide_record[f'tone_{code}'] = float(row['sum_weighted_tone'] / total_sw)
                        wide_record[f'impact_{code}'] = float(row['sum_weighted_impact'] / total_sw)
                    else:
                        wide_record[f'tone_{code}'] = 0.0
                        wide_record[f'impact_{code}'] = 0.0
            
            raw_df = pd.DataFrame([wide_record])

            to_save=GdeltMacroSentimentModel.format_dataframe(raw_df)  # 验证数据结构正确性
            
            
            SentimentRepo().insert_gdelt_macro_sentiment(to_save)
            
            return 1

        except Exception as e:
            app_logger.error(f"❌ GDELT 处理文件 {filename} 失败: {str(e)}")
            return 0

    def sync_v2_incremental(self):
        """同步 GDELT 2.0 增量数据"""
      
        start_ts = SentimentRepo().get_latest_gdelt_macro_sentiment()
        app_logger.info(f"🔄 GDELT 增量同步起点: {start_ts}")

        try:
            r = requests.get(self.master_url, timeout=20)
            if r.status_code != 200: return False
            
            lines = r.text.strip().split('\n')
            for line in lines:
                if self._stop_event.is_set(): break
                parts = line.split(' ')
                if len(parts) < 3: continue
                
                file_url = parts[2]
                if '.export.CSV.zip' not in file_url: continue
                
                file_ts_str = file_url.split('/')[-1].split('.')[0]
                try:
                    file_ts = datetime.strptime(file_ts_str, "%Y%m%d%H%M%S").replace(tzinfo=pytz.UTC)
                except: continue
                if file_ts > start_ts:
                    app_logger.info(f"📥 聚合 GDELT 开始下载文件: {file_ts_str}")
                    success = self.fetch_and_process_v2(file_url, file_ts_str)
                    if success:
                        start_ts = file_ts
                    else:
                        app_logger.warning(f"⚠️ GDELT 文件下载失败: {file_ts_str}， 10秒后再试一次")
                        time.sleep(10)
                        success=self.fetch_and_process_v2(file_url, file_ts_str)
                        start_ts = file_ts 
                        if not success:
                            app_logger.error(f"🧨 GDELT 文件重试失败: {file_ts_str}，跳过这个文件")

        except Exception as e:
            app_logger.error(f"❌ GDELT 获取列表失败: {str(e)}")
            
        
    def _checking_data_complementation(self):
        self.sync_v2_incremental()
        
    
    def _main_loop(self):
        app_logger.info("🛡️ GDELT 2.0 聚合搜刮子线程启动。")
        self._checking_data_complementation()
        self.scheduler = BlockingScheduler(timezone='US/Eastern')
        self.scheduler.add_job(
            self.sync_v2_incremental, 
            'cron', 
            minute="*/15", 
            id='15min_gdelt_scraping',
            coalesce=True            
        )
        # 👇 必须加上这行，阻塞当前子线程并开始调度！
        self.scheduler.start()
      

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        app_logger.info("✅ GDELT 聚合搜刮器激活。")

    def stop(self):
        self._stop_event.set()
        if self.scheduler:
            try:
                self.scheduler.shutdown(wait=False)
                app_logger.info("🛑 GDELT 聚合搜刮器 调度器已关闭。")
            except Exception as e:
                app_logger.error(f"⚠️ 关闭 GDELT 聚合搜刮器 调度器时出错: {e}")
        if self._thread:
            self._thread.join(timeout=5)
            app_logger.info("🛑 GDELT 聚合搜刮器 子线程已退出。")

if __name__ == "__main__":
    scraper = GDELTScraper()
    scraper.sync_v2_incremental()
