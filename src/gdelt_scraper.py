import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import time
from datetime import datetime, timedelta
import pytz
import threading
from dotenv import load_dotenv
from src.utils.db_manager import ClickHouseManager
from src.utils.logger import app_logger

load_dotenv()

# 每小时的0分，15分，30分，45分拉取一次。
class GDELTScraper:
    def __init__(self):
        self.db = None
        self.tz_utc = pytz.UTC
        self._stop_event = threading.Event()
        self._thread = None
        self.master_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
        
        # 核心关注的 CAMEO 根代码 (系统性风险)
        self.target_codes = ['16', '17', '18', '19', '20']
        
        # 配置读取逻辑
        self.start_date_str = os.getenv("SCRAPING_START_DATE", "2014-01-01")

    def _init_db(self):
        if self.db is None:
            self.db = ClickHouseManager()

    def _get_start_timestamp(self):
        """获取抓取起点：库中最后时间戳 -> .env 配置 -> 默认值"""
        query = "SELECT max(publish_timestamp) as last_ts FROM gdelt_macro_sentiment"
        res = self.db.client.query_df(query)
        last_ts = res.iloc[0]['last_ts']
        
        if last_ts and not pd.isna(last_ts):
            return last_ts.replace(tzinfo=pytz.UTC)
        
        env_start = os.getenv("SCRAPING_START_DATE")
        if env_start:
            try:
                return datetime.strptime(env_start, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
            except:
                pass
        
        return datetime.strptime(self.start_date_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)

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
            wide_record = {'publish_timestamp': dt}
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

            # 4. 写入数据库 (单行写入)
            to_save = pd.DataFrame([wide_record])
            
            # 映射字段顺序以匹配 ClickHouse DDL
            cols_order = ['publish_timestamp']
            for code in self.target_codes:
                cols_order += [f'count_{code}', f'tone_{code}', f'impact_{code}']
            
            self.db.client.insert_df('gdelt_macro_sentiment', to_save[cols_order])
            return 1

        except Exception as e:
            app_logger.error(f"❌ GDELT 处理文件 {filename} 失败: {str(e)}")
            return 0

    def sync_v2_incremental(self):
        """同步 GDELT 2.0 增量数据"""
        self._init_db()
        start_ts = self._get_start_timestamp()
        app_logger.info(f"🔄 GDELT 增量同步起点: {start_ts}")

        try:
            r = requests.get(self.master_url, timeout=20)
            if r.status_code != 200: return False
            
            lines = r.text.strip().split('\n')
            last_file_ts = start_ts
            
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
                    app_logger.info(f"📥 聚合 GDELT 智能加权宽行: {file_ts_str}")
                    success = self.fetch_and_process_v2(file_url, file_ts_str)
                    if success:
                        last_file_ts = file_ts
                    time.sleep(0.5)

            now_utc = datetime.now(pytz.UTC)
            return (now_utc - last_file_ts).total_seconds() < 1800 

        except Exception as e:
            app_logger.error(f"❌ GDELT 获取列表失败: {str(e)}")
            return False

    def _main_loop(self):
        app_logger.info("🛡️ GDELT 2.0 聚合搜刮子线程启动。")
        while not self._stop_event.is_set():
            try:
                is_caught_up = self.sync_v2_incremental()
                if is_caught_up:
                    app_logger.info("😴 GDELT 已补齐智能加权数据，休眠 15 分钟...")
                    for _ in range(15): 
                        if self._stop_event.is_set(): break
                        time.sleep(60)
                else:
                    app_logger.info("🚀 GDELT 历史智能加权进行中...")
                    time.sleep(5)
            except Exception as e:
                app_logger.error(f"🧨 GDELT 调度异常: {str(e)}")
                time.sleep(60)

    def start(self):
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        app_logger.info("✅ GDELT 聚合搜刮器激活。")

    def stop(self):
        self._stop_event.set()
        if self._thread: self._thread.join()

if __name__ == "__main__":
    scraper = GDELTScraper()
    scraper.sync_v2_incremental()
