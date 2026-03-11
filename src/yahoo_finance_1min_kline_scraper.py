import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import random
import pytz
import threading
from src.utils.db_manager import ClickHouseManager
from src.utils.logger import app_logger

class YahooFinanceScraper:
    def __init__(self, chunk_size=40):
        self.db = None 
        self.tz_et = pytz.timezone('US/Eastern')
        self.chunk_size = chunk_size
        self._stop_event = threading.Event()
        self._thread = None

    def _init_db(self):
        if self.db is None:
            self.db = ClickHouseManager()

    def is_market_open(self):
        now_et = datetime.now(self.tz_et)
        if now_et.weekday() >= 5: return False
        market_start = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_end = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
        return market_start <= now_et <= market_end

    def fetch_batch_data(self, tickers, start_ts_map, figi_map):
        # yfinance 1分钟线的极限是过去 7 天 (保留一点余量)
        limit_date = datetime.now(pytz.UTC) - timedelta(days=6, hours=23)
        
        # 🌟 修复 3：正确计算 Chunk 的增量起跑线
        valid_starts = []
        for ticker in tickers:
            figi = figi_map.get(ticker)
            last_ts = start_ts_map.get(figi)
            if last_ts and not pd.isna(last_ts):
                if last_ts.tzinfo is None: 
                    last_ts = pytz.UTC.localize(last_ts)
                valid_starts.append(last_ts)
        
        if valid_starts:
            # 找到这批股票里最老的那个时间戳
            min_start = min(valid_starts)
            # 确保没有超出 yfinance 的 7 天极限，加 1 分钟避免拉取重复的最后一根K线
            min_start = max(min_start, limit_date) + timedelta(minutes=1)
        else:
            # 如果全都没数据，就从 7 天前开始
            min_start = limit_date
        
        # 如果起跑线已经超过了当前时间，说明数据已是最新，跳过
        if min_start >= datetime.now(pytz.UTC):
            return 0
        
        try:
            df_batch = yf.download(
                tickers=" ".join(tickers), 
                start=min_start, 
                interval='1m', 
                group_by='ticker', 
                auto_adjust=True, 
                progress=False,
                threads=True 
            )

            if df_batch.empty: return 0

            total_saved = 0
            for ticker in tickers:
                figi = figi_map.get(ticker)
                
                # 🌟 修复 2：防范 yfinance 的单股票降维崩溃 (MultiIndex 陷阱)
                if isinstance(df_batch.columns, pd.MultiIndex):
                    if ticker not in df_batch.columns.levels[0]: continue
                    df_ticker = df_batch[ticker].dropna(subset=['Close']).copy()
                else:
                    # 如果这批只拉取了 1 只股票，或者是 df_batch 降维了
                    if len(tickers) == 1 or ticker == tickers[0]:
                        df_ticker = df_batch.dropna(subset=['Close']).copy()
                    else:
                        continue
                
                if df_ticker.empty: continue

                # 严格裁剪：只保留大于库中最新时间的数据
                last_ts = start_ts_map.get(figi)
                if last_ts and not pd.isna(last_ts):
                    if last_ts.tzinfo is None: 
                        last_ts = pytz.UTC.localize(last_ts)
                    # 必须统一转为 UTC 比较
                    df_ticker.index = pd.to_datetime(df_ticker.index, utc=True)
                    df_ticker = df_ticker[df_ticker.index > last_ts]
                
                if df_ticker.empty: continue

                df_ticker = df_ticker.reset_index()
                # 兼容 yfinance 大小写问题
                col_map = {c: c.lower() for c in df_ticker.columns}
                col_map.update({'Datetime': 'timestamp', 'Date': 'timestamp'})
                df_ticker.rename(columns=col_map, inplace=True)
                
                df_ticker['timestamp'] = pd.to_datetime(df_ticker['timestamp'], utc=True)
                df_ticker['volume'] = df_ticker['volume'].fillna(0).astype('uint64')
                
                # 🌟 保险起见：强制注入主键，防止 save_klines 漏接
                df_ticker['composite_figi'] = figi
                
                self.db.save_klines(df_ticker, figi)
                total_saved += 1
                
            return total_saved

        except Exception as e:
            app_logger.error(f"❌ Yahoo 批量抓取异常: {str(e)}")
            return 0

    def run_sync_cycle(self):
        query = "SELECT ticker, composite_figi FROM us_stock_universe WHERE active = 1"
        df_u = self.db.client.query_df(query)
        if df_u.empty:
            app_logger.warning("⚠️ 股票宇宙为空，跳过同步。")
            return
            
        figi_map = dict(zip(df_u['ticker'], df_u['composite_figi']))
        tickers = list(figi_map.keys())
        
        total = len(tickers)
        app_logger.info(f"🚀 [Sync Cycle] 开始同步 {total} 只标的的分时增量...")
        
        for i in range(0, total, self.chunk_size):
            if self._stop_event.is_set(): break 

            chunk_tickers = tickers[i : i + self.chunk_size]
            chunk_figis = [figi_map[t] for t in chunk_tickers]
            
            # 🌟 修复 1：将查询结果缓存，避免重复查询 ClickHouse
            figi_list = "','".join(chunk_figis)
            q_ts = f"SELECT composite_figi, max(timestamp) as last_ts FROM us_stock_1min WHERE composite_figi IN ('{figi_list}') GROUP BY composite_figi"
            
            df_ts = self.db.client.query_df(q_ts)
            if not df_ts.empty:
                start_ts_map = dict(zip(df_ts['composite_figi'], df_ts['last_ts']))
            else:
                start_ts_map = {}
            
            # 执行抓取
            self.fetch_batch_data(chunk_tickers, start_ts_map, figi_map)
            
            # 动态休眠，防止被 Yahoo 封锁 IP
            time.sleep(random.uniform(1.2, 2.5))
            if (i // self.chunk_size) % 5 == 0:
                app_logger.info(f"📊 进度: {min(i + self.chunk_size, total)}/{total}")

    def _main_loop(self):
        """子线程执行的内部循环"""
        self._init_db()
        app_logger.info("🛡️ Yahoo 搜刮后台子线程已启动。")
        
        while not self._stop_event.is_set():
            now_et = datetime.now(self.tz_et)
            if now_et.weekday() >= 5:
                time.sleep(3600)
                continue

            try:
                self.run_sync_cycle()
            except Exception as e:
                app_logger.error(f"🧨 后台循环错误: {str(e)}")
                time.sleep(60)
                continue

            # 轮询间隔
            wait_time = 600 if self.is_market_open() else 3600
            # 分段休眠，以便能够快速响应停止信号
            for _ in range(wait_time // 10):
                if self._stop_event.is_set(): break
                time.sleep(10)

        if self.db: self.db.close()
        app_logger.info("🛑 Yahoo 搜刮后台子线程已平稳停止。")

    def start(self):
        """在子线程中启动搜刮器"""
        if self._thread is not None and self._thread.is_alive():
            app_logger.warning("⚠️ 搜刮器已在运行中。")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._main_loop, daemon=True)
        self._thread.start()
        app_logger.info("✅ 后台搜刮器已激活。")

    def stop(self):
        """停止后台搜刮器"""
        app_logger.info("⏳ 正在尝试停止后台搜刮器...")
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

if __name__ == "__main__":
    scraper = YahooFinanceScraper()
    scraper.start()
    
    try:
        while True:
            time.sleep(30)
    except KeyboardInterrupt:
        scraper.stop()
