# -*- coding: utf-8 -*-
"""
================================================================================
OpenInsider 内部人士交易搜刮器 (OpenInsiderScraper)
================================================================================

[核心需求 - 已实现]
1. 数据源: OpenInsider.com (Form 4 聚合).
2. 状态追踪 (Smart Sync):
   - 垂直解耦: 使用 `us_stock_insider_trades_state` 表管理退市标的同步进度。
   - 滚动重刷: 针对活跃标的，每日扫描全市场最新 Filing，实现 O(1) 增量更新。
   - 退市处理: 针对未完成的退市标的，拉取 10 年历史至 delisted_date。
3. 数据特性:
   - 包含真实的公开市场买卖 (P/S)，自动过滤非交易性噪音。
   - 保留精确的 Filing Timestamp (PIT) 用于回测。
4. 调度周期:
   - 每日 21:00 NYC 执行。
================================================================================
"""

import pandas as pd
import requests
import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pytz
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from src.dao.fundamental_repo import FundamentalRepo
from src.dao.market_data_repo import MarketDataRepo
from src.model.us_stock_insider_trades_model import UsStockInsiderTradesModel
from src.utils.logger import app_logger as logger
import os
import concurrent.futures
import time

class OpenInsiderScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: Optional[BlockingScheduler] = None):
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def start(self):
        if self.scheduler:
            # 每日同步最新全市场 Filing
            self.scheduler.add_job(
                self.sync_daily_filings, 
                'cron', 
                hour=21, 
                minute=0, 
                timezone=self.NYC, 
                id="daily_insider_sync"
            )
            # 启动时触发一次
            self.scheduler.add_job(
                self.run_backfill, 
                next_run_time=datetime.now(self.NYC), 
                id="insider_initial_backfill"
            )
            logger.info("✅ Massive 内部人交易搜刮器已启动。")

    def stop(self):
        logger.info("🛑 Massive 内部人交易搜刮器停止。")

    def _fetch_from_openinsider(self, params_str: str) -> pd.DataFrame:
        url = f"http://openinsider.com/screener?{params_str}"
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            
            dfs = pd.read_html(io.StringIO(response.text))
            for table in dfs:
                table.columns = [c.replace('\xa0', ' ') if isinstance(c, str) else c for c in table.columns]
                if 'Ticker' in table.columns and 'Trade Date' in table.columns:
                    return table.dropna(subset=['Ticker', 'Trade Date'])
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"OpenInsider 请求失败: {e}")
            return pd.DataFrame()

    def sync_daily_filings(self):
        """
        增量同步：拉取全市场最近 30 天的 Filing。
        这种方式极其高效，不需要分 ticker 抓取，且 30 天窗口能完美覆盖程序停机导致的断层。
        """
        logger.info("🚀 正在同步全市场最新内部人交易 Filing (滚动 30 天)...")
        # fd=30 表示过去 30 天, cnt=1000 确保能覆盖 (通常一个月全市场几千条)
        # 为防止一页装不下，实际生产中可以加入翻页逻辑，但为了精简，我们扩大 limit 假设或者直接抓几页
        
        # 抓取第一页
        params = "ft=all&fd=30&lo=1&sc=1&sortcol=0&cnt=5000&page=1"
        df_raw = self._fetch_from_openinsider(params)
        
        if df_raw.empty: return

        # 获取 figi 映射
        universe = self.market_repo.get_universe_tickers()
        figi_map = dict(zip(universe["ticker"], universe["composite_figi"]))

        # 格式化并入库
        clean_df = UsStockInsiderTradesModel.format_dataframe(df_raw)
        # 注入 FIGI
        clean_df['composite_figi'] = clean_df['ticker'].map(figi_map)
        clean_df = clean_df.dropna(subset=['composite_figi'])

        if not clean_df.empty:
            self.fundamental_repo.insert_stock_insider_trades(clean_df)
            logger.info(f"全市场最新同步完成: 插入 {len(clean_df)} 条记录。")

    def backfill_ticker_history(self, task_data: tuple):
        """针对特定标的的回补逻辑 (冷启动 10 年历史)"""
        ticker, composite_figi, delisted_date, active, sync_state = task_data
        try:
            # 1. 判定边界
            # 已退市且标记完成的，直接跳过
            if not active and sync_state == 1:
                return

            last_ts = self.fundamental_repo.get_latest_insider_trade_filing(composite_figi)
            
            if not active and pd.notna(delisted_date):
                delisted_dt = pd.to_datetime(delisted_date).replace(tzinfo=pytz.UTC)
                # 数据时间 + 8小时 >= 下市时间 就改 state 标记并跳过
                if last_ts + timedelta(hours=8) >= delisted_dt:
                    self.market_repo.update_sync_status('us_stock_insider_trades', composite_figi, 'composite_figi', 1)
                    return
            else:
                delisted_dt = None

            # 活跃的只用读最后一行的数据时间
            # 如果最后记录时间在 30 天以内，日常增量 (sync_daily_filings) 可以覆盖，无需回补
            if active and last_ts >= datetime.now(pytz.UTC) - timedelta(days=30):
                return

            # 2. 抓取 (fd=0 为所有历史)
            logger.debug(f"正在补齐 {ticker} 的 Insider 历史...")
            params = f"s={ticker}&fd=0&lo=1&sc=1&sortcol=0&cnt=5000&page=1"
            df_raw = self._fetch_from_openinsider(params)
            
            if df_raw.empty:
                # 数据没了，对于下市标的改 state 标记
                if not active: 
                    self.market_repo.update_sync_status('us_stock_insider_trades', composite_figi, 'composite_figi', 1)
                return

            # 3. 格式化与入库
            clean_df = UsStockInsiderTradesModel.format_dataframe(df_raw, composite_figi)
            
            # 过滤掉已存在和超过退市日期的
            clean_df = clean_df[clean_df['filing_timestamp'] > last_ts]
            if delisted_dt:
                clean_df = clean_df[clean_df['filing_timestamp'] <= delisted_dt]

            if not clean_df.empty:
                self.fundamental_repo.insert_stock_insider_trades(clean_df)
            
            # 4. 标记退市完成
            if not active:
                self.market_repo.update_sync_status('us_stock_insider_trades', composite_figi, 'composite_figi', 1)
                
            time.sleep(0.5) # 礼貌抓取

        except Exception as e:
            logger.error(f"[{ticker}] Insider 回补异常: {e}")

    def run_backfill(self):
        """启动全量补数任务"""
        logger.info("🚀 启动内部人交易历史补齐 (Backfill)...")
        tasks_df = self.market_repo.get_sync_tasks('us_stock_insider_trades', id_column='composite_figi')
        if tasks_df.empty: return

        # 活跃的只用读最后一行的数据时间，非活跃继续下载读 _state 的状态
        sync_mask = (tasks_df['active'] == 1) | ((tasks_df['active'] == 0) & (tasks_df['sync_state'] == 0))
        filtered_tasks = tasks_df[sync_mask].dropna(subset=['ticker'])
        
        task_list = [
            (r.ticker, r.composite_figi, r.delisted_date, r.active, r.sync_state) 
            for r in filtered_tasks.itertuples(index=False)
        ]

        
        logger.info(f"Insider 补齐任务: 准备处理 {len(task_list)} 个标的。")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self.backfill_ticker_history, task_list)
        
        logger.info("✅ 内部人交易补齐任务结束。")

if __name__ == "__main__":
    scraper = OpenInsiderScraper()
    scraper.sync_daily_filings()
