# -*- coding: utf-8 -*-
"""
================================================================================
Massive 财务基本面搜刮器 (MassiveFundamentalScraper) - 需求与逻辑文档
================================================================================

[核心需求 - 已实现]
1. 数据源: 
   - Massive API Financials v1 (Income, Balance, Cash Flow).
   - 模式: 三表合一 PIT (Point-in-Time) 合并。
2. 状态追踪 (Smart Sync):
   - 垂直解耦: 使用 `us_stock_fundamentals_state` 表追踪退市标的完成状态。
   - 退市截断: 严格根据 `delisted_date` 截断数据，不处理退市后的冗余信息。
   - 活跃增量: 活跃标的始终执行增量同步，冷启动后不标记状态（始终保持增量探测）。
3. 计算逻辑:
   - 自动计算 EPS, ROE, 营收/净利 YoY, 自由现金流, 债务权益比, 流动比率。
   - 向量化处理: 使用 UsStockFundamentalsModel.format_dataframe 进行工业级清洗与入库。
4. 调度策略:
   - 美东时间每日 03:00 自动执行。
================================================================================
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
import pytz
from src.api.massive_api import MassiveApi
from src.dao.fundamental_repo import FundamentalRepo
from src.dao.market_data_repo import MarketDataRepo
from src.model.us_stock_fundamental_model import UsStockFundamentalsModel
from src.utils.logger import app_logger
import concurrent.futures
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from zoneinfo import ZoneInfo

class MassiveFundamentalScraper:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: Optional[BlockingScheduler] = None):
        self.api = MassiveApi()
        self.fundamental_repo = FundamentalRepo()
        self.market_repo = MarketDataRepo()
        self.scheduler = scheduler

    def start(self):
        """
        Register the fundamental sync task with the scheduler.
        """
        if self.scheduler:
            # Schedule daily sync at 03:00 AM NYC time
            self.scheduler.add_job(
                self.run, 'cron', hour=3, minute=0, timezone=self.NYC, id="daily_fundamental_sync"
            )
            
            # Also trigger an initial sync on startup
            self.scheduler.add_job(
                self.run, 
                next_run_time=datetime.now(self.NYC), 
                id="initial_fundamental_sync"
            )
            app_logger.info("✅ Massive Fundamental Scraper started (Daily at 03:00 NYC + Initial Sync).")

    def stop(self):
        """
        Stop the scraper (placeholder for consistency).
        """
        app_logger.info("🛑 Massive Fundamental Scraper stopping...")

    def _calculate_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate financial ratios and YoY growth using vectorized pandas operations.
        """
        if df.empty:
            return df
        
        # Ensure period_end is datetime for offset calculations
        df['period_end_dt'] = pd.to_datetime(df['period_end'])
        df = df.sort_values('period_end_dt')

        # EPS from income statement (basic_earnings_per_share)
        df['eps'] = df.get('basic_earnings_per_share', 0.0).fillna(0.0)

        # YoY Growth Calculation
        df_prev = df[['period_end_dt', 'revenue', 'consolidated_net_income_loss']].copy()
        df_prev['period_end_dt'] = df_prev['period_end_dt'] + pd.DateOffset(years=1)
        df_prev = df_prev.rename(columns={
            'revenue': 'prev_revenue',
            'consolidated_net_income_loss': 'prev_net_income'
        })
        
        df = pd.merge(df, df_prev, on='period_end_dt', how='left')
        
        df['revenue_growth_yoy'] = (df['revenue'] - df['prev_revenue']) / df['prev_revenue'].abs()
        df['net_income_growth_yoy'] = (df['consolidated_net_income_loss'] - df['prev_net_income']) / df['prev_net_income'].abs()
        
        for col in ['revenue_growth_yoy', 'net_income_growth_yoy']:
            df[col] = df[col].replace([float('inf'), float('-inf')], 0.0).fillna(0.0)

        df['roe'] = df['consolidated_net_income_loss'] / df['total_equity'].abs()
        df['roe'] = df['roe'].replace([float('inf'), float('-inf')], 0.0).fillna(0.0)

        df['free_cash_flow'] = df.get('net_cash_from_operating_activities', 0.0).fillna(0.0) + \
                               df.get('purchase_of_property_plant_and_equipment', 0.0).fillna(0.0)

        df['debt_to_equity'] = df['total_liabilities'] / df['total_equity'].abs()
        df['debt_to_equity'] = df['debt_to_equity'].replace([float('inf'), float('-inf')], 0.0).fillna(0.0)

        df['current_ratio'] = df['total_current_assets'] / df['total_current_liabilities'].abs()
        df['current_ratio'] = df['current_ratio'].replace([float('inf'), float('-inf')], 0.0).fillna(0.0)

        return df

    def sync_cik(self, task_data: tuple):
        """
        Synchronize fundamental data for a single CIK.
        task_data: (cik, delisted_date, composite_figi, active, sync_state)
        """
        cik, delisted_date, composite_figi, active, sync_state = task_data
        if not cik:
            return

        try:
            # 1. 判定边界与状态
            # 已退市且标记完成的，直接跳过
            if not active and sync_state == 1:
                return

            last_ts = self.fundamental_repo.get_latest_fundamental_timestamp(cik)
            
            if not active and pd.notna(delisted_date):
                delisted_dt = pd.to_datetime(delisted_date).replace(tzinfo=pytz.UTC)
                # 数据时间 + 8小时 >= 下市时间 就改 state 标记并跳过
                if last_ts + timedelta(hours=8) >= delisted_dt:
                    self.market_repo.update_sync_status('us_stock_fundamentals', cik, 'cik', 1)
                    return
            else:
                delisted_dt = None

            # 2. 确定抓取起始时间
            scraping_start_env = os.getenv("SCRAPING_START_DATE", "2014-01-01")
            scraping_start_dt = datetime.strptime(scraping_start_env, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
            
            # 取 (最后同步时间, 全局开始时间) 的最大值，并回溯 500 天以确保 YoY 计算有历史对比
            effective_start_dt = max(last_ts, scraping_start_dt) - timedelta(days=500)
            start_date_str = effective_start_dt.strftime("%Y-%m-%d")
            
            # 3. 抓取三表
            inc_df = self.api.get_income_statements(cik, filing_date=start_date_str)
            bal_df = self.api.get_balance_sheets(cik, filing_date=start_date_str)
            cf_df = self.api.get_cashflow_statements(cik, filing_date=start_date_str)
            
            if inc_df.empty and bal_df.empty and cf_df.empty:
                # 如果没抓到任何数据，对于退市标的标记为完成
                if not active:
                    self.market_repo.update_sync_status('us_stock_fundamentals', cik, 'cik', 1)
                return

            # 4. PIT 合并与时间对齐
            if not inc_df.empty: inc_df = inc_df.rename(columns={'filing_date': 'filing_date_inc'})
            if not bal_df.empty: bal_df = bal_df.rename(columns={'filing_date': 'filing_date_bal'})
            if not cf_df.empty: cf_df = cf_df.rename(columns={'filing_date': 'filing_date_cf'})
            
            merged_df = pd.DataFrame(columns=['period_end'])
            if not inc_df.empty:
                merged_df = inc_df
            if not bal_df.empty:
                merged_df = pd.merge(merged_df, bal_df, on='period_end', how='outer') if not merged_df.empty else bal_df
            if not cf_df.empty:
                merged_df = pd.merge(merged_df, cf_df, on='period_end', how='outer') if not merged_df.empty else cf_df
            
            if merged_df.empty: 
                if not active: self.market_repo.update_sync_status('us_stock_fundamentals', cik, 'cik', 1)
                return

            # 提取发布时间戳 (PIT)
            filing_cols = [col for col in ['filing_date_inc', 'filing_date_bal', 'filing_date_cf'] if col in merged_df.columns]
            merged_df['publish_timestamp'] = merged_df[filing_cols].max(axis=1)
            merged_df['publish_timestamp'] = pd.to_datetime(merged_df['publish_timestamp']).dt.tz_localize('UTC')
            
            # 严格截断退市后的数据
            if delisted_dt:
                merged_df = merged_df[merged_df['publish_timestamp'] <= delisted_dt]
            
            if merged_df.empty:
                # 退市标的一旦数据被截断为空，标记完成
                if not active: self.market_repo.update_sync_status('us_stock_fundamentals', cik, 'cik', 1)
                return

            # 5. 计算指标与入库
            merged_df = self._calculate_ratios(merged_df)
            final_df = merged_df[merged_df['publish_timestamp'] > last_ts].copy()
            
            if not final_df.empty:
                insert_df = UsStockFundamentalsModel.format_dataframe(final_df, cik)
                if not insert_df.empty:
                    self.fundamental_repo.insert_stock_fundamentals(insert_df)
                    app_logger.info(f"CIK {cik}: 插入 {len(insert_df)} 条新财报记录。")

            # 6. 状态更新：仅针对已退市标的，标记为已完成
            if not active:
                 self.market_repo.update_sync_status('us_stock_fundamentals', cik, 'cik', 1)

        except Exception as e:
            app_logger.error(f"CIK {cik} 同步异常: {e}")

    def run(self, max_workers: int = 10):
        """
        Entry point for the synchronization task.
        """
        app_logger.info("Starting Massive Fundamental Sync Task (Smart State Sync)...")
        
        # 🌟 使用 JOIN 获取任务列表，Fundamentals 以 CIK 为关联键
        tasks_df = self.market_repo.get_sync_tasks('us_stock_fundamentals', id_column='cik')
        if tasks_df.empty:
            app_logger.warning("Universe is empty, nothing to sync.")
            return

        # 过滤规则：拉模式，活跃标的全部拉取；退市标的仅 sync_state == 0 拉取
        sync_mask = (tasks_df['active'] == 1) | ((tasks_df['active'] == 0) & (tasks_df['sync_state'] == 0))
        filtered_tasks = tasks_df[sync_mask].dropna(subset=['cik']).drop_duplicates(subset=['cik'])
        
        task_list = list(filtered_tasks[['cik', 'delisted_date', 'composite_figi', 'active', 'sync_state']].itertuples(index=False, name=None))
        
        app_logger.info(f"Identified {len(task_list)} CIK tasks (Active + Unfinished Delisted).")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self.sync_cik, task_list)

        app_logger.info("Massive Fundamental Sync Task Completed.")

if __name__ == "__main__":
    scraper = MassiveFundamentalScraper()
    scraper.run()
