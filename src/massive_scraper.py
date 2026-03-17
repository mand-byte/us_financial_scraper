"""
================================================================================
Massive K线搜刮器 (MassiveDataFetcher) - 需求与逻辑文档
================================================================================

[核心需求 - 已实现]
1. 数据源: 
   - Massive API v2 Aggs (get_historical_klines).
   - 模式: 流式抓取 (Generator)，逐页入库，支持 10 年补数。
2. 状态追踪 (Smart Sync):
   - 垂直解耦: 使用 `us_minutes_klines_state` 表存储退市标的的完成状态。
   - 效率优化: 彻底取代“全表聚合探测”模式，实现毫秒级任务分发。
   - 退市处理: 一旦退市标的数据补齐至 `delisted_date`，永久标记为完成。
3. 数据规范:
   - Model-First: 所有 K 线入库前必须通过 UsStockMinutesKlineModel 格式化。
   - 包含盘前与盘后数据。
4. 调度任务:
   - 宇宙表同步: 美东 01:00 和 09:00。
   - K线同步: 美东 19:00 (每日增量)。
================================================================================
"""

import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from src.api.massive_api import MassiveApi
from src.model.us_stock_minutes_kline_model import UsStockMinutesKlineModel
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger as logger
import os

class MassiveDataFetcher:
    NYC = ZoneInfo("America/New_York")

    def __init__(self, scheduler: BlockingScheduler):
        self.massive = MassiveApi()
        self.repo = MarketDataRepo()
        self.scheduler = scheduler
        self.KLINE_SPAN = int(os.getenv("KLINE_SPAN", 5))
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2021-01-01")

    def start(self):
        """异步注册与高频调度启动"""
        # 1. 宇宙表高频同步 (美东 01:00 和 09:00, 周一至周五)
        for hour in [1, 9]:
            self.scheduler.add_job(
                self.load_stock_universe, 'cron', day_of_week='mon-fri', 
                hour=hour, minute=0, timezone=self.NYC, id=f"universe_sync_{hour}"
            )

        # 2. 🌟 每日滚动重刷 (每天一次，每次回溯 3 天)
        self.scheduler.add_job(
            self.refresh_recent_klines, 
            'cron', 
            hour=20, 
            minute=0, 
            timezone=self.NYC, 
            id="rolling_kline_refresh"
        )

        # 3. 启动时立即执行一次
        self.scheduler.add_job(
            self.refresh_recent_klines, 
            next_run_time=datetime.now(self.NYC), 
            id="rolling_kline_refresh",
            replace_existing=True
        )
        logger.info("✅ Massive K线搜刮器已启动 (单日滚动重刷模式: 3 days)。")

    def load_stock_universe(self):
        """
        同步全美股宇宙表。
        逻辑增强：通过对比库内 active 列表与 API 返回的 delisted 列表，识别变动的退市/私有化事件。
        """
        logger.info("Massive: 正在执行宇宙表例行同步并探测退市事件...")
        try:
            # 1. 获取最新活跃和不活跃清单
            active_raw = self.massive.get_all_tickers(active=True)
            delisted_raw = self.massive.get_all_tickers(active=False)
            
            if active_raw.empty and delisted_raw.empty: return

            # 2. 差分探测：谁从活跃列表中消失了，并且出现在了退市列表中？
            db_active = self.repo.get_active_tickers()
            if not db_active.empty and not delisted_raw.empty:
                # 找出在数据库中活跃但在最新活跃清单中消失的 figi
                missing_figis = set(db_active['composite_figi']) - set(active_raw['composite_figi'])
                
                for figi in missing_figis:
                    ticker = db_active[db_active['composite_figi'] == figi]['ticker'].iloc[0]
                    # 检查它是否出现在了退市列表中
                    delisted_info = delisted_raw[delisted_raw['composite_figi'] == figi]
                    if not delisted_info.empty:
                        delisted_date = delisted_info['delisted_date'].iloc[0]
                        logger.warning(f"🚨 检测到标的退市/私有化: [{ticker}] ({figi})。退市时间: {delisted_date}")
                    else:
                        logger.warning(f"🚨 检测到标的消失: [{ticker}] ({figi})，但未在退市列表中找到。")
            
            # 3. 正常存入（ReplacingMergeTree 自动处理更新）
            from src.model import UsStockUniverseModel
            
            if not active_raw.empty:
                active_data = UsStockUniverseModel.format_dataframe(active_raw)
                self.repo.insert_us_stock_universe(active_data)

            if not delisted_raw.empty:
                delisted_data = UsStockUniverseModel.format_dataframe(delisted_raw)
                self.repo.insert_us_stock_universe(delisted_data)

        except Exception as e:
            logger.error(f"宇宙表同步或变动探测失败: {e}")

    def fetch_klines(self, backfill_days: Optional[int] = None):
        """
        流式消费补齐任务。
        采用拉模式 (Pull Model)：活跃标的无状态只读最后时间；退市标的由自己判断是否彻底结束并打标。
        """
        tasks_df = self.repo.get_sync_tasks('us_minutes_klines', id_column='composite_figi')
        if tasks_df.empty: return

        # 过滤：活跃的全部拉取；退市的且 sync_state == 0 (未完成) 的拉取
        sync_mask = (tasks_df['active'] == 1) | ((tasks_df['active'] == 0) & (tasks_df['sync_state'] == 0))
        filtered_tasks = tasks_df[sync_mask].dropna(subset=['ticker'])

        for row in filtered_tasks.itertuples():
            try:
                now_nyc = datetime.now(self.NYC)
                end_ms = int(now_nyc.timestamp() * 1000)
                
                # 获取最后一条记录时间 (所有标的通用)
                last_dt = self.repo.get_latest_stock_minutes_klines(row.composite_figi)
                last_ms = int(last_dt.timestamp() * 1000)
                
                # 退市标的自我判决边界
                if row.active == 0:
                    delisted_dt = pd.to_datetime(row.delisted_date, utc=True)
                    delisted_ms = int(delisted_dt.timestamp() * 1000)
                    
                    # 终止条件：数据时间 + 8小时 >= 下市时间，自我打标结束
                    if last_ms + (8 * 3600 * 1000) >= delisted_ms:
                        self.repo.update_sync_status('us_minutes_klines', row.composite_figi, 'composite_figi', 1)
                        continue
                    
                    # 截断抓取终点
                    end_ms = min(end_ms, delisted_ms)

                # 活跃标的滚动重刷逻辑
                if row.active == 1 and backfill_days:
                    start_dt = now_nyc - timedelta(days=backfill_days)
                    # 为了避免浪费，取 (当前最新时间, 回溯点) 的较小值作为起点？不，我们要回溯，
                    # 但不应早于冷启动边界。简单起见，既然要重刷，就直接用回溯点。
                    last_ms = int(start_dt.timestamp() * 1000)
                
                # 如果同步起点 > 终点，跳过
                if last_ms >= (end_ms - 60000): continue
                
                # 3. 执行流式抓取 (强制获取未复权原始数据)
                has_data = False
                for page_df in self.massive.get_historical_klines(
                    ticker=row.ticker, 
                    multiplier=self.KLINE_SPAN, 
                    start=str(last_ms + 1), 
                    end=str(end_ms),
                    adjusted=False  # 🌟 核心量化原则：底层永远存原始数据
                ):
                    if page_df.empty: continue
                    has_data = True
                    
                    clean_df = UsStockMinutesKlineModel.format_dataframe(
                        page_df, row.ticker, row.composite_figi
                    )
                    self.repo.insert_stock_minutes_klines(clean_df)
                
                # 如果是退市标的，拉了一遍后发现还是啥都没有 (或者拉完了)，打标
                if not row.active:
                    self.repo.update_sync_status('us_minutes_klines', row.composite_figi, 'composite_figi', 1)

                logger.info(f"[{row.ticker}] 补齐完成 (回溯模式: {backfill_days or '增量'})。")
            except Exception as e:
                logger.error(f"处理标的 {row.ticker} 异常: {e}")

    def refresh_recent_klines(self):
        """强制回溯最近 3 天数据"""
        logger.info("📅 执行最近 3 天 K 线滚动重刷...")
        self.fetch_klines(backfill_days=3)
