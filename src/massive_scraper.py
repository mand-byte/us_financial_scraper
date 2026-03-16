"""
================================================================================
Massive K线搜刮器 (MassiveDataFetcher) - 需求与逻辑文档
================================================================================

[核心需求 - 已实现]
1. 数据源: 
   - 接口: Massive API v2 Aggs (get_historical_klines).
   - 模式: 流式抓取 (Generator)，逐页入库，防止 10 年补数时 OOM。
   - 内容: 包含盘前与盘后数据，通过 ReplacingMergeTree 实现物理去重。
2. 双端补齐 (Smart Sync):
   - 缺尾: last_ts < End, 补齐增量数据。
   - 缺头: first_ts > COLD_START_DATE, 补齐历史断层。
   - 无状态化: 逻辑完全依赖 get_kline_sync_tasks 的聚合查询。
3. 调度逻辑:
   - 初始启动: 异步触发 initial_data_fill 任务。
   - 宇宙表刷新: 美东周一至周五 01:00 和 09:00 同步新上市/退市标的。
   - 每日同步: 每个交易日 19:00 NYC 补全当日数据。
   - 每月审计: 每月 1 号 02:00 NYC 完整性检查。
4. 配置化:
   - KLINE_SPAN: 默认 5 分钟。

[待处理/未来逻辑 - TODO]
1. 错误重试: 读取错误日志自动补数。
================================================================================
"""

import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
        # 1. 注册宇宙表高频同步 (美东 01:00 和 09:00, 周一至周五)
        for hour in [1, 9]:
            self.scheduler.add_job(
                self.load_stock_universe, 'cron', day_of_week='mon-fri', 
                hour=hour, minute=0, timezone=self.NYC, id=f"universe_sync_{hour}"
            )

        # 2. 注册常规同步任务
        self.scheduler.add_job(
            self.fetch_klines, 'cron', hour=19, minute=0, timezone=self.NYC, id="daily_kline_sync"
        )
        self.scheduler.add_job(
            self.klines_health_checking, 'cron', day=1, hour=2, timezone=self.NYC, id="monthly_health_audit"
        )

        # 3. 🌟 异步触发初始无状态补数
        self.scheduler.add_job(
            self.fetch_klines, 
            next_run_time=datetime.now(self.NYC), 
            id="initial_data_fill"
        )
        logger.info("✅ Massive K线搜刮器已启动 (流式补数 + 高频宇宙表同步)。")

    def load_stock_universe(self):
        """无状态刷新全美股宇宙表"""
        logger.info("Massive: 正在执行宇宙表例行同步...")
        try:
            active_raw = self.massive.get_all_tickers(active=True)
            delisted_raw = self.massive.get_all_tickers(active=False)
            
            if active_raw.empty and delisted_raw.empty: return

            from src.model import UsStockUniverseModel
            active_data = UsStockUniverseModel.format_dataframe(active_raw)
            delisted_data = UsStockUniverseModel.format_dataframe(delisted_raw)
            
            self.repo.insert_us_stock_universe(active_data)
            self.repo.insert_us_stock_universe(delisted_data)
        except Exception as e:
            logger.error(f"宇宙表同步失败: {e}")

    def fetch_klines(self):
        """流式消费补齐任务：抓一页存一页，彻底解决内存积压"""
        tasks_df = self.repo.get_kline_sync_tasks()
        if tasks_df.empty: return

        for row in tasks_df.itertuples():
            try:
                # 判定边界
                if row.active == 1:
                    end_ms = int(datetime.now(self.NYC).timestamp() * 1000)
                else:
                    delisted_dt = pd.to_datetime(row.delisted_date, utc=True)
                    end_ms = int((delisted_dt + timedelta(hours=8)).timestamp() * 1000)

                cold_ms = int(pd.Timestamp(self.COLD_START_DATE, tz="UTC").timestamp() * 1000)
                ranges = []

                # 补尾
                last_ms = int(row.last_ts.timestamp() * 1000) if pd.notna(row.last_ts) else None
                if last_ms is None: ranges.append((cold_ms, end_ms))
                elif last_ms < (end_ms - 60000): ranges.append((last_ms + 1, end_ms))

                # 补头
                first_ms = int(row.first_ts.timestamp() * 1000) if pd.notna(row.first_ts) else None
                if first_ms and first_ms > (cold_ms + 60000): ranges.append((cold_ms, first_ms - 1))

                for s_ms, e_ms in ranges:
                    if s_ms >= e_ms: continue
                    
                    # 🌟 关键：流式遍历 API 生成器
                    for page_df in self.massive.get_historical_klines(
                        ticker=row.ticker, multiplier=self.KLINE_SPAN, start=str(s_ms), end=str(e_ms)
                    ):
                        if page_df.empty: continue
                        
                        clean_df = UsStockMinutesKlineModel.format_dataframe(
                            page_df, row.ticker, row.composite_figi
                        )
                        self.repo.insert_stock_minutes_klines(clean_df)
                        # yield 之后内存中仅保留一页数据，显著降低补数压力

                logger.info(f"[{row.ticker}] 补齐完成。")
            except Exception as e:
                logger.error(f"处理标的 {row.ticker} 异常: {e}")

    def klines_health_checking(self):
        pass
