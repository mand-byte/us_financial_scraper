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
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from src.api import MassiveApi, OpenFIGIClient
from src.model import (
    UsStockMinutesKlineModel,
    UsStockUniverseModel,
    UsStockFigiTickerMappingModel,
)
from src.dao.market_data_repo import MarketDataRepo
from src.utils.logger import app_logger as logger
import os
import numpy as np


class MassiveKlineScraper:
    NYC = ZoneInfo("America/New_York")
    API_KLINE_MAX_LIMIT = 50000
    API_TICKERS_MAX_LIMIT = 1000

    def __init__(self, scheduler: BlockingScheduler):
        self.massive = MassiveApi()
        self.repo = MarketDataRepo()
        self.scheduler = scheduler
        self.KLINE_SPAN = int(os.getenv("KLINE_SPAN", 5))
        self.COLD_START_DATE = os.getenv("SCRAPING_START_DATE", "2021-01-01")

    def start(self):
        """异步注册与高频调度启动"""
        # 1. 宇宙表高频同步 (美东 01:00 和 09:00, 周一至周五)

        self.scheduler.add_job(
            self.load_stock_universe,
            "cron",
            day_of_week="mon-fri",
            hour="1,9",
            minute=0,
            timezone=self.NYC,
            id="universe_sync",
            next_run_time=datetime.now(self.NYC),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )

        self.scheduler.add_job(
            self.fetch_klines,
            "cron",
            hour="10-16",
            minute="0/5",
            timezone=self.NYC,
            day_of_week="mon-fri",
            id="fetch_klines",
            next_run_time=datetime.now(self.NYC),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )

        logger.info("✅ Massive K线搜刮器已启动。")

    def load_all_figi_ticker_mapping(self, universe_tickers: pd.DataFrame):
        result = []
        for _, row in universe_tickers.iterrows():
            ticker = row.ticker
            figi = row.composite_figi
            raw_data = self.massive.get_ticker_events(ticker)
            if raw_data is None:
                logger.error(f"Massive: 获取 {ticker} 的 ticker events 失败")
                continue
            events = raw_data.get("events", [])

            ticker_date_map = [
                {
                    "composite_figi": figi,
                    "ticker": event["ticker_change"]["ticker"],
                    "date": event["date"],
                }
                for event in events
                if "ticker_change" in event
            ]
            if ticker_date_map:
                result.extend(ticker_date_map)
        if result:
            data_raw = pd.DataFrame(result)
            data = UsStockFigiTickerMappingModel.format_dataframe(data_raw)
            self.repo.insert_us_stock_figi_ticker_mapping(data)

    def enrich_figi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        提取空 FIGI 的记录并调用 OpenFIGIClient 进行补全
        """
        if df.empty or "composite_figi" not in df.columns:
            return df

        # 1. 识别真正缺失 FIGI 的行
        mask_missing = (
            (df["composite_figi"].isna())
            | (df["composite_figi"] == "")
            | (df["composite_figi"] == "nan")
        )
        missing_tickers = df.loc[mask_missing, "ticker"].unique().tolist()

        if not missing_tickers:
            return df

        # 2. 调用 OpenFIGI 客户端获取映射
        figi_client = OpenFIGIClient()
        mapping_df = figi_client.fetch_figis(missing_tickers)

        if not mapping_df.empty:
            # 3. 建立映射字典并回填
            mapping_dict = mapping_df.set_index("ticker")["composite_figi"].to_dict()

            # 仅针对缺失部分进行更新
            df.loc[mask_missing, "composite_figi"] = df.loc[mask_missing, "ticker"].map(
                mapping_dict
            )

        return df

    def load_stock_universe(self):
        """
        通过首字母全量拉取股票宇宙表
        """
        logger.info("Massive: 正在执行宇宙表例行同步并探测退市事件...")

        old_df = self.repo.get_universe_tickers()
        try:
            all_tickers_raw = []
            tickers_raw = self.massive.get_all_tickers(sort_type="ticker")
            if tickers_raw is None or tickers_raw.empty:
                logger.error("Massive: 首次获取全美股宇宙表失败,等待调度器下次运行")
                return
            start_ticker_with = tickers_raw.iloc[-1]["ticker"]
            all_tickers_raw.append(tickers_raw)
            while True:
                tickers_raw = self.massive.get_all_tickers(
                    ticker_filter_type="ticker.gt",
                    ticker=start_ticker_with,
                    sort_type="ticker",
                )
                if tickers_raw is None:
                    logger.error("Massive: 获取全美股宇宙表失败,等待调度器下次运行")
                    return
                if tickers_raw.empty:
                    break
                all_tickers_raw.append(tickers_raw)
                start_ticker_with = tickers_raw.iloc[-1]["ticker"]
                if len(tickers_raw) < self.API_TICKERS_MAX_LIMIT:
                    break
            all_tickers_raw = pd.concat(all_tickers_raw, ignore_index=True)
            all_tickers = UsStockUniverseModel.format_dataframe(all_tickers_raw)

            # 3. 继承老表的 FIGI (基于 Ticker 映射)
            # 这一步是为了避免给 OpenFIGI 增加不必要的负担
            if not old_df.empty:
                ticker_to_figi = old_df.set_index("ticker")["composite_figi"].to_dict()
                # 统一处理空值，确保 fillna 生效
                all_tickers["composite_figi"] = all_tickers["composite_figi"].replace(
                    {"": np.nan, None: np.nan}
                )
                all_tickers["composite_figi"] = all_tickers["composite_figi"].fillna(
                    all_tickers["ticker"].map(ticker_to_figi)
                )

            # 4. 找出真正缺失 FIGI 的行进行补全
            # 只要是新 Ticker 或者 依然没有 FIGI 的记录，都需要去 OpenFIGI 跑一趟
            missing_figi_mask = (
                (all_tickers["composite_figi"].isna())
                | (all_tickers["composite_figi"] == "")
                | (all_tickers["composite_figi"] == "nan")
            )

            if missing_figi_mask.any():
                pending_df = all_tickers[missing_figi_mask].copy()
                logger.info(f"发现 {len(pending_df)} 条记录缺失 FIGI，执行补全...")

                # 补全后的数据
                enriched_part = self.enrich_figi(pending_df)

                # 【优化点】将补全后的 FIGI 写回 all_tickers 总表
                enriched_map = enriched_part.set_index("ticker")[
                    "composite_figi"
                ].to_dict()
                # 统一空字符串为 NaN，确保 fillna 能覆盖所有缺失情况
                all_tickers["composite_figi"] = all_tickers["composite_figi"].replace(
                    {"": np.nan, "nan": np.nan}
                )
                all_tickers["composite_figi"] = all_tickers["composite_figi"].fillna(
                    all_tickers["ticker"].map(enriched_map)
                )

            # 5. 过滤掉最终还是没有 FIGI 的非法记录（主键不能为空）
            # 剩下的就是：全新的、补齐的、以及需要更新元数据的存量记录
            final_to_insert = all_tickers[
                all_tickers["composite_figi"].notna()
                & (all_tickers["composite_figi"] != "")
            ]

            if not final_to_insert.empty:
                # 6. 全量/增量写入（ReplacingMergeTree 会自动根据 FIGI 处理新增或更新）
                self.repo.insert_stock_universe(final_to_insert)

                # 7. 只有真正“新出现”的 Ticker 或变动，才去跑 Mapping 历史
                # 建议：如果 old_df 为空，说明是首次，全跑；否则只跑新 Ticker
                if old_df.empty:
                    self.load_all_figi_ticker_mapping(final_to_insert)
                else:
                    new_tickers = final_to_insert[
                        ~final_to_insert["ticker"].isin(old_df["ticker"])
                    ]
                    if not new_tickers.empty:
                        self.load_all_figi_ticker_mapping(new_tickers)

        except Exception as e:
            logger.error(f"宇宙表同步失败: {e}", exc_info=True)

    def fetch_klines(self):
        """
        流式消费补齐任务。
        1. 从 get_sync_tasks 获取所有 ticker 的任务列表 (包含 sync_state)
        2. 批量获取每只股票在 us_minutes_klines 中的最新时间戳
        3. 从最新时间戳拉取到现在，冷启动时从 COLD_START_DATE 开始
        4. active=0 + 总返回数据量 < limit → 标记 state=1
        """
        logger.info("🚀 启动 K 线增量拉取...")

        # 1. 获取任务列表
        tasks_df = self.repo.get_sync_tasks(
            "us_minutes_klines", id_column="composite_figi"
        )
        if tasks_df.empty:
            logger.info("无 K 线拉取任务。")
            return

        # 2. 批量获取每只股票在 us_minutes_klines 中的最新时间戳
        latest_ts_df = self.repo.get_all_stocks_latest_ts_df_by_group()
        ts_map = {}  # composite_figi -> last_ts (ms)
        if not latest_ts_df.empty:
            for _, r in latest_ts_df.iterrows():
                if pd.notna(r["last_ts"]):
                    ts_map[r["composite_figi"]] = int(
                        pd.to_datetime(r["last_ts"]).timestamp() * 1000
                    )

        now_ms = int(datetime.now(self.NYC).timestamp() * 1000)
        cold_start_ms = int(
            datetime.strptime(self.COLD_START_DATE, "%Y-%m-%d")
            .replace(tzinfo=self.NYC)
            .timestamp()
            * 1000
        )

        for _, row in tasks_df.iterrows():
            # 跳过已完结的退市标的
            if row["sync_state"] == 1:
                continue

            ticker = row["ticker"]
            composite_figi = row["composite_figi"]
            active = row["active"]

            try:
                # 3. 确定拉取起点
                start_ms = ts_map.get(composite_figi, cold_start_ms)
                start_ms += 1  # +1ms 避免重复拉取最后一条

                # 如果距当前不足 1 分钟，跳过
                if start_ms >= now_ms - 60000:
                    continue

                data_raw = self.massive.get_historical_klines(
                    ticker=ticker,
                    multiplier=self.KLINE_SPAN,
                    start=str(start_ms),
                    end=str(now_ms),
                    adjusted=False,
                    limit=self.API_KLINE_MAX_LIMIT,
                )
                if data_raw is None:
                    logger.warning(f"⚠️ API failed for {ticker}. Skipping.")
                    continue
                if data_raw.empty:
                    # 当上次拉取下市股票k线数据刚好为50000条时，本次拉取为空，补上标记为已完结
                    if active == 0:
                        self.repo.update_sync_status(
                            "us_minutes_klines", composite_figi, "composite_figi", 1
                        )

                    continue

                clean_df = UsStockMinutesKlineModel.format_dataframe(
                    data_raw, ticker, composite_figi
                )
                self.repo.insert_stock_minutes_klines(clean_df)

                # 5. 退市完结判定：生成器正常跑完（无报错）+ active=0 → 数据已全部拉完
                if active == 0 and len(data_raw) < self.API_KLINE_MAX_LIMIT:
                    self.repo.update_sync_status(
                        "us_minutes_klines", composite_figi, "composite_figi", 1
                    )
                    logger.info(
                        f"🏁 {ticker} delisted & data exhausted ({len(data_raw)} rows). Marked state=1."
                    )

            except Exception as e:
                logger.error(f"❌ 处理标的 {ticker} 异常: {e}")
                continue

        logger.info("✅ K 线拉取任务完成。")
