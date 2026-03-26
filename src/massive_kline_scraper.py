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
   - K线同步: 交易日 10:00-16:59 每 5 分钟增量同步（盘中高频）+ 18:30 收盘后回补。
================================================================================
"""

import pandas as pd
import numpy as np
import requests
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
from src.config.settings import settings


class MassiveKlineScraper:
    NYC = ZoneInfo("America/New_York")
    API_KLINE_MAX_LIMIT = 5000
    API_TICKERS_MAX_LIMIT = 100
    SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    SEC_API_IO_MAPPING_URL = "https://api.sec-api.io/mapping/ticker/{ticker}"

    def __init__(self, scheduler: BlockingScheduler):
        self.massive = MassiveApi()
        self.repo = MarketDataRepo()
        self.scheduler = scheduler
        self.KLINE_SPAN = settings.scraper.kline_span
        self.COLD_START_DATE = settings.scraper.scraping_start_date
        self._sec_ticker_cik_map: dict[str, str] | None = None
        self._sec_api_cik_cache: dict[str, str] = {}

    def _fetch_tickers_by_active(self, active: bool) -> pd.DataFrame | None:
        frames: list[pd.DataFrame] = []
        tickers_raw = self.massive.get_all_tickers(sort_type="ticker", active=active)
        if tickers_raw is None:
            logger.error("Massive: 迭代获取股票列表中断,等待调度器下次运行")
            return None
        if tickers_raw.empty:
            return pd.DataFrame()

        frames.append(tickers_raw)
        last_ticker = tickers_raw.iloc[-1]["ticker"]
        while True:
            tickers_raw = self.massive.get_all_tickers(
                ticker_filter_type="ticker.gt",
                ticker=last_ticker,
                sort_type="ticker",
                active=active,
                limit=self.API_TICKERS_MAX_LIMIT,
            )
            if tickers_raw is None:
                logger.error("Massive: 迭代获取股票列表中断,等待调度器下次运行")
                return None
            if tickers_raw.empty:
                break
            frames.append(tickers_raw)
            last_ticker = tickers_raw.iloc[-1]["ticker"]
            if len(tickers_raw) < self.API_TICKERS_MAX_LIMIT:
                break
        return pd.concat(frames, ignore_index=True)

    def start(self):
        """异步注册与高频调度启动"""
        # 1. 宇宙表同步 (美东 01:00 和 09:00, 周一至周五)

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
            minute="*/5",
            timezone=self.NYC,
            day_of_week="mon-fri",
            id="fetch_klines",
            next_run_time=datetime.now(self.NYC),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.fetch_klines,
            "cron",
            hour=18,
            minute=30,
            timezone=self.NYC,
            day_of_week="mon-fri",
            id="fetch_klines_close_backfill",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        # 盘中高频 + 收盘后回补，降低漏数风险。

        logger.info("✅ Massive K线搜刮器已启动。")

    def load_all_figi_ticker_mapping(self, universe_tickers: pd.DataFrame):
        result = []
        for _, row in universe_tickers.iterrows():
            ticker = row.ticker
            figi = row.composite_figi
            raw_data = self.massive.get_ticker_events(ticker)
            if raw_data is None:
                logger.warning(f"Massive: 获取 {ticker} 的 ticker events 失败")
                result.append(
                    {
                        "composite_figi": figi,
                        "ticker": ticker,
                        "date": "1970-01-01",
                    }
                )
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
                and event.get("ticker_change", {}).get("ticker")
                and event.get("date")
            ]
            if ticker_date_map:
                result.extend(ticker_date_map)

            # 增量写入，防止进程中断导致全量丢失 (1.9w 股票需要跑很久)
            if len(result) >= 500:
                data_raw = pd.DataFrame(result)
                data = UsStockFigiTickerMappingModel.format_dataframe(data_raw)
                if not data.empty:
                    self.repo.insert_us_stock_figi_ticker_mapping(data)
                    logger.info(f"Massive: 已增量写入 {len(data)} 条 FIGI 映射历史。")
                result = []

        if result:
            data_raw = pd.DataFrame(result)
            data = UsStockFigiTickerMappingModel.format_dataframe(data_raw)
            if not data.empty:
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

        missing_data = df.loc[
            mask_missing, ["ticker", "primary_exchange"]
        ].drop_duplicates(subset=["ticker"])

        if missing_data.empty:
            return df

        missing_tasks = missing_data.to_dict("records")

        # 2. 调用 OpenFIGI 客户端获取映射
        figi_client = OpenFIGIClient()
        mapping_df = figi_client.fetch_figis(missing_tasks)

        if not mapping_df.empty:
            # 3. 建立映射字典并回填
            mapping_dict = mapping_df.set_index("ticker")["composite_figi"].to_dict()

            # 仅针对缺失部分进行更新
            df.loc[mask_missing, "composite_figi"] = df.loc[mask_missing, "ticker"].map(
                mapping_dict
            )

        return df

    def enrich_cik(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        提取空 CIK 的记录并补全。
        优先走 SEC 官方 ticker->CIK 静态映射；若仍缺失，再 fallback 到
        覆盖 listed + delisted 的 sec-api mapping 接口。
        """
        if df.empty or "cik" not in df.columns:
            return df

        mask_missing = df["cik"].isna() | (df["cik"] == "") | (df["cik"] == "nan")
        if not mask_missing.any():
            return df

        pending = df.loc[mask_missing, ["ticker"]].drop_duplicates(subset=["ticker"])
        if pending.empty:
            return df

        ticker_to_cik = self._get_sec_ticker_cik_map()
        if ticker_to_cik:
            df.loc[mask_missing, "cik"] = (
                df.loc[mask_missing, "ticker"]
                .astype(str)
                .str.upper()
                .map(ticker_to_cik)
            )

        still_missing_mask = df["cik"].isna() | (df["cik"] == "") | (df["cik"] == "nan")
        if still_missing_mask.any():
            pending = df.loc[still_missing_mask, ["ticker", "name"]].drop_duplicates(
                subset=["ticker"]
            )
            if not pending.empty:
                fallback_map = self._get_sec_api_fallback_map(pending)
                if fallback_map:
                    df.loc[still_missing_mask, "cik"] = (
                        df.loc[still_missing_mask, "ticker"]
                        .astype(str)
                        .str.upper()
                        .map(fallback_map)
                    )

        return df

    def _get_sec_ticker_cik_map(self) -> dict[str, str]:
        if self._sec_ticker_cik_map is not None:
            return self._sec_ticker_cik_map

        headers = {
            "User-Agent": 'Wuhan Hubber Consulting Co., Ltd. "wei@hubber.top"',
            "Accept": "application/json",
        }
        try:
            response = requests.get(
                self.SEC_COMPANY_TICKERS_URL,
                headers=headers,
                timeout=(10, 60),
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            logger.error(f"拉取 SEC company_tickers.json 失败: {exc}")
            self._sec_ticker_cik_map = {}
            return self._sec_ticker_cik_map

        ticker_to_cik: dict[str, str] = {}
        records = payload.values() if isinstance(payload, dict) else payload
        for item in records:
            ticker = str(item.get("ticker", "")).strip().upper()
            cik_raw = item.get("cik_str")
            if not ticker or cik_raw in (None, ""):
                continue
            ticker_to_cik[ticker] = str(cik_raw).strip().zfill(10)

        self._sec_ticker_cik_map = ticker_to_cik
        logger.debug(f"SEC ticker->CIK 映射加载完成，共 {len(ticker_to_cik)} 条。")
        return self._sec_ticker_cik_map

    def _get_sec_api_fallback_map(self, pending: pd.DataFrame) -> dict[str, str]:
        api_key = (settings.api.sec_api_io_key or "").strip()
        if not api_key:
            return {}

        result: dict[str, str] = {}
        headers = {"Authorization": api_key}
        for row in pending.itertuples(index=False):
            ticker = str(row.ticker).strip().upper()
            if not ticker:
                continue
            if ticker in self._sec_api_cik_cache:
                cached = self._sec_api_cik_cache[ticker]
                if cached:
                    result[ticker] = cached
                continue

            cik = self._lookup_sec_api_cik(
                ticker=ticker,
                company_name=str(getattr(row, "name", "") or "").strip(),
                headers=headers,
            )
            self._sec_api_cik_cache[ticker] = cik
            if cik:
                result[ticker] = cik

        if result:
            logger.debug(f"sec-api fallback 补到 {len(result)} 条 ticker->CIK。")
        return result

    def _lookup_sec_api_cik(
        self, ticker: str, company_name: str, headers: dict[str, str]
    ) -> str:
        url = self.SEC_API_IO_MAPPING_URL.format(ticker=ticker)
        try:
            response = requests.get(url, headers=headers, timeout=(10, 30))
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            logger.warning(f"sec-api 查询 {ticker} 的 CIK 失败: {exc}")
            return ""

        if not isinstance(payload, list) or not payload:
            return ""

        normalized_name = self._normalize_company_name(company_name)
        exact_ticker_rows = [
            item
            for item in payload
            if str(item.get("ticker", "")).strip().upper() == ticker
        ]
        candidates = exact_ticker_rows or payload

        if normalized_name:
            for item in candidates:
                item_name = self._normalize_company_name(str(item.get("name", "")))
                if item_name and (
                    item_name == normalized_name
                    or item_name in normalized_name
                    or normalized_name in item_name
                ):
                    cik = str(item.get("cik", "")).strip()
                    if cik:
                        return cik.zfill(10)

        for item in candidates:
            cik = str(item.get("cik", "")).strip()
            if cik:
                return cik.zfill(10)
        return ""

    @staticmethod
    def _normalize_company_name(name: str) -> str:
        cleaned = str(name or "").upper()
        for token in [
            ",",
            ".",
            "(",
            ")",
            "/",
            "-",
            " INC",
            " CORP",
            " CORPORATION",
            " LTD",
            " LIMITED",
            " PLC",
            " NV",
            " SA",
            " LP",
            " LLC",
            " ADS",
            " COMMON STOCK",
            " COM STK",
        ]:
            cleaned = cleaned.replace(token, " ")
        return " ".join(cleaned.split())

    @staticmethod
    def _deduplicate_by_figi(df: pd.DataFrame) -> pd.DataFrame:
        """
        同 FIGI 去重规则:
        1) 若 active 值不一致，优先保留 active=1 的记录；
        2) 若 active 值一致，保留 last_updated_utc 最大的记录。
        """
        if df.empty:
            return df

        picked_rows: list[pd.Series] = []
        for _, group in df.groupby("composite_figi", sort=False):
            active_values = set(
                pd.to_numeric(group["active"], errors="coerce").fillna(0).astype(int)
            )
            if len(active_values) == 1:
                chosen = group.sort_values(
                    by=["last_updated_utc", "ticker"],
                    ascending=[False, False],
                    kind="stable",
                ).iloc[0]
            else:
                active_group = group[
                    pd.to_numeric(group["active"], errors="coerce")
                    .fillna(0)
                    .astype(int)
                    == 1
                ]
                if active_group.empty:
                    chosen = group.sort_values(
                        by=["last_updated_utc", "ticker"],
                        ascending=[False, False],
                        kind="stable",
                    ).iloc[0]
                else:
                    chosen = active_group.sort_values(
                        by=["last_updated_utc", "ticker"],
                        ascending=[False, False],
                        kind="stable",
                    ).iloc[0]
            picked_rows.append(chosen)

        return pd.DataFrame(picked_rows).reset_index(drop=True)

    def load_stock_universe(self):
        """
        通过首字母全量拉取股票宇宙表 (使用 ticker.gt 鲁棒迭代)
        """
        logger.info("Massive Universe 同步开始。")
        total_raw = 0
        total_filtered = 0
        inserted_rows = 0
        missing_figi_count = 0
        missing_cik_count = 0

        old_df = self.repo.get_universe_tickers()
        if not old_df.empty:
            # 立即清洗老数据，防止 ClickHouse 返回的 bytes 类型污染后续处理流
            old_df = UsStockUniverseModel.format_dataframe(old_df)
        try:
            # 1. 全量抓取股票 (不区分活跃状态，由 API 或后续逻辑统一处理)
            logger.debug("正在通过 ticker 顺序迭代拉取全量 Ticker 列表...")
            active_df = self._fetch_tickers_by_active(active=True)
            if active_df is None:
                return
            inactive_df = self._fetch_tickers_by_active(active=False)
            if inactive_df is None:
                return

            all_tickers_dfs = [df for df in [active_df, inactive_df] if not df.empty]

            if not all_tickers_dfs:
                logger.error("Massive: api 返回为空，等待调度器下次运行")
                return
            all_tickers_raw = pd.concat(all_tickers_dfs, ignore_index=True)
            total_raw = len(all_tickers_raw)

            # 【过滤】仅保留业务需要的 4 种标的类型
            if "type" in all_tickers_raw.columns:
                before_count = len(all_tickers_raw)
                all_tickers_raw["type"] = all_tickers_raw["type"].apply(
                    lambda x: x.decode("utf-8", "ignore")
                    if isinstance(x, bytes)
                    else x
                )
                all_tickers_raw["type"] = (
                    all_tickers_raw["type"].astype(str).str.upper()
                )
                allowed_types = {"CS", "OS", "ADRC", "NYRS"}
                all_tickers_raw = all_tickers_raw[
                    all_tickers_raw["type"].isin(allowed_types)
                ].copy()
                after_count = len(all_tickers_raw)
                logger.debug(
                    f"✂️ 已过滤为 {sorted(allowed_types)}，剩余 {after_count} / {before_count} 个 Ticker。"
                )
            total_filtered = len(all_tickers_raw)

            logger.debug(
                f"本次宇宙表同步准备更新，共计处理 {total_filtered} 个 Ticker。"
            )

            all_tickers = UsStockUniverseModel.format_dataframe(all_tickers_raw)

            # 3. 继承老表的 FIGI (基于 Ticker 映射)
            # 这一步是为了避免给 OpenFIGI 增加不必要的负担
            if not old_df.empty:
                # 确保 FIGI 是字符串类型，防止 bytes 导致字典主键匹配失败
                old_df["composite_figi"] = old_df["composite_figi"].apply(
                    lambda x: (
                        x.decode("utf-8")
                        if isinstance(x, bytes)
                        else str(x)
                        if pd.notna(x)
                        else ""
                    )
                )
                ticker_to_figi = old_df.set_index("ticker")["composite_figi"].to_dict()
                # 统一处理空值，确保 fillna 生效
                all_tickers["composite_figi"] = all_tickers["composite_figi"].replace(
                    {"": np.nan, None: np.nan}
                )
                all_tickers["composite_figi"] = all_tickers["composite_figi"].fillna(
                    all_tickers["ticker"].map(ticker_to_figi)
                )

                # 继承老表已有的 CIK，优先用 FIGI，再回退到 ticker。
                old_df["cik"] = old_df["cik"].apply(
                    lambda x: (
                        x.decode("utf-8")
                        if isinstance(x, bytes)
                        else str(x)
                        if pd.notna(x)
                        else ""
                    )
                )
                old_df["cik"] = old_df["cik"].replace({"nan": "", None: ""})
                old_df["composite_figi"] = old_df["composite_figi"].replace(
                    {"nan": "", None: ""}
                )

                figi_to_cik = old_df.loc[
                    old_df["cik"] != "", ["composite_figi", "cik"]
                ].drop_duplicates(subset=["composite_figi"])
                ticker_to_cik = old_df.loc[
                    old_df["cik"] != "", ["ticker", "cik"]
                ].drop_duplicates(subset=["ticker"])

                if not figi_to_cik.empty:
                    all_tickers["cik"] = all_tickers["cik"].replace(
                        {"": np.nan, None: np.nan}
                    )
                    all_tickers["cik"] = all_tickers["cik"].fillna(
                        all_tickers["composite_figi"].map(
                            figi_to_cik.set_index("composite_figi")["cik"].to_dict()
                        )
                    )

                if not ticker_to_cik.empty:
                    all_tickers["cik"] = all_tickers["cik"].replace(
                        {"": np.nan, None: np.nan}
                    )
                    all_tickers["cik"] = all_tickers["cik"].fillna(
                        all_tickers["ticker"].map(
                            ticker_to_cik.set_index("ticker")["cik"].to_dict()
                        )
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
                logger.debug(f"发现 {len(pending_df)} 条记录缺失 FIGI，执行补全...")

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

            # 4.1 继承旧值后，剩余缺失 CIK 的行用 Massive ticker events 补齐
            missing_cik_mask = (
                all_tickers["cik"].isna()
                | (all_tickers["cik"] == "")
                | (all_tickers["cik"] == "nan")
            )
            if missing_cik_mask.any():
                pending_cik_df = all_tickers[missing_cik_mask].copy()
                logger.debug(f"发现 {len(pending_cik_df)} 条记录缺失 CIK，执行补全...")
                enriched_cik_part = self.enrich_cik(pending_cik_df)
                enriched_cik_map = enriched_cik_part.set_index("ticker")[
                    "cik"
                ].to_dict()
                all_tickers["cik"] = all_tickers["cik"].replace(
                    {"": np.nan, "nan": np.nan}
                )
                all_tickers["cik"] = all_tickers["cik"].fillna(
                    all_tickers["ticker"].map(enriched_cik_map)
                )

            # 经过 FIGI / CIK 回填后，再统一跑一次格式化，避免 float NaN 混入字符串列。
            all_tickers = UsStockUniverseModel.format_dataframe(all_tickers)

            # 5. 过滤掉最终还是没有 FIGI 的非法记录（主键不能为空）
            # 剩下的就是：全新的、补齐的、以及需要更新元数据的存量记录
            final_to_insert = all_tickers[
                all_tickers["composite_figi"].notna()
                & (all_tickers["composite_figi"] != "")
            ].copy()

            # 同一 FIGI 可能对应历史旧 ticker 与当前 ticker（如 SCH / SCHW）。
            # Universe 表主键是 composite_figi，入库前必须折叠为 1 条并优先保留 active=1。
            if not final_to_insert.empty:
                before_dedup_count = len(final_to_insert)
                final_to_insert = self._deduplicate_by_figi(final_to_insert)
                dropped_same_figi = before_dedup_count - len(final_to_insert)
                if dropped_same_figi > 0:
                    logger.warning(
                        f"发现 {dropped_same_figi} 条 FIGI 重复记录，已按 active/last_updated/ticker 优先级折叠。"
                    )

                # 折叠后再做一次格式化，避免 groupby / DataFrame 重建把列 dtype 重新污染成 float。
                final_to_insert = UsStockUniverseModel.format_dataframe(final_to_insert)

            no_figi_count = len(all_tickers) - len(final_to_insert)
            missing_figi_count = no_figi_count
            missing_cik_count = int(
                (
                    final_to_insert["cik"].isna()
                    | (final_to_insert["cik"] == "")
                    | (final_to_insert["cik"] == "nan")
                ).sum()
            )
            if no_figi_count > 0:
                no_figi_df = all_tickers[
                    all_tickers["composite_figi"].isna()
                    | (all_tickers["composite_figi"] == "")
                ]
                logger.warning(
                    f"⚠️ 最终仍有 {no_figi_count} 个 Ticker 缺失 FIGI (大多为特殊标的)，将跳过入库。示例：{no_figi_df.head(5)}"
                )

            if not final_to_insert.empty:
                # 6. 全量/增量写入（ReplacingMergeTree 会自动根据 FIGI 处理新增或更新）
                self.repo.insert_stock_universe(final_to_insert)
                inserted_rows = len(final_to_insert)

                # 7. 只有真正“新出现”的 Ticker 或变动，以及 Mapping 表为空时，才去跑 Mapping 历史
                is_mapping_empty = self.repo.is_mapping_table_empty()
                if old_df.empty or is_mapping_empty:
                    if is_mapping_empty:
                        logger.debug("检测到 FIGI 映射表为空，开始执行存量补全...")
                    self.load_all_figi_ticker_mapping(final_to_insert)
                else:
                    new_tickers = final_to_insert[
                        ~final_to_insert["ticker"].isin(old_df["ticker"])
                    ]
                    if not new_tickers.empty:
                        self.load_all_figi_ticker_mapping(new_tickers)

        except Exception as e:
            logger.error(f"宇宙表同步失败: {e}", exc_info=True)
        else:
            logger.info(
                f"✅ Massive Universe 本轮完成: 原始={total_raw} 过滤后={total_filtered} "
                f"入库={inserted_rows} 缺失FIGI跳过={missing_figi_count} "
                f"缺失CIK保留={missing_cik_count}"
            )

    def fetch_klines(self):
        """
        流式消费补齐任务。
        1. 从 get_sync_tasks 获取所有 ticker 的任务列表 (包含 sync_state)
        2. 批量获取每只股票在 us_minutes_klines 中的最新时间戳
        3. 从最新时间戳拉取到现在，冷启动时从 COLD_START_DATE 开始
        4. active=0 + 总返回数据量 < limit → 标记 state=1
        """
        logger.info("🚀 启动 K 线增量拉取...")
        total_tasks = 0
        inserted_rows = 0
        failed_tickers = 0
        marked_done = 0

        # 1. 获取任务列表
        tasks_df = self.repo.get_sync_tasks(
            "us_minutes_klines", id_column="composite_figi"
        )
        if tasks_df.empty:
            logger.debug("无 K 线拉取任务。")
            return
        total_tasks = len(tasks_df)

        # 2. 批量获取每只股票在 us_minutes_klines 中的最新时间戳
        latest_ts_df = self.repo.get_all_stocks_latest_ts_df_by_group()
        ts_map = {}  # composite_figi -> last_ts (ms)
        if not latest_ts_df.empty:
            for _, r in latest_ts_df.iterrows():
                if pd.notna(r["last_ts"]):
                    figi_key = (
                        r["composite_figi"].decode("utf-8", "ignore")
                        if isinstance(r["composite_figi"], bytes)
                        else str(r["composite_figi"])
                    )
                    ts_map[figi_key] = int(
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
            composite_figi = (
                row["composite_figi"].decode("utf-8", "ignore")
                if isinstance(row["composite_figi"], bytes)
                else str(row["composite_figi"])
            )
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
                    # 当上次拉取下市股票k线数据刚好为api limit条时，本次拉取为空，补上标记为已完结
                    if active == 0:
                        self.repo.update_sync_status(
                            "us_minutes_klines", composite_figi, "composite_figi", 1
                        )
                        marked_done += 1

                    continue

                clean_df = UsStockMinutesKlineModel.format_dataframe(
                    data_raw, composite_figi
                )
                self.repo.insert_stock_minutes_klines(clean_df)
                inserted_rows += len(clean_df)

                # 5. 退市完结判定：生成器正常跑完（无报错）+ active=0 → 数据已全部拉完
                if active == 0 and len(data_raw) < self.API_KLINE_MAX_LIMIT:
                    self.repo.update_sync_status(
                        "us_minutes_klines", composite_figi, "composite_figi", 1
                    )
                    marked_done += 1
                    logger.debug(
                        f"🏁 {ticker} delisted & data exhausted ({len(data_raw)} rows). Marked state=1."
                    )

            except Exception as e:
                failed_tickers += 1
                logger.error(f"❌ 处理标的 {ticker} 异常: {e}")
                continue

        logger.info(
            f"✅ K线本轮完成: 任务={total_tasks} 新增行数={inserted_rows} "
            f"退市完成标记={marked_done} 失败ticker={failed_tickers}"
        )

    def stop(self):
        if hasattr(self, "scheduler") and self.scheduler:
            try:
                self.scheduler.remove_job("universe_sync")
            except Exception:
                pass
            try:
                self.scheduler.remove_job("fetch_klines")
            except Exception:
                pass
            try:
                self.scheduler.remove_job("fetch_klines_close_backfill")
            except Exception:
                pass
        logger.info("🛑 Massive K线搜刮器已停止。")
