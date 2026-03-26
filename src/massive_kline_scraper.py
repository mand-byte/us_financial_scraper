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

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

from src.api import MassiveApi, OpenFIGIClient
from src.config.settings import settings
from src.dao.market_data_repo import MarketDataRepo
from src.model import (
    UsStockFigiTickerMappingModel,
    UsStockMinutesKlineModel,
    UsStockUniverseModel,
)
from src.utils.logger import app_logger as logger


@dataclass(slots=True)
class UniverseSyncStats:
    total_raw: int = 0
    total_filtered: int = 0
    inserted_rows: int = 0
    missing_figi_count: int = 0
    missing_cik_count: int = 0


@dataclass(slots=True)
class KlineSyncTask:
    ticker: str
    composite_figi: str
    active: int
    sync_state: int


@dataclass(slots=True)
class KlineTaskResult:
    inserted_rows: int = 0
    marked_done: bool = False
    failed: bool = False


class MassiveKlineScraper:
    NYC = ZoneInfo("America/New_York")
    API_KLINE_MAX_LIMIT = 5000
    API_TICKERS_MAX_LIMIT = 100
    ALLOWED_TICKER_TYPES = frozenset({"CS", "OS", "ADRC", "NYRS"})
    KLINE_SYNC_FRESHNESS_BUFFER_MS = 60_000
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

    @staticmethod
    def _decode_text(value: object) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", "ignore")
        if value is None or pd.isna(value):
            return ""

        text = str(value)
        return "" if text in {"nan", "None"} else text

    @classmethod
    def _missing_text_mask(cls, series: pd.Series) -> pd.Series:
        normalized = series.apply(cls._decode_text)
        return normalized.eq("")

    @classmethod
    def _normalize_text_series(cls, series: pd.Series) -> pd.Series:
        return series.apply(cls._decode_text)

    @staticmethod
    def _to_int(value: object, default: int = 0) -> int:
        try:
            if value is None or pd.isna(value):
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

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

        dedup_df = df.copy()
        dedup_df["_active_rank"] = (
            pd.to_numeric(dedup_df["active"], errors="coerce").fillna(0).astype(int)
        )
        dedup_df = dedup_df.sort_values(
            by=["composite_figi", "_active_rank", "last_updated_utc", "ticker"],
            ascending=[True, False, False, False],
            kind="stable",
        )
        dedup_df = dedup_df.drop_duplicates(subset=["composite_figi"], keep="first")
        return dedup_df.drop(columns=["_active_rank"]).reset_index(drop=True)

    def _load_existing_universe(self) -> pd.DataFrame:
        existing_df = self.repo.get_universe_tickers()
        if existing_df.empty:
            return pd.DataFrame()
        return UsStockUniverseModel.format_dataframe(existing_df)

    def _fetch_full_stock_universe_raw(self) -> pd.DataFrame | None:
        logger.debug("正在通过 ticker 顺序迭代拉取全量 Ticker 列表...")

        active_df = self._fetch_tickers_by_active(active=True)
        if active_df is None:
            return None

        inactive_df = self._fetch_tickers_by_active(active=False)
        if inactive_df is None:
            return None

        universe_parts = [df for df in [active_df, inactive_df] if not df.empty]
        if not universe_parts:
            logger.error("Massive: api 返回为空，等待调度器下次运行")
            return pd.DataFrame()

        return pd.concat(universe_parts, ignore_index=True)

    def _filter_stock_universe(self, universe_raw: pd.DataFrame) -> pd.DataFrame:
        if universe_raw.empty or "type" not in universe_raw.columns:
            return universe_raw.copy()

        filtered_df = universe_raw.copy()
        filtered_df["type"] = self._normalize_text_series(filtered_df["type"]).str.upper()

        before_count = len(filtered_df)
        filtered_df = filtered_df[
            filtered_df["type"].isin(self.ALLOWED_TICKER_TYPES)
        ].copy()

        logger.debug(
            f"✂️ 已过滤为 {sorted(self.ALLOWED_TICKER_TYPES)}，剩余 {len(filtered_df)} / {before_count} 个 Ticker。"
        )
        return filtered_df

    def _inherit_universe_identifiers(
        self, universe_df: pd.DataFrame, existing_df: pd.DataFrame
    ) -> pd.DataFrame:
        if universe_df.empty or existing_df.empty:
            return universe_df

        inherited_df = universe_df.copy()
        existing = existing_df.copy()

        inherited_df["ticker"] = self._normalize_text_series(inherited_df["ticker"])
        inherited_df["composite_figi"] = self._normalize_text_series(
            inherited_df["composite_figi"]
        ).replace({"": np.nan})
        inherited_df["cik"] = self._normalize_text_series(inherited_df["cik"]).replace(
            {"": np.nan}
        )

        existing["ticker"] = self._normalize_text_series(existing["ticker"])
        existing["composite_figi"] = self._normalize_text_series(
            existing["composite_figi"]
        ).replace({"": np.nan})
        existing["cik"] = self._normalize_text_series(existing["cik"]).replace(
            {"": np.nan}
        )

        ticker_to_figi = (
            existing.loc[existing["composite_figi"].notna(), ["ticker", "composite_figi"]]
            .drop_duplicates(subset=["ticker"])
            .set_index("ticker")["composite_figi"]
            .to_dict()
        )
        if ticker_to_figi:
            inherited_df["composite_figi"] = inherited_df["composite_figi"].fillna(
                inherited_df["ticker"].map(ticker_to_figi)
            )

        figi_to_cik = (
            existing.loc[
                existing["cik"].notna() & existing["composite_figi"].notna(),
                ["composite_figi", "cik"],
            ]
            .drop_duplicates(subset=["composite_figi"])
            .set_index("composite_figi")["cik"]
            .to_dict()
        )
        if figi_to_cik:
            inherited_df["cik"] = inherited_df["cik"].fillna(
                inherited_df["composite_figi"].map(figi_to_cik)
            )

        ticker_to_cik = (
            existing.loc[existing["cik"].notna(), ["ticker", "cik"]]
            .drop_duplicates(subset=["ticker"])
            .set_index("ticker")["cik"]
            .to_dict()
        )
        if ticker_to_cik:
            inherited_df["cik"] = inherited_df["cik"].fillna(
                inherited_df["ticker"].map(ticker_to_cik)
            )

        return inherited_df

    def _enrich_missing_universe_identifiers(self, universe_df: pd.DataFrame) -> pd.DataFrame:
        if universe_df.empty:
            return universe_df

        enriched_df = universe_df.copy()
        enriched_df["composite_figi"] = self._normalize_text_series(
            enriched_df["composite_figi"]
        ).replace({"": np.nan})
        enriched_df["cik"] = self._normalize_text_series(enriched_df["cik"]).replace(
            {"": np.nan}
        )

        missing_figi_mask = enriched_df["composite_figi"].isna()
        if missing_figi_mask.any():
            pending_figi_df = enriched_df.loc[missing_figi_mask].copy()
            logger.debug(f"发现 {len(pending_figi_df)} 条记录缺失 FIGI，执行补全...")
            figi_enriched_df = self.enrich_figi(pending_figi_df)
            figi_map = (
                figi_enriched_df.assign(
                    ticker=self._normalize_text_series(figi_enriched_df["ticker"]),
                    composite_figi=self._normalize_text_series(
                        figi_enriched_df["composite_figi"]
                    ),
                )
                .loc[
                    lambda df: df["composite_figi"].ne(""),
                    ["ticker", "composite_figi"],
                ]
                .drop_duplicates(subset=["ticker"])
                .set_index("ticker")["composite_figi"]
                .to_dict()
            )
            if figi_map:
                enriched_df["composite_figi"] = enriched_df["composite_figi"].fillna(
                    enriched_df["ticker"].map(figi_map)
                )

        missing_cik_mask = enriched_df["cik"].isna()
        if missing_cik_mask.any():
            pending_cik_df = enriched_df.loc[missing_cik_mask].copy()
            logger.debug(f"发现 {len(pending_cik_df)} 条记录缺失 CIK，执行补全...")
            cik_enriched_df = self.enrich_cik(pending_cik_df)
            cik_map = (
                cik_enriched_df.assign(
                    ticker=self._normalize_text_series(cik_enriched_df["ticker"]),
                    cik=self._normalize_text_series(cik_enriched_df["cik"]),
                )
                .loc[lambda df: df["cik"].ne(""), ["ticker", "cik"]]
                .drop_duplicates(subset=["ticker"])
                .set_index("ticker")["cik"]
                .to_dict()
            )
            if cik_map:
                enriched_df["cik"] = enriched_df["cik"].fillna(
                    enriched_df["ticker"].map(cik_map)
                )

        return enriched_df

    def _prepare_universe_for_insert(
        self, universe_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, int, int]:
        formatted_df = UsStockUniverseModel.format_dataframe(universe_df)
        final_to_insert = formatted_df.loc[
            ~self._missing_text_mask(formatted_df["composite_figi"])
        ].copy()

        if not final_to_insert.empty:
            before_dedup_count = len(final_to_insert)
            final_to_insert = self._deduplicate_by_figi(final_to_insert)
            dropped_same_figi = before_dedup_count - len(final_to_insert)
            if dropped_same_figi > 0:
                logger.warning(
                    f"发现 {dropped_same_figi} 条 FIGI 重复记录，已按 active/last_updated/ticker 优先级折叠。"
                )
            final_to_insert = UsStockUniverseModel.format_dataframe(final_to_insert)

        missing_figi_count = len(formatted_df) - len(final_to_insert)
        missing_cik_count = (
            int(self._missing_text_mask(final_to_insert["cik"]).sum())
            if not final_to_insert.empty
            else 0
        )
        return final_to_insert, missing_figi_count, missing_cik_count

    def _log_missing_universe_figi(self, universe_df: pd.DataFrame) -> None:
        missing_figi_df = universe_df.loc[
            self._missing_text_mask(universe_df["composite_figi"]),
            ["ticker", "name", "type"],
        ].copy()
        if missing_figi_df.empty:
            return

        sample = missing_figi_df.head(5).to_dict("records")
        logger.warning(
            f"⚠️ 最终仍有 {len(missing_figi_df)} 个 Ticker 缺失 FIGI，将跳过入库。示例：{sample}"
        )

    def _sync_figi_mapping_history(
        self, final_to_insert: pd.DataFrame, existing_df: pd.DataFrame
    ) -> None:
        if final_to_insert.empty:
            return

        is_mapping_empty = self.repo.is_mapping_table_empty()
        if existing_df.empty or is_mapping_empty:
            if is_mapping_empty:
                logger.debug("检测到 FIGI 映射表为空，开始执行存量补全...")
            self.load_all_figi_ticker_mapping(final_to_insert)
            return

        known_tickers = set(self._normalize_text_series(existing_df["ticker"]))
        new_tickers = final_to_insert.loc[
            ~self._normalize_text_series(final_to_insert["ticker"]).isin(known_tickers)
        ].copy()
        if not new_tickers.empty:
            self.load_all_figi_ticker_mapping(new_tickers)

    def load_stock_universe(self) -> None:
        """全量刷新股票宇宙，并在入库前完成 FIGI/CIK 继承、补全和去重。"""
        logger.info("Massive Universe 同步开始。")
        stats = UniverseSyncStats()

        try:
            existing_df = self._load_existing_universe()
            universe_raw = self._fetch_full_stock_universe_raw()
            if universe_raw is None or universe_raw.empty:
                return

            stats.total_raw = len(universe_raw)
            filtered_universe = self._filter_stock_universe(universe_raw)
            stats.total_filtered = len(filtered_universe)
            logger.debug(f"本次宇宙表同步准备更新，共计处理 {stats.total_filtered} 个 Ticker。")

            universe_df = UsStockUniverseModel.format_dataframe(filtered_universe)
            universe_df = self._inherit_universe_identifiers(universe_df, existing_df)
            universe_df = self._enrich_missing_universe_identifiers(universe_df)

            final_to_insert, stats.missing_figi_count, stats.missing_cik_count = (
                self._prepare_universe_for_insert(universe_df)
            )

            if stats.missing_figi_count > 0:
                self._log_missing_universe_figi(universe_df)

            if final_to_insert.empty:
                logger.warning("Massive Universe 本轮没有可入库记录。")
                return

            self.repo.insert_stock_universe(final_to_insert)
            stats.inserted_rows = len(final_to_insert)
            self._sync_figi_mapping_history(final_to_insert, existing_df)

        except Exception as exc:
            logger.error(f"宇宙表同步失败: {exc}", exc_info=True)
        else:
            logger.info(
                f"✅ Massive Universe 本轮完成: 原始={stats.total_raw} 过滤后={stats.total_filtered} "
                f"入库={stats.inserted_rows} 缺失FIGI跳过={stats.missing_figi_count} "
                f"缺失CIK保留={stats.missing_cik_count}"
            )

    def _build_latest_kline_ts_map(self) -> dict[str, int]:
        latest_ts_df = self.repo.get_all_stocks_latest_ts_df_by_group()
        if latest_ts_df.empty:
            return {}

        latest_ts_map: dict[str, int] = {}
        for row in latest_ts_df.itertuples(index=False):
            last_ts = getattr(row, "last_ts", None)
            if pd.isna(last_ts):
                continue

            composite_figi = self._decode_text(getattr(row, "composite_figi", ""))
            if not composite_figi:
                continue

            latest_ts_map[composite_figi] = int(
                pd.to_datetime(last_ts).timestamp() * 1000
            )

        return latest_ts_map

    def _build_kline_sync_task(self, row: object) -> KlineSyncTask:
        return KlineSyncTask(
            ticker=self._decode_text(getattr(row, "ticker", "")),
            composite_figi=self._decode_text(getattr(row, "composite_figi", "")),
            active=self._to_int(getattr(row, "active", 0)),
            sync_state=self._to_int(getattr(row, "sync_state", 0)),
        )

    def _get_cold_start_ms(self) -> int:
        return int(
            datetime.strptime(self.COLD_START_DATE, "%Y-%m-%d")
            .replace(tzinfo=self.NYC)
            .timestamp()
            * 1000
        )

    def _mark_kline_sync_done(self, composite_figi: str) -> None:
        self.repo.update_sync_status(
            "us_minutes_klines", composite_figi, "composite_figi", 1
        )

    def _sync_single_kline_task(
        self,
        task: KlineSyncTask,
        latest_ts_map: dict[str, int],
        now_ms: int,
        cold_start_ms: int,
    ) -> KlineTaskResult:
        if task.sync_state == 1:
            return KlineTaskResult()

        if not task.ticker or not task.composite_figi:
            logger.warning(f"跳过非法 K 线任务: ticker={task.ticker} figi={task.composite_figi}")
            return KlineTaskResult(failed=True)

        try:
            start_ms = latest_ts_map.get(task.composite_figi, cold_start_ms) + 1
            if start_ms >= now_ms - self.KLINE_SYNC_FRESHNESS_BUFFER_MS:
                return KlineTaskResult()

            data_raw = self.massive.get_historical_klines(
                ticker=task.ticker,
                multiplier=self.KLINE_SPAN,
                start=str(start_ms),
                end=str(now_ms),
                adjusted=False,
                limit=self.API_KLINE_MAX_LIMIT,
            )
            if data_raw is None:
                logger.warning(f"⚠️ API failed for {task.ticker}. Skipping.")
                return KlineTaskResult()

            if data_raw.empty:
                if task.active == 0:
                    self._mark_kline_sync_done(task.composite_figi)
                    return KlineTaskResult(marked_done=True)
                return KlineTaskResult()

            clean_df = UsStockMinutesKlineModel.format_dataframe(
                data_raw, task.composite_figi
            )
            self.repo.insert_stock_minutes_klines(clean_df)

            marked_done = False
            if task.active == 0 and len(data_raw) < self.API_KLINE_MAX_LIMIT:
                self._mark_kline_sync_done(task.composite_figi)
                marked_done = True
                logger.debug(
                    f"🏁 {task.ticker} delisted & data exhausted ({len(data_raw)} rows). Marked state=1."
                )

            return KlineTaskResult(
                inserted_rows=len(clean_df),
                marked_done=marked_done,
            )

        except Exception as exc:
            logger.error(f"❌ 处理标的 {task.ticker} 异常: {exc}")
            return KlineTaskResult(failed=True)

    def fetch_klines(self) -> None:
        """批量拉取分钟 K 线增量数据，并对退市标的执行完成态收敛。"""
        logger.info("🚀 启动 K 线增量拉取...")

        tasks_df = self.repo.get_sync_tasks(
            "us_minutes_klines", id_column="composite_figi"
        )
        if tasks_df.empty:
            logger.debug("无 K 线拉取任务。")
            return

        total_tasks = len(tasks_df)
        inserted_rows = 0
        failed_tickers = 0
        marked_done = 0

        latest_ts_map = self._build_latest_kline_ts_map()
        now_ms = int(datetime.now(self.NYC).timestamp() * 1000)
        cold_start_ms = self._get_cold_start_ms()

        for row in tasks_df.itertuples(index=False):
            task = self._build_kline_sync_task(row)
            result = self._sync_single_kline_task(
                task=task,
                latest_ts_map=latest_ts_map,
                now_ms=now_ms,
                cold_start_ms=cold_start_ms,
            )
            inserted_rows += result.inserted_rows
            marked_done += int(result.marked_done)
            failed_tickers += int(result.failed)

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
