# -*- coding: utf-8 -*-


from datetime import datetime
from typing import Optional

import pandas as pd
from zoneinfo import ZoneInfo

from src.utils.logger import app_logger


class MarketDataRepo:
    @property
    def db(self):
        from src.dao.clickhouse_manager import get_db_manager
        return get_db_manager()

    def __init__(self) -> None:
        pass

    def _query_df(
        self,
        query: str,
        error_message: str,
        *,
        empty_fallback: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        try:
            return self.db.client.query_df(query)
        except Exception as exc:
            app_logger.error(f"{error_message}: {exc}")
            return pd.DataFrame() if empty_fallback is None else empty_fallback

    def insert_stock_universe(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_stock_universe", df)
        except Exception as e:
            app_logger.error(f"插入股票列表数据失败: {e}")
            raise

    def get_active_tickers(self) -> pd.DataFrame:
        from src.model.us_stock_universe_model import UsStockUniverseModel

        return self._query_df(
            UsStockUniverseModel.QUERY_ACTIVE_TICKERS_SQL,
            "查询活跃股票列表失败",
        )

    def get_delisted_tickers(self) -> pd.DataFrame:
        from src.model.us_stock_universe_model import UsStockUniverseModel

        return self._query_df(
            UsStockUniverseModel.QUERY_DELISTED_TICKERS_SQL,
            "查询退市股票列表失败",
        )

    def get_universe_tickers(self) -> pd.DataFrame:
        from src.model.us_stock_universe_model import UsStockUniverseModel

        return self._query_df(
            UsStockUniverseModel.QUERY_ALL_TICKERS_SQL,
            "查询股票列表失败",
        )

    def get_sync_tasks(
        self, table_name: str, id_column: str = "composite_figi"
    ) -> pd.DataFrame:
        """
        Identify symbols needing sync by joining with a specific _state table.
        table_name: The base table name (e.g., 'us_stock_fundamentals')
        id_column: 'cik' or 'composite_figi'
        """
        state_table = f"{table_name}_state"
        from src.model.us_stock_universe_model import UsStockUniverseModel

        query = UsStockUniverseModel.build_query_sync_tasks_sql(
            state_table=state_table,
            id_column=id_column,
        )
        return self._query_df(
            query,
            f"Query sync tasks failed for {table_name}",
        )

    def insert_stock_minutes_klines(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_minutes_klines", df)
        except Exception as e:
            app_logger.error(f"插入分钟K线数据失败: {e}")
            raise

    def get_all_stocks_latest_ts_df_by_group(self) -> pd.DataFrame:
        from src.model.us_stock_minutes_kline_model import UsStockMinutesKlineModel

        query = UsStockMinutesKlineModel.QUERY_LATEST_TS_BY_GROUP_SQL
        return self._query_df(
            query,
            "查询分钟K线最新分组时间失败",
        )

    def insert_us_stock_figi_ticker_mapping(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_stock_figi_ticker_mapping", df)
        except Exception as e:
            app_logger.error(f"插入figi ticker 映射表失败: {e}")
            raise

    def get_us_stock_figi_ticker_mapping(self, figi: str) -> pd.DataFrame:
        from src.model.us_stock_figi_ticker_mapping_model import (
            UsStockFigiTickerMappingModel,
        )

        query = UsStockFigiTickerMappingModel.build_query_mapping_by_figi_sql(
            figi=figi
        )
        return self._query_df(
            query,
            "查询股票列表数据失败",
        )

    def get_cik_to_figi_mapping(self) -> dict:
        """获取 CIK -> FIGI 映射 (用于 enrichment)"""
        from src.model.us_stock_universe_model import UsStockUniverseModel
        df = self._query_df(
            UsStockUniverseModel.QUERY_CIK_TO_FIGI_MAPPING_SQL,
            "获取 CIK->FIGI 映射失败",
        )
        if df.empty:
            return {}
        return dict(zip(df["cik"].astype(str), df["composite_figi"].astype(str)))

    def get_figi_mapping_by_tickers(self, tickers: list[str]) -> dict:
        if not tickers:
            return {}
        from src.model.us_stock_figi_ticker_mapping_model import (
            UsStockFigiTickerMappingModel,
        )

        query = UsStockFigiTickerMappingModel.build_query_mapping_by_tickers_sql(
            tickers=tickers
        )
        res = self._query_df(query, "查询 tickers 映射失败")
        if res.empty:
            return {}
        return dict(zip(res["ticker"], res["composite_figi"]))

    def get_figi_mapping_history_by_tickers(self, tickers: list[str]) -> pd.DataFrame:
        if not tickers:
            return pd.DataFrame()
        from src.model.us_stock_figi_ticker_mapping_model import (
            UsStockFigiTickerMappingModel,
        )

        query = (
            UsStockFigiTickerMappingModel.build_query_mappings_history_by_tickers_sql(
                tickers=tickers
            )
        )
        return self._query_df(
            query,
            "查询 tickers 历史映射失败",
        )

    def is_mapping_table_empty(self) -> bool:
        """检查 mapping 表是否为空"""
        from src.model.us_stock_figi_ticker_mapping_model import (
            UsStockFigiTickerMappingModel,
        )

        result = self._query_df(
            UsStockFigiTickerMappingModel.QUERY_ROW_COUNT_SQL,
            "查询 mapping 表行数失败",
        )
        if result.empty:
            return True
        return int(result.iloc[0]["row_count"]) == 0

    def insert_benchmark_etf_klines(self, df: pd.DataFrame) -> None:
        try:
            self.db.client.insert_df("us_benchmark_etf_klines", df)
        except Exception as e:
            app_logger.error(f"插入基准ETF K线数据失败: {e}")
            raise

    def get_latest_benchmark_etf_klines(self, ticker: str) -> Optional[datetime]:
        from src.model.us_benchmark_etf_kline_model import BenchmarkEtfKlineModel

        query = BenchmarkEtfKlineModel.build_query_latest_ts_by_ticker_sql(
            ticker=ticker
        )
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]["last_ts"]
            if pd.notna(last_ts):
                dt = pd.to_datetime(last_ts).replace(tzinfo=ZoneInfo("UTC"))
                # ClickHouse max(DateTime) on empty table returns 1970-01-01
                if dt.year > 1970:
                    return dt
            return None
        except Exception as e:
            app_logger.error(f"查询最新基准ETF K线时间戳失败: {e}")
            return None

    def insert_macro_daily_klines(self, df: pd.DataFrame) -> None:
        # 此时传进来的 df 已经是被 Model 洗干净的了
        self.db.client.insert_df("us_macro_daily_klines", df)

    def get_latest_trade_date_in_macro_daily_klines(
        self, symbols: list | dict | str
    ) -> Optional[str]:
        if isinstance(symbols, dict):
            target_symbols = list(symbols.values())
        elif isinstance(symbols, str):
            target_symbols = [symbols]
        elif isinstance(symbols, list):
            target_symbols = symbols
        else:
            raise ValueError("symbols 参数必须是 str, list 或 dict 类型")

        if not target_symbols:
            app_logger.error(f"symbols 参数非法: {symbols}")
            return None

        from src.model.us_macro_daily_kline_model import UsMacroDailyKlineModel

        query = UsMacroDailyKlineModel.build_max_trade_date_query_sql(
            symbols=target_symbols
        )

        try:
            res = self.db.client.query_df(query)

            last_date = res.iloc[0]["ts"]

            if pd.isna(last_date):
                return None

            dt = pd.to_datetime(last_date)
            # 过滤 ClickHouse 空表返回的 1970
            if dt.year > 1970:
                return dt.strftime("%Y-%m-%d")
            return None

        except Exception as e:
            app_logger.error(f"查询最新交易日期失败: {e}")
            return None

    # 用来存已经delisted的个股已经抓取完了用1表示。没有delisted或者没有抓取完就是0。
    def update_sync_status(
        self,
        table_name: str,
        identifier: str,
        id_column: str = "composite_figi",
        state: int = 1,
    ) -> None:
        """
        Update completion state for a specific table.
        Model -> DF -> Repo pattern.
        """
        try:
            from src.model.us_stock_state_model import UsStockStateModel

            state_table = f"{table_name}_state"

            df = UsStockStateModel.format_dataframe(identifier, id_column, state)

            if not df.empty:
                self.db.client.insert_df(state_table, df)

        except Exception as e:
            app_logger.error(
                f"Update sync status failed for {table_name} [{identifier}]: {e}"
            )

    def insert_macro_indicators(self, df: pd.DataFrame) -> None:
        self.db.client.insert_df("us_macro_indicators", df)

    # 兼容历史拼写，避免影响现有调用方。
    def insert_marco_indicators(self, df: pd.DataFrame) -> None:
        self.insert_macro_indicators(df)

    def get_latest_macro_indicators(
        self, indicator_code: list | dict | str
    ) -> Optional[datetime]:
        if isinstance(indicator_code, dict):
            target_codes = list(indicator_code.values())
        elif isinstance(indicator_code, str):
            target_codes = [indicator_code]
        elif isinstance(indicator_code, list):
            target_codes = indicator_code
        else:
            raise ValueError("indicator_code 参数必须是 str, list 或 dict 类型")

        if not target_codes:
            app_logger.error(f"indicator_code 参数非法: {indicator_code}")
            return None
        from src.model.us_macro_indicators_model import UsMacroIndicatorsModel

        query = UsMacroIndicatorsModel.build_max_published_timestamp_query_sql(
            target_codes=target_codes
        )
        try:
            res = self.db.client.query_df(query)
            last_ts = res.iloc[0]["last_ts"]
            if pd.notna(last_ts):
                dt = pd.to_datetime(last_ts).replace(tzinfo=ZoneInfo("UTC"))
                if dt.year > 1970:
                    return dt
            return None
        except Exception as e:
            app_logger.error(f"查询最新宏观指标时间戳失败: {e}")
            return None
