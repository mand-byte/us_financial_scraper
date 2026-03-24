import asyncio
import threading
from typing import Callable, List, Optional

import pandas as pd
import requests

from .massive_wss_client import MassiveWssClient
from src.config.settings import settings
from src.utils.logger import app_logger


# 在数据清晰中，要考虑一个cik应对多个ticker的情况，比如google和Berkshire Hathaway
class MassiveApi:
    REQUEST_TIMEOUT_SECONDS = (5, 30)
    MAX_PAGES_PER_REQUEST = 5000

    def __init__(self):
        self.api_key = settings.api.massive_api_key
        self.base_url = "https://api.massive.com"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

        self.session = requests.Session()

    # 🌟 修复 1: 坚决不用 params: dict = {}
    def request(
        self, method: str, endpoint: str, params: Optional[dict] = None
    ) -> dict:
        if params is None:
            params = {}
        else:
            params = params.copy()  # 拷贝一份，防止污染原字典

        params["apiKey"] = self.api_key

        # 🌟 修复 2: 终结 next_url 拼接痛点
        # 如果传进来的是完整的 url，就不去拼接 base_url 了！
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            url = endpoint
        else:
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        try:
            if method.upper() == "GET":
                response = self.session.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                )
            else:
                response = self.session.request(
                    method,
                    url,
                    headers=self.headers,
                    json=params,
                    timeout=self.REQUEST_TIMEOUT_SECONDS,
                )

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            app_logger.error(f"请求 Massive API 失败: {e}")
            raise

    def _collect_paginated_results(
        self, endpoint: str, params: Optional[dict] = None
    ) -> list[dict]:
        all_results: list[dict] = []
        result_limit: Optional[int] = None
        if params and params.get("limit") is not None:
            try:
                result_limit = max(1, int(params["limit"]))
            except (TypeError, ValueError):
                result_limit = None

        data_ = self.request("GET", endpoint, params)
        pages = 0
        seen_next_urls: set[str] = set()

        while True:
            pages += 1
            all_results.extend(data_.get("results", []))
            # if result_limit is not None and len(all_results) >= result_limit:
            #     # 保护策略：达到本轮 limit 后立即返回，剩余分页留给下次调度
            #     app_logger.debug(
            #         f"Massive API 达到本轮 limit={result_limit}，提前结束分页。"
            #     )
            #     return all_results[:result_limit]
            next_url = data_.get("next_url")
            if not next_url:
                break
            if next_url in seen_next_urls:
                app_logger.warning(
                    f"Massive API next_url 重复，提前停止分页: {next_url}"
                )
                break
            # if pages >= self.MAX_PAGES_PER_REQUEST:
            #     app_logger.warning(
            #         f"Massive API 分页超过上限 {self.MAX_PAGES_PER_REQUEST}，提前停止以防失控。"
            #     )
            #     break
            seen_next_urls.add(next_url)
            data_ = self.request("GET", next_url)

        return all_results

    # 这个逻辑要改，活跃的股票不止1000条。
    def get_all_tickers(
        self,
        ticker_filter_type: Optional[str] = None,
        ticker: Optional[str] = None,
        sort_type: Optional[str] = None,
        active: Optional[bool] = None,
        order: str = "asc",
        limit: int = 100,
    ) -> Optional[pd.DataFrame]:
        """
        :param active: active of the ticker, default is True (API default)
        :param max_pages: maximum pages to fetch
        """
        raw_params = {
            "market": "stocks",
            "limit": limit,
            "order": order,
            "type": "CS",
        }

        if active is not None:
            raw_params["active"] = "true" if active else "false"
        if ticker_filter_type is not None:
            raw_params[ticker_filter_type] = ticker
        if sort_type is not None:
            raw_params["sort"] = sort_type
        endpoint = "/v3/reference/tickers"
        clean_params = {k: v for k, v in raw_params.items() if v is not None}
        result_raw = []
        try:
            data_ = self.request("GET", endpoint, clean_params)
            result_raw.extend(data_.get("results", []))
            return pd.DataFrame(result_raw)

        except Exception as e:
            app_logger.error(f"抓取 Massive 股票列表失败: {e}")
            return None

    def get_historical_klines(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = "minute",
        start: str = "2014-01-01",
        end: str = "2014-01-01",
        limit: int = 5000,
        adjusted: bool = False,
        sort: str = "asc",
    ) -> Optional[pd.DataFrame]:

        endpoint = (
            f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start}/{end}"
        )
        try:
            result_raw = self._collect_paginated_results(
                endpoint,
                {
                    "adjusted": "true" if adjusted else "false",
                    "sort": sort,
                    "limit": limit,
                },
            )
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取历史K线失败 [ticker: {ticker}]: {e}")
            return None

    # published_utc_type支持[published_utc,published_utc.gte,published_utc.gt,published_utc.lte,published_utc.lt]
    def get_stock_news(
        self,
        ticker: Optional[str] = None,
        # 支持参数为published_utc，published_utc.gte，published_utc.gt，published_utc.lte，published_utc.lt
        published_utc_type: str = "published_utc.gte",
        # 类型为string (date-time, date)
        date: str = "2016-6-22",
        order: str = "asc",
        # 官方支持最大1000
        limit: int = 1000,
    ) -> Optional[pd.DataFrame]:
        """🌟 内存防御：改为生成器模式，逐页产出新闻
        ticker: Specify a case-sensitive ticker symbol. For example, AAPL represents Apple Inc.
        published_utc_type: The type of published_utc filter. Possible values include: published_utc, published_utc.gte, published_utc.gt, published_utc.lte, published_utc.lt.
        date: The date of the news. Value must be formatted 'yyyy-mm-dd'.Return results published on, before, or after this date.
        order: The order of the news. Possible values include: asc, desc.
        limit: Limit the number of results returned, default is 10 and max is 1000.
        """
        endpoint = "/v2/reference/news"
        raw_params = {
            "ticker": ticker,
            published_utc_type: date,
            "order": order,
            "sort": "published_utc",
            "limit": limit,
        }
        clean_params = {k: v for k, v in raw_params.items() if v}
        try:
            result_raw = self._collect_paginated_results(endpoint, clean_params)
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取新闻数据失败 [date: {date}]: {e}")
            return None

    # 此接口基本只有改名事件
    def get_ticker_events(self, id: str) -> Optional[dict]:
        """
        id: The ticker symbol or composite FIGI of the company or CUSIP 优先使用 ticker其次cik.
        {"name":"Apple Inc.","composite_figi":"BBG000B9XRY4","cik":"0000320193","events":[{"ticker_change":{"ticker":"AAPL"},"type":"ticker_change","date":"2003-09-10"}]}
        """
        endpoint = f"/vX/reference/tickers/{id}/events"
        try:
            data_ = self.request("GET", endpoint)
            result = data_.get("results", None)
            return result

        except Exception as e:
            app_logger.error(f"抓取ticker 改名 事件失败  [id: {id}]: {e}")
            return None

    # 获取拆合记录
    def get_splits(
        self,
        ticker: Optional[str] = None,
        execution_date: str = "1978-10-25",
        limit: int = 5000,
        sort: str = "execution_date.asc",
    ) -> Optional[pd.DataFrame]:
        endpoint = "/stocks/v1/splits"
        params = {
            "execution_date.gte": execution_date,
            "sort": sort,
            "limit": limit,
        }
        if ticker:
            params["ticker"] = ticker
        try:
            result_raw = self._collect_paginated_results(endpoint, params)
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 splits 数据失败 [ticker: {ticker}] [execution_date: {execution_date}]: {e}"
            )
            return None

    def get_dividends(
        self, ex_dividend_date: str = "2000-01-15", limit: int = 5000
    ) -> Optional[pd.DataFrame]:
        endpoint = "/stocks/v1/dividends"
        params = {
            "ex_dividend_date.gte": ex_dividend_date,
            "sort": "ex_dividend_date.asc",
            "limit": limit,
        }
        try:
            result_raw = self._collect_paginated_results(endpoint, params)
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 dividends 数据失败 [ex_dividend_date: {ex_dividend_date}]: {e}"
            )
            return None

    def get_stock_10k_sections(
        self,
        period_end_gte: str = "2000-01-01",
        limit: int = 1000,
        sort: str = "period_end.asc",
    ) -> Optional[pd.DataFrame]:
        """
        Plain-text content of specific sections from SEC filings. Currently supports the Risk Factors and
        Business sections, providing clean, structured text extracted directly from the filing.
        """
        endpoint = "/stocks/filings/10-K/vX/sections"
        params = {
            "period_end.gte": period_end_gte,
            "limit": limit,
            "sort": sort,
        }
        try:
            result_raw = self._collect_paginated_results(endpoint, params)
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 10-K sections 数据失败 [period_end_gte: {period_end_gte}]: {e}"
            )
            return None

    def get_risk_factors(
        self,
        filing_date_gte: str = "2000-01-01",
        limit: int = 1000,
        sort: str = "filing_date.asc",
    ) -> Optional[pd.DataFrame]:
        """
        Standardized, machine-readable risk factor disclosures from SEC filings.
        filing_date_gte: Search filing_date for values that are greater than or equal to the given value.
        """
        endpoint = "/stocks/filings/vX/risk-factors"
        params = {
            "filing_date.gte": filing_date_gte,
            "limit": limit,
            "sort": sort,
        }
        try:
            result_raw = self._collect_paginated_results(endpoint, params)
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 Risk Factors 数据失败 [filing_date_gte: {filing_date_gte}]: {e}"
            )
            return None

    def get_risk_taxonomy(
        self, limit: int = 999, sort: str = "taxonomy.desc"
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve the taxonomy for risk factors, providing descriptions for each category.
        """
        endpoint = "/stocks/taxonomies/vX/risk-factors"
        params = {
            "limit": limit,
            "sort": sort,
        }
        try:
            result_raw = self._collect_paginated_results(endpoint, params)
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 Risk Taxonomy 数据失败: {e}")
            return None

    def start_wss(self, callback: Callable[[list], None]):
        client_instance = MassiveWssClient(self.onmessage)
        self.client = client_instance

        self.callback = callback
        self.loop = asyncio.new_event_loop()

        # 2. 定义后台线程要干的活
        def _run_loop():
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(client_instance.run_forever())

        # 3. 启动守护线程 (daemon=True 保证主程序退出时它跟着死，不会变僵尸进程)
        self.wss_thread = threading.Thread(target=_run_loop, daemon=True)
        self.wss_thread.start()

    def subscribe(self, channels: List[str]):
        """🌟 线程安全：从主线程把任务投递给后台的 WSS 线程"""
        if self.client and self.loop:
            # 用 run_coroutine_threadsafe 把协程安全地塞进正在跑的 loop 里
            asyncio.run_coroutine_threadsafe(self.client.subscribe(channels), self.loop)

    def stop_wss(self):
        """🌟 优雅停机"""
        if self.client and self.loop:
            self.client.stop()
            self.client = None
            self.callback = None

            # 如果需要，可以等待后台线程安全结束
            if self.wss_thread.is_alive():
                self.wss_thread.join(timeout=2.0)

    def onmessage(self, data):
        """回调拦截器"""
        if self.callback:
            self.callback(data)
