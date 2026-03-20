import os
import requests
import pandas as pd
from typing import Optional, Callable, List
from src.utils.logger import app_logger
import asyncio
from .massive_wss_client import MassiveWssClient
import threading


# 在数据清晰中，要考虑一个cik应对多个ticker的情况，比如google和Berkshire Hathaway
class MassiveApi:
    def __init__(self):
        self.api_key = os.getenv("MASSIVE_API_KEY", "")
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
                response = self.session.get(url, headers=self.headers, params=params)
            else:
                response = self.session.request(
                    method, url, headers=self.headers, json=params
                )

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            app_logger.error(f"请求 Massive API 失败: {e}")
            raise e

    # 这个逻辑要改，活跃的股票不止1000条。
    def get_all_tickers(
        self,
        ticker_filter_type: Optional[str] = None,
        ticker: Optional[str] = None,
        type_name: str = "CS",
        sort_type: Optional[str] = None,
        market: str = "stocks",
        active: Optional[bool] = None,
        order: str = "asc",
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        :param ticker_filter_type: ticker/ticker.gte/ticker.gt/ticker.lte/ticker.lt
        :param ticker: ticker symbol
        :param type_name: type of the ticker, default is CS
        :param sort_type: sort type, default is None.(composite_figi, ticker, name, market, type, cik...)
        :param market: market of the ticker, default is stocks
        :param active: active of the ticker, default is True
        :param order: order of the ticker, default is asc
        :param limit: the api return the length of results, max is 1000
        """
        raw_params = {
            "type": type_name,
            "market": market,
            "limit": limit,
        }
        if active is not None:
            raw_params["active"] = "true" if active else "false"

        if ticker_filter_type is not None:
            raw_params[ticker_filter_type] = ticker
        if sort_type is not None:
            raw_params["sort"] = sort_type
        endpoint = "/v3/reference/tickers"
        clean_params = {k: v for k, v in raw_params.items() if v}
        result_raw = []
        try:
            data_ = self.request("GET", endpoint, clean_params)
            while True:
                result_raw.extend(data_.get("results", []))
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)

        except Exception as e:
            app_logger.error(f"抓取 Massive 股票列表失败: {e}")
            return None

    def get_historical_klines(
        self,
        ticker: str,
        multiplier: int = 15,
        timespan: str = "minute",
        start: str = "2014-01-01",
        end: str = "2014-01-01",
        limit: int = 50000,
        adjusted: bool = False,
        sort: str = "asc",
    ):

        endpoint = (
            f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start}/{end}"
        )
        try:
            data_ = self.request(
                "GET",
                endpoint,
                {
                    "adjusted": "true" if adjusted else "false",
                    "sort": sort,
                    "limit": limit,
                },
            )
            result_raw = []
            while True:
                # 立即产出当前页数据，让调用方写入并清理内存
                result_raw.extend(data_.get("results", []))
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取历史K线失败 [ticker: {ticker}]: {e}")
            return None

    # published_utc_type支持[published_utc,published_utc.gte,published_utc.gt,published_utc.lte,published_utc.lt]
    def get_stock_news(
        self,
        ticker: Optional[str] = None,
        # 支持参数为published_utc，published_utc.gte，published_utc.gt，published_utc.lte，published_utc.lt
        published_utc_type: str = "published_utc.gt",
        # 类型为string (date-time, date)
        date: str = "2016-6-22",
        order: str = "asc",
        # 官方支持最大1000
        limit: int = 1000,
    ):
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
            data_ = self.request("GET", endpoint, clean_params)
            result_raw = []
            while True:
                results = data_.get("results", [])
                result_raw.extend(results)
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取新闻数据失败 [date: {date}]: {e}")
            return None

    # 此接口基本只有改名事件
    def get_ticker_events(self, id: str):
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

    def get_balance_sheets(
        self,
        cik: Optional[str] = None,
        timeframe: str = "quarterly",
        limit: int = 50000,
        period_end_tpye: str = "period_end.gt",
        date: str = "2009-03-29",
    ) -> pd.DataFrame:
        """
        获取资产负债表数据 (Balance Sheets)
        cik: The company's Central Index Key (CIK), a unique identifier assigned by the U.S. Securities and Exchange Commission (SEC). You can look up a company's CIK using the SEC CIK Lookup tool.
        timeframe: The reporting period type. Possible values include: quarterly, annual.
        limit: Limit the maximum number of results returned. Defaults to '100' if not specified. The maximum allowed limit is '50000'.
        filing_date: The date when the financial statement was filed with the SEC. Value must be formatted 'yyyy-mm-dd'.
        Retrieve comprehensive balance sheet data for public companies, containing quarterly and annual financial positions. This dataset includes detailed asset, liability, and equity positions representing the company's financial position at specific points in time. Balance sheet data represents point-in-time snapshots rather than cumulative flows, showing what the company owns, owes, and shareholders' equity as of each period end date.
        """
        endpoint = "/stocks/financials/v1/balance-sheets"
        params = {
            "timeframe": timeframe,
            period_end_tpye: date,
            "limit": limit,
            "sort": "period_end.asc",
        }
        if cik is not None:
            params["cik"] = cik

        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))

                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break

            return pd.DataFrame(result_raw)

        except Exception as e:
            app_logger.error(
                f"抓取资产负债表数据失败  [period_end_tpye: {period_end_tpye}] [date: {date}]: {e}"
            )
            return None

    def get_cashflow_statements(
        self,
        cik: Optional[str] = None,
        timeframe: str = "quarterly",
        limit: int = 50000,
        period_end_type: str = "period_end.gt",
        date: str = "2009-03-29",
    ) -> pd.DataFrame:
        endpoint = "/stocks/financials/v1/cash-flow-statements"
        params = {
            period_end_type: date,
            "limit": limit,
            "timeframe": timeframe,
            "sort": "period_end.asc",
        }
        if cik is not None:
            params["cik"] = cik
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break

            return pd.DataFrame(result_raw)

        except Exception as e:
            app_logger.error(
                f"抓取现金流量表数据失败  [period_end_type: {period_end_type}] [date: {date}]: {e}"
            )
            return None

    def get_income_statements(
        self,
        cik: Optional[str] = None,
        timeframe: str = "quarterly",
        limit: int = 50000,
        period_end_type: str = "period_end.gte",
        date: str = "2009-03-29",
    ) -> pd.DataFrame:
        endpoint = "/stocks/financials/v1/income-statements"
        params = {
            period_end_type: date,
            "limit": limit,
            "timeframe": timeframe,
            "sort": "period_end.asc",
        }
        if cik is not None:
            params["cik"] = cik
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break

            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取利润表数据失败 [period_end: {date}]: {e}")
            return None

    #
    def get_ratios(
        self,
        ticker: Optional[str] = None,
        limit: int = 50000,
    ) -> pd.DataFrame:
        """
        Retrieve comprehensive financial ratios data providing key valuation, profitability, liquidity, and leverage metrics for public companies.
        """
        endpoint = "/stocks/financials/v1/ratios"
        params = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break

            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 Ratios 数据失败 {e}")
            return None

    def get_short_interest(
        self,
        ticker: Optional[str] = None,
        settlement_date_type: str = "settlement_date.gte",
        date: str = "2017-11-29",
        limit: int = 50000,
    ) -> pd.DataFrame:
        """
        Retrieve bi-monthly aggregated short interest data reported to FINRA by broker-dealers for a specified stock ticker. Short interest represents the total number of shares sold short but not yet covered or closed out, serving as an indicator of market sentiment and potential price movements. High short interest can signal bearish sentiment or highlight opportunities such as potential short squeezes. This endpoint provides essential insights for investors monitoring market positioning and sentiment.
        ticker: The primary ticker symbol for the stock.
        settlement_date: The date (formatted as YYYY-MM-DD) on which the short interest data is considered settled, typically based on exchange reporting schedules.
        limit: Limit the maximum number of results returned. Defaults to '10' if not specified. The maximum allowed limit is '50000'.
        """
        endpoint = "/stocks/v1/short-interest"
        params = {
            settlement_date_type: date,
            "limit": limit,
            "sort": "settlement_date.asc",
        }
        if ticker:
            params["ticker"] = ticker

        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break

            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 short_interest 数据失败 [ticker: {ticker}]: {e}")
            return None

    def get_short_volume(
        self,
        ticker: Optional[str] = None,
        data_type: str = "date.gte",
        # 最早数据起始点
        date: str = "2024-02-06",
        # 最大返回条数
        limit: int = 50000,
    ) -> pd.DataFrame:
        """
        从场外交易场所和另类交易系统 (ATS) 获取指定股票代码每日汇总的卖空交易量数据，这些数据由美国金融业监管局 (FINRA) 上报。与衡量特定报告周期内未平仓空头头寸的空头持仓量不同，卖空交易量反映的是每日的卖空交易活动
        api返回的result结构体为{
          "adf_short_volume": 0,
          "adf_short_volume_exempt": 0,
          "date": "2025-03-25",
          "exempt_volume": 1,
          "nasdaq_carteret_short_volume": 179943,
          "nasdaq_carteret_short_volume_exempt": 1,
          "nasdaq_chicago_short_volume": 1,
          "nasdaq_chicago_short_volume_exempt": 0,
          "non_exempt_volume": 181218,
          "nyse_short_volume": 1275,
          "nyse_short_volume_exempt": 0,
          "short_volume": 181219,
          "short_volume_ratio": 31.57,
          "ticker": "A",
          "total_volume": 574084
         }
        """
        endpoint = "/stocks/v1/short-volume"
        params = {data_type: date, "limit": limit, "sort": "date.asc"}
        if ticker is not None:
            params["ticker"] = ticker
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break

            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 short_volume 数据失败 [ticker: {ticker}] [date: {date}]: {e}"
            )
            return None

    def get_float(self, limit: int = 5000) -> pd.DataFrame:
        """
        Retrieve the latest free float for a specified stock ticker. Free float represents the shares outstanding that are considered available for public trading, after accounting for shares held by strategic or long-term holders.

        Shares held by founders, officers and directors, employees, controlling shareholders, affiliated companies, private equity and venture capital firms, sovereign wealth funds, government entities, employee plans, trusts, restricted or locked-up holdings, and any shareholder owning 5% or more of total issued shares are not treated as part of the tradable supply. Shares held by pension funds, mutual funds, ETFs, hedge funds without board representation, depositary banks, and other broadly diversified investors are generally treated as part of the public free float.

        Free float reflects a stock’s effective tradable supply and is a key input for assessing liquidity, volatility, market impact, and ownership structure. Figures may reflect ownership changes with a reporting lag, since they are based on publicly available disclosures and ownership information rather than real-time trading activity
                api返回的result结构体为{
                    "effective_date": "2025-11-01",
                    "free_float": 15000000000,
                    "free_float_percent": 98.5,
                    "ticker": "AAPL"
                }
        """
        endpoint = "/stocks/v1/float"
        params = {"limit": limit, "sort": "effective_date.asc"}
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break

            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 float 数据失败: {e}")
            return None

    # 获取拆合记录
    def get_splits(
        self,
        ticker: Optional[str] = None,
        execution_date: str = "1978-10-25",
        limit: int = 5000,
        sort: str = "execution_date.asc",
    ):
        endpoint = "/stocks/v1/splits"
        params = {
            "execution_date.gte": execution_date,
            "sort": sort,
            "limit": limit,
        }
        if ticker:
            params["ticker"] = ticker
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 splits 数据失败 [ticker: {ticker}] [execution_date: {execution_date}]: {e}"
            )
            return None

    def get_dividends(
        self, ex_dividend_date: str = "2000-01-15", limit: int = 5000
    ) -> pd.DataFrame:
        endpoint = "/stocks/v1/dividends"
        params = {
            "ex_dividend_date.gte": ex_dividend_date,
            "sort": "ex_dividend_date.asc",
            "limit": limit,
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 dividends 数据失败 [ex_dividend_date: {ex_dividend_date}]: {e}"
            )
            return None

    def get_stock_10k_sections(
        self,
        period_end: str = "2000-01-01",
        limit: int = 9999,
        sort: str = "period_end.desc",
    ) -> pd.DataFrame:
        """
        Plain-text content of specific sections from SEC filings. Currently supports the Risk Factors and
        Business sections, providing clean, structured text extracted directly from the filing.
        """
        endpoint = "/stocks/filings/10-K/vX/sections"
        params = {
            "limit": limit,
            "sort": sort,
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))

                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 10-K sections 数据失败 [period_end: {period_end}]: {e}"
            )
            return None

    def get_risk_factors(
        self,
        filing_date_gte: str = "2000-01-01",
        limit: int = 49999,
        sort: str = "filing_date.asc",
    ) -> pd.DataFrame:
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
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))

                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(
                f"抓取 Risk Factors 数据失败 [filing_date_gte: {filing_date_gte}]: {e}"
            )
            return None

    def get_risk_taxonomy(
        self, limit: int = 1000, sort: str = "taxonomy.desc"
    ) -> pd.DataFrame:
        """
        Retrieve the taxonomy for risk factors, providing descriptions for each category.
        """
        endpoint = "/stocks/taxonomies/vX/risk-factors"
        params = {
            "limit": limit,
            "sort": sort,
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                result_raw.extend(data_.get("results", []))

                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
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
