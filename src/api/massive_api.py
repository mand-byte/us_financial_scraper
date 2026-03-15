import os
import requests
import pandas as pd
from typing import Optional,Callable,List
from src.utils.logger import app_logger
import asyncio
from .massive_wss_client import MassiveWssClient
import threading
class MassiveApi:
    def __init__(self):
        self.api_key = os.getenv("MASSIVE_API_KEY","")
        self.base_url = "https://api.massive.com"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        
        self.session = requests.Session()  
         

    # 🌟 修复 1: 坚决不用 params: dict = {}
    def request(self, method: str, endpoint: str, params: Optional[dict] = None) -> dict:  
        if params is None:
            params = {}
        else:
            params = params.copy() # 拷贝一份，防止污染原字典
            
        params['apiKey'] = self.api_key  

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
                response = self.session.request(method, url, headers=self.headers, json=params)
                
            response.raise_for_status() 
            return response.json() 
            
        except requests.RequestException as e:
            app_logger.error(f"请求 Massive API 失败: {e}")
            raise e
        
    def get_all_tickers(self, type_name: str = "CS", market: str = 'stocks', 
                              active: bool = True, limit: int = 1000) -> pd.DataFrame:
        
        raw_params = {
            "type": type_name,
            "market": market,
            "active": "true" if active else "false",
            "limit": limit
        }
        endpoint = "/v3/reference/tickers"
        clean_params = {k: v for k, v in raw_params.items() if v}
        result_raw = []
        try:
            # 第一次请求
            data_ = self.request("GET", endpoint, clean_params)
            # 🌟 修复 3: 极其优雅的 while True 铺平写法，彻底干掉嵌套函数
            while True:
                # 处理当前页的数据
                for item in data_.get("results", []):
                    cik = item.get("cik")
                    result_raw.append({
                        "ticker": item.get("ticker"),
                        "name": item.get("name"),
                        "cik": str(cik).zfill(10) if cik else None, 
                        "composite_figi": item.get("share_class_figi"),
                        # 映射回你表结构里的 UInt8 (1或0)
                        "active": 1 if item.get("active") else 0, 
                        "delisted_date": item.get("delisted_date")
                    })
                # 检查有没有下一页
                next_url = data_.get("next_url")
                if next_url:
                    # 直接把带 https 的绝对路径扔给 request，它会自己识别
                    data_ = self.request("GET", next_url)
                else:
                    # 没有 next_url，说明抓完了，退出循环
                    break
                    
            return pd.DataFrame(result_raw)    
            
        except Exception as e:
            app_logger.error(f"抓取 Massive 股票列表失败: {e}")
            return pd.DataFrame()

    def get_historical_klines(self, ticker:str, multiplier:int=15,timespan:str="minute", start: str="2014-01-01", 
                           end:str="2014-01-01", limit:int=50000,adjusted:bool=True,sort:str="asc") -> pd.DataFrame:
        endpoint = f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start}/{end}"
        try:
            data_ = self.request("GET", endpoint, {"adjusted": "true" if adjusted else "false", "sort": sort, "limit": limit})
            result_raw=[]
            # 🌟 修复 3: 极其优雅的 while True 铺平写法，彻底干掉嵌套函数
            while True:
                # 处理当前页的数据
                for item in data_.get("results", []):
                    result_raw.append({
                        "open": item.get("o"),
                        "close": item.get("c"),
                        "high": item.get("h"),
                        "low": item.get("l"),
                        "volume": item.get("v"),
                        "trades_count":item.get("n"),# The number of transactions in the aggregate window.
                        "timestamp": item.get("t"), # The Unix millisecond timestamp for the start of the aggregate window.
                        "vwap":item.get("vw")# The volume weighted average price
                    })
                # 检查有没有下一页
                next_url = data_.get("next_url")
                if next_url:
                    # 直接把带 https 的绝对路径扔给 request，它会自己识别
                    data_ = self.request("GET", next_url)
                else:
                    # 没有 next_url，说明抓完了，退出循环
                    break
            return pd.DataFrame(result_raw)  
        except Exception as e:
            app_logger.error(f"抓取 {ticker} 历史K线数据失败: {e}")
            return pd.DataFrame()


    #published_utc_type支持[published_utc,published_utc.gte,published_utc.gt,published_utc.lte,published_utc.lt]
    def get_stock_news(self, ticker:str,published_utc_type:str="published_utc",date:str="2016-6-22",order:str="asc", limit:int=1000)->pd.DataFrame:
        endpoint = f"/v2/reference/news"
        try:
            data_ = self.request("GET", endpoint, {"ticker": ticker, published_utc_type: date, "order": order, "limit": limit})
            result_raw=[]
            # 🌟 修复 3: 极其优雅的 while True 铺平写法，彻底干掉嵌套函数
            while True:
                # 处理当前页的数据
                for item in data_.get("results", []):
                    result_raw.append({
                        "author": item.get("author"),
                        "published_utc": item.get("published_utc"),
                        "article_url": item.get("article_url"),
                        "tickers": ",".join(item.get("tickers", [])),
                        "title": item.get("title"),
                        "description": item.get("description")
                    })
                # 检查有没有下一页
                next_url = data_.get("next_url")
                if next_url:
                    # 直接把带 https 的绝对路径扔给 request，它会自己识别
                    data_ = self.request("GET", next_url)
                else:
                    # 没有 next_url，说明抓完了，退出循环
                    break
            return pd.DataFrame(result_raw)  
        except Exception as e:
            app_logger.error(f"抓取 {ticker} 新闻数据失败: {e}")
            return pd.DataFrame()

    def get_balance_sheets(self, cik: str, timeframe: str = "quarterly", limit: int = 50000, 
                           filing_date: Optional[str] = "2009-03-29") -> pd.DataFrame:
        """
        获取资产负债表数据 (Balance Sheets)
        Endpoint: /stocks/financials/v1/balance-sheets
        """
        endpoint = "/stocks/financials/v1/balance-sheets"
        params = {
            "cik": cik,
            "timeframe": timeframe,
            'filing_date.gte':filing_date,
            "limit": limit,
            "sort": 'period_end.asc'
        }
        
            
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "period_end": item.get("period_end"),
                        "filing_date": item.get("filing_date"),
                        # total_current_assets / total_current_liabilities -> 计算你的 current_ratio (流动比率)
                        'total_current_assets':item.get("total_current_assets"),
                        'total_current_liabilities':item.get("total_current_liabilities"),
                        # total_liabilities / total_equity -> 计算你的 debt_to_equity (债务权益比)
                        'total_liabilities':item.get("total_liabilities"),
                        'total_equity':item.get("total_equity")
                    }    
                    result_raw.append(row)
                
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
                    
            return pd.DataFrame(result_raw)
            
        except Exception as e:
            app_logger.error(f"抓取 {cik} 资产负债表数据失败: {e}")
            return pd.DataFrame()

    def get_cashflow_statements(self,cik: str, timeframe: str = "quarterly", limit: int = 50000, 
                           filing_date: str = "2009-03-29")->pd.DataFrame:
        endpoint = "/stocks/financials/v1/cash-flow-statements"
        params = {
            "cik": cik,
            "filing_date.gtw": filing_date,
            "limit": limit,
            "timeframe":timeframe,
            'sort':'period_end.asc',
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "period_end": item.get("period_end"),
                        "filing_date": item.get("filing_date"),
                        #net_cash_from_operating_activities + purchase_of_property_plant_and_equipment (资本支出) -> 计算得出你的 free_cash_flow (自由现金流)
                        "net_cash_from_operating_activities": item.get("net_cash_from_operating_activities"),
                        "purchase_of_property_plant_and_equipment": item.get("purchase_of_property_plant_and_equipment")
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
                    
            return pd.DataFrame(result_raw)
            
        except Exception as e:
            app_logger.error(f"抓取 {cik} 现金流量表数据失败: {e}")
            return pd.DataFrame()
        
    def get_income_statements(self,cik: str, timeframe: str = "quarterly", limit: int = 50000, 
                           filing_date: str = "2009-03-29")->pd.DataFrame:
        endpoint = "/stocks/financials/v1/income-statements"
        params = {
            "cik": cik,
            "filing_date.gtw": filing_date,
            "limit": limit,
            "timeframe":timeframe,
            'sort':'period_end.asc',
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "period_end": item.get("period_end"),
                        "filing_date": item.get("filing_date"),
                        #basic_earnings_per_share -> 映射为你的 eps
                        "basic_earnings_per_share": item.get("basic_earnings_per_share"),
                        # revenue 和 consolidated_net_income_loss -> 用于计算你的 revenue_growth_yoy 和 net_income_growth_yoy
                        'revenue':item.get("revenue"),
                        'consolidated_net_income_loss':item.get("consolidated_net_income_loss")
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
                    
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 {cik} 利润表数据失败: {e}")
            return pd.DataFrame()

    def get_ratios(self, cik: str,limit: int = 50000, )->pd.DataFrame:
        endpoint = "/stocks/financials/v1/ratios"
        params = {
            "cik": cik,
            "limit": limit
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "date": item.get("date"),
                        "market_cap": item.get("market_cap"),
                        "enterprise_value": item.get("enterprise_value"),
                        "pe_ratio": item.get("pe_ratio"),
                        "pb_ratio": item.get("pb_ratio"),
                        "ps_ratio": item.get("ps_ratio"),
                        "ev_to_ebitda": item.get("ev_to_ebitda"),
                        "dividend_yield": item.get("dividend_yield"),
                        
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
                    
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 {cik} Ratios数据失败: {e}")
            return pd.DataFrame()
    
    def get_short_interest(self, ticker: str,limit: int = 50000, )->pd.DataFrame:
        endpoint = "/stocks/v1/short-interest"
        params = {
            "ticker": ticker,
            "limit": limit
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "short_interest": item.get("short_interest"),
                        "days_to_cover": item.get("days_to_cover")
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
                    
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 {ticker} short_interest 数据失败: {e}")
            return pd.DataFrame()
    
    def get_short_volume(self, ticker: str,date:str='2024-02-06',limit: int = 50000, )->pd.DataFrame:
        endpoint = "/stocks/v1/short-volume"
        params = {
            "ticker": ticker,
            "date":date,
            "limit": limit
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "date": item.get("date"),
                        "short_volume": item.get("short_volume"),
                        "short_volume_ratio": item.get("short_volume_ratio")
                        
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
                    
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 {ticker} short_volume数据失败: {e}")
            return pd.DataFrame()
    
    def get_float(self, ticker: str ,limit: int = 5000)->pd.DataFrame:
        endpoint = "/stocks/v1/short-volume"
        params = {
            "ticker": ticker,
            "limit": limit
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "free_float": item.get("free_float"),
                        "free_float_percent": item.get("free_float_percent")
                        
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
                    
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 {ticker} float 数据失败: {e}")
            return pd.DataFrame()
    #获取拆合记录
    def get_splits(self,ticker:str,execution_date:str="1978-10-25",limit:int=5000,sort:str='execution_date.asc'):
        endpoint = "/stocks/v1/splits"
        params = {
            "ticker": ticker,
            "execution_date":execution_date,
            "sort":sort,
            "limit": limit
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        #Classification of the share-change event. Possible values include: forward_split (share count increases), reverse_split (share count decreases), stock_dividend (shares issued as a dividend)
                        "adjustment_type": item.get("adjustment_type"),
                        "ex_date": item.get("execution_date"),
                        'split_from':item.get("split_from"),
                        'split_to':item.get("split_to")
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 {ticker} splits 数据失败: {e}")
            return pd.DataFrame()
    #获取派息记录
    def get_dividens(self,ticker:str,ex_dividend_date:str="2000-1-15",limit:int=5000)->pd.DataFrame:
        endpoint = "/stocks/v1/dividends"
        params = {
            "ticker": ticker,
            "ex_dividend_date":ex_dividend_date,
            "limit": limit
        }
        try:
            data_ = self.request("GET", endpoint, params)
            result_raw = []
            while True:
                for item in data_.get("results", []):
                    # 基础元数据
                    row = {
                        "date": item.get("ex_dividend_date"),
                        'cash_amount':item.get("cash_amount"),
                        'pay_date':item.get("pay_date"),
                        'declaration_date':item.get("declaration_date"),
                        
                    } 
                    result_raw.append(row)
                # 检查下一页
                next_url = data_.get("next_url")
                if next_url:
                    data_ = self.request("GET", next_url)
                else:
                    break
            return pd.DataFrame(result_raw)
        except Exception as e:
            app_logger.error(f"抓取 {ticker} dividends 数据失败: {e}")
            return pd.DataFrame()
    #获取stock改名记录
    def get_stock_events(self,figi:str):
        endpoint = f"/vX/reference/tickers/{figi}/events"
        data_ = self.request("GET", endpoint)
        try:
            result_raw=[]
            for item in data_.get("results", []):
                result_raw.append({
                    "date": item.get("date"),
                    "ticker":item.get("ticker_change").get("ticker"),
                })
        except Exception as e:
            app_logger.error(f"抓取 {figi} stock_events 数据失败: {e}")
            return pd.DataFrame()


    def start_wss(self,callback:Callable[[list], None]):
        client_instance = MassiveWssClient(self.onmessage)
        self.client=client_instance

        self.callback=callback
        self.loop=asyncio.new_event_loop()
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
            asyncio.run_coroutine_threadsafe(
                self.client.subscribe(channels), 
                self.loop
            )

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
    