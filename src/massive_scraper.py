import requests
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
class MassiveDataFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def fetch_klines(self, ticker: str, start_date: str, end_date: str) -> list:
        """分块拉取 5 年 1 分钟 K 线（避免单次请求超限）"""
        all_bars = []
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        final_end = datetime.strptime(end_date, "%Y-%m-%d")
        
        # 按月切分请求，确保单次不超过 50,000 条限制
        while current_start < final_end:
            current_end = current_start + relativedelta(months=1)
            if current_end > final_end:
                current_end = final_end
                
            url = f"{self.base_url}/v2/aggs/ticker/{ticker}/range/15/minute/{current_start.strftime('%Y-%m-%d')}/{current_end.strftime('%Y-%m-%d')}"
            params = {"adjusted": "true", "sort": "asc", "limit": 50000}
            
            response = requests.get(url, headers=self.headers, params=params).json()
            if "results" in response:
                all_bars.extend(response["results"])
                
            current_start = current_end + relativedelta(days=1)
            time.sleep(0.2) # 控制并发避免 429 Too Many Requests
            
        return all_bars

    def fetch_historical_news(self, ticker: str) -> list:
        """使用游标 (Cursor) 拉取个股全量新闻"""
        all_news = []
        url = f"{self.base_url}/v2/reference/news"
        params = {"ticker": ticker, "limit": 1000, "sort": "published_utc", "order": "asc"}
        
        while url:
            response = requests.get(url, headers=self.headers, params=params).json()
            if "results" in response:
                all_news.extend(response["results"])
            
            # 检查是否有下一页游标
            if "next_url" in response:
                url = response["next_url"]
                params = {} # next_url 已经包含了必要的参数和 cursor
                time.sleep(0.2)
            else:
                url = None
                
        return all_news

    def fetch_financial_statements(self, ticker: str, statement_type: str = "income-statements") -> list:
        """
        拉取最新的结构化财报数据
        :param statement_type: 可选 'income-statements', 'balance-sheets', 'cash-flow-statements'
        """
        all_data = []
        # 根据 Massive 最新的生产级端点路径
        url = f"{self.base_url}/stocks/financials/v1/{statement_type}"
        params = {
            "ticker": ticker, 
            "timeframe": "quarterly", 
            "limit": 100
        }
        
        while url:
            response = requests.get(url, headers=self.headers, params=params).json()
            if "results" in response:
                all_data.extend(response["results"])
                
            # 处理游标分页
            if "next_url" in response:
                url = response["next_url"]
                params = {} # next_url 已经包含了必要的翻页信息
                time.sleep(0.2)
            else:
                url = None
                
        return all_data

    def fetch_all_massive_tickers(api_key: str) -> pd.DataFrame:
        """
        抓取全美股市场所有 ticker (含退市)，并提取 CIK 与 FIGI 建立映射底表
        """
        base_url = "https://api.polygon.io/v3/reference/tickers"
        headers = {"Authorization": f"Bearer {api_key}"}
        all_tickers = []

        # 分别抓取存续股 (true) 和 已退市股 (false)
        for is_active in ["true", "false"]:
            print(f"正在抓取 active={is_active} 的股票名录...")
            url = base_url
            params = {
                "market": "stocks",
                "active": is_active,
                "limit": 1000  # 单次最大返回量
            }
            
            while url:
                response = requests.get(url, headers=headers, params=params)
                
                if response.status_code != 200:
                    print(f"请求失败: {response.status_code}, {response.text}")
                    break
                    
                data = response.json()
                
                if "results" in data:
                    for item in data["results"]:
                        all_tickers.append({
                            "ticker": item.get("ticker"),
                            "name": item.get("name"),
                            "cik": str(item.get("cik")).zfill(10) if item.get("cik") else None, # 补齐SEC标准的10位
                            "composite_figi": item.get("share_class_figi"),
                            "active": item.get("active"),
                            "delisted_date": item.get("delisted_date") if item.get("delisted_date") else None
                        })
                
                # 处理游标翻页
                if "next_url" in data:
                    url = data["next_url"]
                    params = {}  # next_url 已包含所有必要参数和 cursor
                    time.sleep(0.2)  # 控制并发，防止触发 429 Too Many Requests
                else:
                    url = None
                    
        # 转为 DataFrame 方便后续去重或存入数据库
        df = pd.DataFrame(all_tickers)
        # 剔除没有 CIK 或 FIGI 的非标准标的（如某些奇葩的 OTC 或 warrant）
        df_clean = df.dropna(subset=['cik', 'composite_figi']).drop_duplicates(subset=['ticker'])
        if 'delisted_date' in df.columns:
            df['delisted_date'] = pd.to_datetime(df['delisted_date']).dt.date
        return df_clean
# 使用示例
# fetcher = MassiveDataFetcher(api_key="YOUR_API_KEY")
# klines = fetcher.fetch_1min_klines("AAPL", "2021-01-01", "2026-01-01")
# news = fetcher.fetch_historical_news("AAPL")
# financials = fetcher.fetch_financials_vx("AAPL")