import time
import requests
import pandas as pd
from typing import List, Dict, Optional
from src.utils.logger import app_logger
import os


class OpenFIGIClient:
    """
    OpenFIGI V3 映射工具
    功能：Ticker -> FIGI 批量转换，支持速率控制与 Batch 处理
    """

    URL = "https://api.openfigi.com/v3/mapping"

    def __init__(self):
        self.api_key = os.getenv("OPENFIGI_API_KEY", None)
        # 有 Key: 每6秒25次请求，没 Key: 每分钟25次
        self.wait_time = 6.1 / 25 if self.api_key else 60.1 / 25
        self.batch_size = (
            100 if self.api_key else 10
        )  # V3 限制：有 Key 100个，无 Key 10个

    def fetch_figis(self, tickers: List[str], exch_code: str = "US") -> pd.DataFrame:
        """
        批量获取 FIGI 映射关系
        :param tickers: Ticker 列表
        :param exch_code: 交易所代码 (如 US, HK)
        :return: 包含 ticker, figi, name 的 DataFrame
        """
        results_all = []
        # 1. 自动化分批处理
        for i in range(0, len(tickers), self.batch_size):
            batch = tickers[i : i + self.batch_size]
            jobs = [
                {"idType": "TICKER", "idValue": t, "exchCode": exch_code} for t in batch
            ]

            app_logger.info(f"正在请求 OpenFIGI 映射: {len(batch)} 个 Ticker")

            try:
                response = self._make_request(jobs)
                if response:
                    # 2. 解析返回结果 (OpenFIGI 返回列表嵌套列表)
                    for idx, item in enumerate(response):
                        ticker = batch[idx]
                        data = item.get("data")
                        if data:
                            # 默认取第一个匹配项 (通常是 Primary Listing)
                            primary = data[0]
                            results_all.append(
                                {
                                    "ticker": ticker,
                                    "composite_figi": primary.get("compositeFIGI"),
                                    "name": primary.get("name"),
                                    "exch_code": exch_code,
                                }
                            )
                        else:
                            app_logger.warning(
                                f"Ticker {ticker} 未找到映射数据: {item.get('error')}"
                            )

                # 3. 严格遵循速率限制
                time.sleep(self.wait_time)

            except Exception as e:
                app_logger.error(f"OpenFIGI 请求异常: {e}")
                continue

        return pd.DataFrame(results_all)

    def _make_request(self, jobs: List[Dict]) -> Optional[List]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key

        resp = requests.post(self.URL, json=jobs, headers=headers)

        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            app_logger.error("触发 OpenFIGI 速率限制，请检查 API Key 权限")
            return None
        else:
            app_logger.error(f"OpenFIGI API 错误 [{resp.status_code}]: {resp.text}")
            return None
