import time
from typing import List, Dict, Optional

import pandas as pd
import requests

from src.utils.logger import app_logger
from src.config.settings import settings


class OpenFIGIClient:
    """
    OpenFIGI V3 映射工具
    功能：Ticker -> FIGI 批量转换，支持速率控制与 Batch 处理
    """

    URL = "https://api.openfigi.com/v3/mapping"
    REQUEST_TIMEOUT_SECONDS = (5, 30)

    def __init__(self):
        self.api_key = settings.api.openfigi_api_key
        # 有 Key: 每6秒25次请求，没 Key: 每分钟25次
        self.wait_time = 6.1 / 25 if self.api_key else 60.1 / 25
        self.batch_size = (
            100 if self.api_key else 10
        )  # V3 限制：有 Key 100个，无 Key 10个

    def fetch_figis(self, tasks: List[Dict[str, str]]) -> pd.DataFrame:
        """
        批量获取 FIGI 映射关系
        :param tasks: 包含 ticker 和 primary_exchange 的字典列表 [{'ticker': 'AAPL', 'primary_exchange': 'XNAS'}, ...]
        :return: 包含 ticker, figi, name 的 DataFrame
        """
        results_all = []
        # 1. 自动化分批处理
        for i in range(0, len(tasks), self.batch_size):
            batch = tasks[i : i + self.batch_size]
            jobs = []
            for item in batch:
                # 统一使用 exchCode='US' 而不是具体的 micCode (如 XNAS).
                # 经验证实 OpenFIGI 在 micCode 下对某些资产类别(如 B类股/ADR)索引不全，通用 US 码命中率更高。
                job = {
                    "idType": "TICKER",
                    "idValue": item["ticker"],
                    #"exchCode": "US"
                }
                jobs.append(job)

            #app_logger.info(f"正在请求 OpenFIGI 映射: {len(batch)} 个 Ticker")

            try:
                response = self._make_request(jobs)
                if response:
                    # 2. 解析返回结果 (OpenFIGI 返回列表嵌套列表)
                    for idx, item in enumerate(response):
                        task_item = batch[idx]
                        ticker = task_item["ticker"]
                        data = item.get("data")
                        if data:
                            # 默认取第一个匹配项 (通常是 Primary Listing)
                            primary = data[0]
                            results_all.append(
                                {
                                    "ticker": ticker,
                                    "composite_figi": primary.get("compositeFIGI"),
                                    "name": primary.get("name"),
                                    "exch_code": "US",#task_item.get("primary_exchange", "US"),
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

        try:
            resp = requests.post(
                self.URL,
                json=jobs,
                headers=headers,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            app_logger.error(f"OpenFIGI 网络请求失败: {exc}")
            return None

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError as exc:
                app_logger.error(f"OpenFIGI 返回非 JSON 响应: {exc}")
                return None
        elif resp.status_code == 429:
            app_logger.error("触发 OpenFIGI 速率限制，请检查 API Key 权限")
            return None
        else:
            app_logger.error(f"OpenFIGI API 错误 [{resp.status_code}]: {resp.text}")
            return None
