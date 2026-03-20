# -*- coding: utf-8 -*-
"""
SEC EDGAR HTTP Client
=====================
- 封装 EDGAR 的两套核心 API：
  1. data.sec.gov   — Submissions JSON (按 CIK 获取申报索引)
  2. efts.sec.gov    — Full-Text Search (按表单类型 + 日期全局搜索)
- 内置限速器 (10 req/s) + 指数退避
- 自动处理 User-Agent 要求 (SEC 强制)

潜在风险:
  1. SEC 会对无 User-Agent 或高频请求返回 403
  2. submissions JSON 最多含最近 ~1000 条，更多需 follow `files` 数组
  3. full-text search 返回的 accession number 需进一步拼接获取原始 XML
"""

import os
import time
import requests
from typing import Optional, List, Dict, Any
from src.utils.logger import app_logger

# SEC 要求格式: "Company Name AdminEmail"
_DEFAULT_UA = os.getenv(
    "SEC_EDGAR_USER_AGENT",
    "QuantSystemBot admin@example.com"
)


class EdgarClient:
    """SEC EDGAR API 统一客户端"""

    BASE_DATA = "https://data.sec.gov"
    BASE_EFTS = "https://efts.sec.gov/LATEST"
    BASE_ARCHIVES = "https://www.sec.gov/Archives/edgar/data"

    # SEC 限速: 10 req/s → 每请求最少间隔 0.12s (留 20% 余量)
    MIN_INTERVAL: float = 0.12
    MAX_RETRIES: int = 3

    def __init__(self, user_agent: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent or _DEFAULT_UA,
            "Accept-Encoding": "gzip, deflate",
        })
        self._last_request_ts: float = 0.0

    # ──────────────────────────────────────────────
    # 限速 + 重试
    # ──────────────────────────────────────────────
    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_ts
        if elapsed < self.MIN_INTERVAL:
            time.sleep(self.MIN_INTERVAL - elapsed)

    def _request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        accept: str = "application/json",
    ) -> requests.Response:
        """带限速 + 指数退避的 GET 请求"""
        for attempt in range(self.MAX_RETRIES):
            self._throttle()
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    headers={"Accept": accept},
                    timeout=30,
                )
                self._last_request_ts = time.time()

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    app_logger.warning(f"SEC 429 限速, 等待 {wait}s...")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp

            except requests.RequestException as e:
                if attempt < self.MAX_RETRIES - 1:
                    wait = 2 ** (attempt + 1)
                    app_logger.warning(f"SEC 请求失败 ({e}), {wait}s 后重试...")
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError(f"SEC 请求超过最大重试次数: {url}")

    # ──────────────────────────────────────────────
    # API 1: data.sec.gov — 按 CIK 获取 submissions
    # ──────────────────────────────────────────────
    def get_submissions(self, cik: str) -> Dict[str, Any]:
        """
        获取某公司全部 submission 索引。
        返回 JSON 包含 filings.recent 和 filings.files (历史分页)。
        """
        cik_padded = str(cik).zfill(10)
        url = f"{self.BASE_DATA}/submissions/CIK{cik_padded}.json"
        resp = self._request(url)
        return resp.json()

    def get_submission_archive(self, filename: str) -> Dict[str, Any]:
        """获取 submissions 的历史分页文件"""
        url = f"{self.BASE_DATA}/submissions/{filename}"
        resp = self._request(url)
        return resp.json()

    # ──────────────────────────────────────────────
    # API 2: efts.sec.gov — Full-Text Search
    # ──────────────────────────────────────────────
    def full_text_search(
        self,
        forms: str,
        date_from: str,
        date_to: str,
        query: str = "*",
        start: int = 0,
        size: int = 100,
    ) -> Dict[str, Any]:
        """
        全文搜索 EDGAR 申报数据。
        forms: "4", "SC 13D", "13F-HR", "8-K", "10-Q", "10-K"
        date_from/date_to: "YYYY-MM-DD"
        """
        url = f"{self.BASE_EFTS}/search-index"
        params = {
            "q": query,
            "forms": forms,
            "dateRange": "custom",
            "startdt": date_from,
            "enddt": date_to,
            "from": start,
            "size": size,
        }
        resp = self._request(url, params=params)
        return resp.json()

    # ──────────────────────────────────────────────
    # 原始文档获取
    # ──────────────────────────────────────────────
    def get_filing_document(
        self,
        cik: str,
        accession_number: str,
        document_name: str,
    ) -> str:
        """
        获取申报原始文档 (XML/HTML)。
        accession_number: "0001234567-24-012345" → 路径化为 "000123456724012345"
        """
        cik_clean = str(int(cik))
        acc_clean = accession_number.replace("-", "")
        url = f"{self.BASE_ARCHIVES}/{cik_clean}/{acc_clean}/{document_name}"
        resp = self._request(url, accept="text/xml")
        return resp.text

    def get_filing_index(self, cik: str, accession_number: str) -> Dict[str, Any]:
        """获取 filing 的索引 JSON (含所有 document 列表)"""
        cik_clean = str(int(cik))
        acc_clean = accession_number.replace("-", "")
        url = f"{self.BASE_ARCHIVES}/{cik_clean}/{acc_clean}/index.json"
        resp = self._request(url)
        return resp.json()

    # ──────────────────────────────────────────────
    # 辅助: 批量获取某 CIK 的特定表单类型的申报记录
    # ──────────────────────────────────────────────
    def get_filings_by_form(
        self,
        cik: str,
        form_type: str,
        filed_after: Optional[str] = None,
    ) -> List[Dict[str, str]]:
        """
        从 submissions JSON 中过滤出指定 form_type 的 filing 列表。
        返回: [{"accessionNumber": ..., "filingDate": ..., "primaryDocument": ...}, ...]
        """
        data = self.get_submissions(cik)
        recent = data.get("filings", {}).get("recent", {})

        forms = recent.get("form", [])
        acc_numbers = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])

        results: List[Dict[str, str]] = []
        for i, f in enumerate(forms):
            if f != form_type:
                continue
            if filed_after and filing_dates[i] < filed_after:
                continue
            results.append({
                "accessionNumber": acc_numbers[i],
                "filingDate": filing_dates[i],
                "primaryDocument": primary_docs[i],
            })

        # 如有更多历史文件 (filings.files)
        extra_files = data.get("filings", {}).get("files", [])
        for file_info in extra_files:
            filename = file_info.get("name", "")
            if not filename:
                continue
            archive_data = self.get_submission_archive(filename)
            a_forms = archive_data.get("form", [])
            a_acc = archive_data.get("accessionNumber", [])
            a_dates = archive_data.get("filingDate", [])
            a_docs = archive_data.get("primaryDocument", [])
            for i, f in enumerate(a_forms):
                if f != form_type:
                    continue
                if filed_after and a_dates[i] < filed_after:
                    continue
                results.append({
                    "accessionNumber": a_acc[i],
                    "filingDate": a_dates[i],
                    "primaryDocument": a_docs[i],
                })

        return results
