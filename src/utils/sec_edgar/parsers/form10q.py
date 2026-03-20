# -*- coding: utf-8 -*-
"""
10-Q 解析器 — 季度报告 (XBRL)
==============================
10-Q 从 2014 起强制内嵌 XBRL 标签, 但原始 filing 为 HTML + 内联 XBRL。
本解析器从 XBRL JSON (data.sec.gov/api/xbrl) 提取结构化财务数据。

提取字段:
  - filer_cik, filer_name, filing_date, period_of_report
  - fiscal_year, fiscal_quarter (从 data.sec.gov companyfacts 推断)
  - revenue, net_income, eps_basic, eps_diluted, total_assets, total_liabilities, stockholders_equity
  - cash_and_equivalents, operating_cash_flow

边界:
  1. companyfacts JSON 聚合了所有历史 XBRL 数据, 不需要解析原始 XML
  2. XBRL tag 命名可能因 taxonomy 版本不同而变化 (us-gaap 2014-2024)
  3. 仅提取 10-Q 级别数据 (form="10-Q")
"""

from typing import Any, Dict, List, Optional
from .base import BaseEdgarParser


# 常用 us-gaap XBRL 标签 → 字段名映射
_XBRL_TAG_MAP: Dict[str, str] = {
    "Revenues": "revenue",
    "RevenueFromContractWithCustomerExcludingAssessedTax": "revenue",
    "SalesRevenueNet": "revenue",
    "NetIncomeLoss": "net_income",
    "EarningsPerShareBasic": "eps_basic",
    "EarningsPerShareDiluted": "eps_diluted",
    "Assets": "total_assets",
    "Liabilities": "total_liabilities",
    "StockholdersEquity": "stockholders_equity",
    "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "stockholders_equity",
    "CashAndCashEquivalentsAtCarryingValue": "cash_and_equivalents",
    "NetCashProvidedByUsedInOperatingActivities": "operating_cash_flow",
}


class Form10QParser(BaseEdgarParser):
    FORM_TYPE = "10-Q"

    def parse(self, xbrl_json: Any, metadata: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        从 data.sec.gov companyfacts JSON 中提取 10-Q 数据。
        xbrl_json: companyfacts JSON dict 或原始 HTML (后者仅提取元数据)
        """
        meta = metadata or {}

        # 如果传入的是 companyfacts JSON
        if isinstance(xbrl_json, dict) and "facts" in xbrl_json:
            return self._parse_companyfacts(xbrl_json, meta)

        # 如果传入的是 HTML 文本, 仅提取元数据
        if isinstance(xbrl_json, str):
            return [{
                "filer_cik": meta.get("filerCik", ""),
                "filing_date": meta.get("filingDate", ""),
                "accession_number": meta.get("accessionNumber", ""),
                "period_of_report": meta.get("reportDate", ""),
                "note": "HTML document - use companyfacts API for structured data",
            }]

        return []

    def _parse_companyfacts(
        self, facts: Dict[str, Any], meta: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """从 companyfacts 中提取 10-Q 级别的 XBRL 数据"""
        cik = meta.get("filerCik", str(facts.get("cik", "")))
        entity_name = facts.get("entityName", "")

        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        if not us_gaap:
            return []

        # 按 filing 聚合: accessionNumber → {field: value}
        filings_map: Dict[str, Dict[str, Any]] = {}

        for xbrl_tag, field_name in _XBRL_TAG_MAP.items():
            tag_data = us_gaap.get(xbrl_tag, {})
            units = tag_data.get("units", {})

            # 遍历所有单位 (USD, USD/shares, shares, ...)
            for unit_entries in units.values():
                for entry in unit_entries:
                    form = entry.get("form", "")
                    if form != "10-Q":
                        continue

                    acc = entry.get("accn", "")
                    if acc not in filings_map:
                        filings_map[acc] = {
                            "filer_cik": cik,
                            "filer_name": entity_name,
                            "accession_number": acc,
                            "filing_date": entry.get("filed", ""),
                            "period_of_report": entry.get("end", ""),
                            "fiscal_year": entry.get("fy", 0),
                            "fiscal_quarter": entry.get("fp", ""),
                        }

                    filings_map[acc][field_name] = entry.get("val", 0)

        return list(filings_map.values())
