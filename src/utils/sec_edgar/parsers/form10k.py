# -*- coding: utf-8 -*-
"""
10-K 解析器 — 年度报告 (XBRL)
==============================
与 10-Q 共享 XBRL 基础设施, 但筛选 form="10-K"。
额外提取年度特有字段 (如 goodwill, intangible assets)。

提取字段:
  - 同 10-Q 所有字段
  - goodwill, intangible_assets_net, long_term_debt
  - total_current_assets, total_current_liabilities

边界:
  1. 10-K 和 10-K/A 均需捕获
  2. 部分外国私人发行人 (FPI) 使用 20-F 代替 10-K
  3. companyfacts 中 form="10-K" 包含 annual 数据
"""

from typing import Any, Dict, List, Optional
from .base import BaseEdgarParser


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
    # 10-K 额外字段
    "Goodwill": "goodwill",
    "IntangibleAssetsNetExcludingGoodwill": "intangible_assets_net",
    "LongTermDebt": "long_term_debt",
    "LongTermDebtNoncurrent": "long_term_debt",
    "AssetsCurrent": "total_current_assets",
    "LiabilitiesCurrent": "total_current_liabilities",
    "CommonStockSharesOutstanding": "shares_outstanding",
}


class Form10KParser(BaseEdgarParser):
    FORM_TYPE = "10-K"

    def parse(self, xbrl_json: Any, metadata: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        从 data.sec.gov companyfacts JSON 中提取 10-K 数据。
        """
        meta = metadata or {}

        if isinstance(xbrl_json, dict) and "facts" in xbrl_json:
            return self._parse_companyfacts(xbrl_json, meta)

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
        cik = meta.get("filerCik", str(facts.get("cik", "")))
        entity_name = facts.get("entityName", "")

        us_gaap = facts.get("facts", {}).get("us-gaap", {})
        if not us_gaap:
            return []

        filings_map: Dict[str, Dict[str, Any]] = {}

        for xbrl_tag, field_name in _XBRL_TAG_MAP.items():
            tag_data = us_gaap.get(xbrl_tag, {})
            units = tag_data.get("units", {})

            for unit_entries in units.values():
                for entry in unit_entries:
                    form = entry.get("form", "")
                    if form not in ("10-K", "10-K/A"):
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
                        }

                    # 不覆盖已有值 (先到的 XBRL tag 优先)
                    if field_name not in filings_map[acc]:
                        filings_map[acc][field_name] = entry.get("val", 0)

        return list(filings_map.values())
