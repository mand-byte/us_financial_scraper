# -*- coding: utf-8 -*-
"""
SC 13D 解析器 — 激进投资者 / 5%+ 大股东申报
============================================
SC 13D 为非结构化 HTML/SGML, 不像 Form 4 有标准 XML schema。
解析策略: 从 submission index 中提取核心元数据, 并从文本中抽取关键字段。

提取字段:
  - issuer_name, issuer_cik, issuer_ticker (CUSIP)
  - filer_name, filer_cik
  - filing_date, date_of_event
  - percent_of_class
  - shares_beneficially_owned
  - purpose_of_transaction (文本摘要)

边界:
  1. SC 13D 为自由格式 HTML/Text, 不同律所模板差异极大
  2. 精确的持仓数据通常在 Cover Page 的 Item 11-13 中
  3. 修正文件 SC 13D/A 比原始 SC 13D 更常见
"""

import re
from typing import Any, Dict, List, Optional
from .base import BaseEdgarParser


class SC13DParser(BaseEdgarParser):
    FORM_TYPE = "SC 13D"

    # 常见正则模式 — 从 HTML/text 中抽取结构化字段
    # 允许 label 和数值之间有任意修饰词 (represented, is approximately, etc.)
    _RE_PERCENT = re.compile(
        r"(?:percent\s+of\s+class|percentage)[\w\s,:]*?(\d+[\.,]?\d*)\s*%",
        re.IGNORECASE,
    )
    _RE_SHARES = re.compile(
        r"(?:aggregate\s+number|number\s+of\s+shares|shares\s+beneficially\s+owned)[\w\s,:]*?([\d,]+)",
        re.IGNORECASE,
    )
    _RE_CUSIP = re.compile(r"CUSIP\s*(?:No\.?|Number)?[:\s]*([A-Z0-9]{6,9})", re.IGNORECASE)
    _RE_DATE_EVENT = re.compile(
        r"date\s+of\s+event[:\s]*(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )

    def parse(self, html_text: str, metadata: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        meta = metadata or {}
        filing_date = meta.get("filingDate", "")
        accession = meta.get("accessionNumber", "")
        filer_cik = meta.get("filerCik", "")
        filer_name = meta.get("filerName", "")

        # 去 HTML 标签
        clean_text = re.sub(r"<[^>]+>", " ", html_text)
        clean_text = re.sub(r"\s+", " ", clean_text)

        percent_match = self._RE_PERCENT.search(clean_text)
        shares_match = self._RE_SHARES.search(clean_text)
        cusip_match = self._RE_CUSIP.search(clean_text)
        date_event_match = self._RE_DATE_EVENT.search(clean_text)

        percent_of_class = 0.0
        if percent_match:
            try:
                percent_of_class = float(percent_match.group(1).replace(",", "."))
            except ValueError:
                pass

        shares_owned = 0
        if shares_match:
            try:
                shares_owned = int(shares_match.group(1).replace(",", ""))
            except ValueError:
                pass

        cusip = cusip_match.group(1) if cusip_match else ""
        date_of_event = date_event_match.group(1) if date_event_match else ""

        row: Dict[str, Any] = {
            "filer_cik": filer_cik,
            "filer_name": filer_name,
            "cusip": cusip,
            "filing_date": filing_date,
            "accession_number": accession,
            "date_of_event": date_of_event,
            "percent_of_class": percent_of_class,
            "shares_beneficially_owned": shares_owned,
        }

        return [row]
