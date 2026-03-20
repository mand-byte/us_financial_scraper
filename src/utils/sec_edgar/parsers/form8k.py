# -*- coding: utf-8 -*-
"""
8-K 解析器 — 重大事件公告
=========================
8-K 为自由格式 HTML, 本解析器从 filing index 中提取元数据,
并从 1.01-9.02 的 Item 编号中识别事件类型。

提取字段:
  - filer_cik, filer_name, filing_date, accession_number
  - report_date (报告日期)
  - items (触发的 Item 编号列表, 如 ["1.01", "2.02", "9.01"])
  - item_descriptions (对应 Item 的标准描述)
  - document_text (正文文本摘要, 截取前 5000 字符)

边界:
  1. 8-K 的 items 信息通常在 submission JSON (form 字段旁的 items 字段) 中
  2. 部分 8-K 仅含附件 (如新闻稿 99.1), 正文极短
  3. 8-K/A (修正文件) 包含对原始 8-K 的修正说明
"""

import re
from typing import Any, Dict, List, Optional
from .base import BaseEdgarParser

# 标准 8-K Item 编号 → 描述映射
_ITEM_DESCRIPTIONS: Dict[str, str] = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.02": "Termination of a Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "1.04": "Mine Safety",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of a Direct Financial Obligation",
    "2.04": "Triggering Events That Accelerate or Increase a Direct Financial Obligation",
    "2.05": "Costs Associated with Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule",
    "3.02": "Unregistered Sales of Equity Securities",
    "3.03": "Material Modification to Rights of Security Holders",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure/Appointment of Directors or Principal Officers",
    "5.03": "Amendments to Articles of Incorporation or Bylaws",
    "5.05": "Amendments to the Registrant's Code of Ethics",
    "5.07": "Submission of Matters to a Vote of Security Holders",
    "5.08": "Shareholder Nominations Pursuant to Exchange Act Rule 14a-11",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}


class Form8KParser(BaseEdgarParser):
    FORM_TYPE = "8-K"

    _RE_ITEM = re.compile(r"Item\s+(\d+\.\d{2})", re.IGNORECASE)

    def parse(self, html_text: str, metadata: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        meta = metadata or {}
        filing_date = meta.get("filingDate", "")
        accession = meta.get("accessionNumber", "")
        filer_cik = meta.get("filerCik", "")
        filer_name = meta.get("filerName", "")
        report_date = meta.get("reportDate", filing_date)

        # 去 HTML 标签
        clean_text = re.sub(r"<[^>]+>", " ", html_text)
        clean_text = re.sub(r"\s+", " ", clean_text)

        # 抽取 Item 编号
        item_matches = self._RE_ITEM.findall(clean_text)
        items = sorted(set(item_matches))
        item_descs = [_ITEM_DESCRIPTIONS.get(item, "Unknown") for item in items]

        # 截取正文摘要
        text_summary = clean_text[:5000].strip()

        row: Dict[str, Any] = {
            "filer_cik": filer_cik,
            "filer_name": filer_name,
            "filing_date": filing_date,
            "accession_number": accession,
            "report_date": report_date,
            "items": items,
            "item_descriptions": item_descs,
            "document_text": text_summary,
        }

        return [row]
