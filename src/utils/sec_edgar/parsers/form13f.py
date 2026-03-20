# -*- coding: utf-8 -*-
"""
13F-HR 解析器 — 机构持仓报告
============================
13F-HR 有标准 XML 结构 (informationTable), 从 2013Q3 起格式统一。
根节点: <informationTable> (在 primary doc 的附表 XML 中)

提取字段:
  - issuer_name, issuer_cusip, class_title
  - value (千美元), shares_or_principal, shares_type (SH/PRN)
  - put_call (PUT/CALL/None)
  - investment_discretion (SOLE/SHARED/DEFINED)
  - voting_authority_sole, voting_authority_shared, voting_authority_none
  - filer_cik, filer_name, filing_date, report_period (from metadata)

边界:
  1. 附表 XML 文件名一般为 *infotable.xml 或 primary_doc.xml
  2. value 单位为千美元, 需 * 1000 还原
  3. 某些机构会拆分成多个 informationTable 文件
"""

from typing import Any, Dict, List, Optional
import xml.etree.ElementTree as ET
from .base import BaseEdgarParser


class Form13FParser(BaseEdgarParser):
    FORM_TYPE = "13F-HR"

    # 13F XML 常见 namespace
    _NS = {
        "ns": "http://www.sec.gov/edgar/document/thirteenf/informationtable",
    }

    def parse(self, xml_text: str, metadata: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        meta = metadata or {}
        filing_date = meta.get("filingDate", "")
        accession = meta.get("accessionNumber", "")
        filer_cik = meta.get("filerCik", "")
        filer_name = meta.get("filerName", "")
        report_period = meta.get("reportPeriod", "")

        # 尝试带 namespace 解析
        root = self._try_parse(xml_text)
        if root is None:
            return []

        # 确定是否使用 namespace
        info_entries = root.findall(".//ns:infoTable", self._NS)
        use_ns = bool(info_entries)
        if not use_ns:
            info_entries = root.findall(".//infoTable")

        results: List[Dict[str, Any]] = []
        for entry in info_entries:
            row: Dict[str, Any] = {
                "filer_cik": filer_cik,
                "filer_name": filer_name,
                "filing_date": filing_date,
                "accession_number": accession,
                "report_period": report_period,
            }

            if use_ns:
                row["issuer_name"] = self._ns_text(entry, "nameOfIssuer")
                row["class_title"] = self._ns_text(entry, "titleOfClass")
                row["cusip"] = self._ns_text(entry, "cusip")
                row["value_x1000"] = self._ns_float(entry, "value")
                shrs_el = entry.find("ns:shrsOrPrnAmt", self._NS)
                row["shares_or_principal"] = self._ns_float(shrs_el, "sshPrnamt") if shrs_el is not None else 0
                row["shares_type"] = self._ns_text(shrs_el, "sshPrnamtType") if shrs_el is not None else ""
                row["put_call"] = self._ns_text(entry, "putCall")
                row["investment_discretion"] = self._ns_text(entry, "investmentDiscretion")
                vote_el = entry.find("ns:votingAuthority", self._NS)
                row["voting_sole"] = self._ns_float(vote_el, "Sole") if vote_el is not None else 0
                row["voting_shared"] = self._ns_float(vote_el, "Shared") if vote_el is not None else 0
                row["voting_none"] = self._ns_float(vote_el, "None") if vote_el is not None else 0
            else:
                row["issuer_name"] = self.get_text(entry, "nameOfIssuer")
                row["class_title"] = self.get_text(entry, "titleOfClass")
                row["cusip"] = self.get_text(entry, "cusip")
                row["value_x1000"] = self.get_float(entry, "value")
                shrs_el = entry.find("shrsOrPrnAmt")
                row["shares_or_principal"] = self.get_float(shrs_el, "sshPrnamt") if shrs_el is not None else 0
                row["shares_type"] = self.get_text(shrs_el, "sshPrnamtType") if shrs_el is not None else ""
                row["put_call"] = self.get_text(entry, "putCall")
                row["investment_discretion"] = self.get_text(entry, "investmentDiscretion")
                vote_el = entry.find("votingAuthority")
                row["voting_sole"] = self.get_float(vote_el, "Sole") if vote_el is not None else 0
                row["voting_shared"] = self.get_float(vote_el, "Shared") if vote_el is not None else 0
                row["voting_none"] = self.get_float(vote_el, "None") if vote_el is not None else 0

            results.append(row)

        return results

    def _try_parse(self, xml_text: str) -> Optional[ET.Element]:
        """尝试多种方式解析 13F XML"""
        # 标准解析
        root = self.safe_parse_xml(xml_text)
        if root is not None:
            return root
        # 有些 13F 包装了额外的 XML 声明
        try:
            return ET.fromstring(xml_text.encode("utf-8"))
        except ET.ParseError:
            return None

    def _ns_text(self, el: Optional[ET.Element], tag: str) -> str:
        if el is None:
            return ""
        node = el.find(f"ns:{tag}", self._NS)
        return node.text.strip() if node is not None and node.text else ""

    def _ns_float(self, el: Optional[ET.Element], tag: str) -> float:
        text = self._ns_text(el, tag)
        if not text:
            return 0.0
        try:
            return float(text.replace(",", ""))
        except (ValueError, TypeError):
            return 0.0
