# -*- coding: utf-8 -*-
"""
SEC Form 3/4/5 解析器
=============================
解析 SEC SGML/XML 格式的内幕交易数据。
"""

import re
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.utils.logger import app_logger

NYC = ZoneInfo("America/New_York")


class Form345Parser:
    def __init__(self):
        pass
        
    def _get_zfilled_cik(self, cik: str) -> str:
        return str(cik).zfill(10)

    def _parse_acceptance_datetime(self, content: str) -> Optional[datetime]:
        match = re.search(r"<ACCEPTANCE-DATETIME>(\d+)", content)
        if not match:
            return None
        dt_str = match.group(1)
        dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
        dt = dt.replace(tzinfo=NYC)
        return dt.astimezone(timezone.utc)

    def _extract_xml_from_sgml(self, content: str) -> Optional[ET.Element]:
        start = content.find("<ownershipDocument")
        if start == -1:
            return None
        end = content.find("</ownershipDocument>") + len("</ownershipDocument>")
        xml_content = content[start:end]
        
        from bs4 import BeautifulSoup
        try:
            # 采用 BeautifulSoup 修复可能存在的非法 SGML 实体和残缺标签
            soup = BeautifulSoup(xml_content, "xml")
            fixed_xml = str(soup)
            # 交回给 ElementTree 进行后续提取，保持向下兼容
            return ET.fromstring(fixed_xml)
        except Exception as e:
            app_logger.warning(f"⚠️ XML 修复或解析依然失败: {e}")
            return None

    def _get_text(self, el: ET.Element, tag: str) -> str:
        child = el.find(tag)
        if child is None:
            return ""
        return child.text or ""

    def _get_value_text(self, el: ET.Element, tag: str) -> str:
        child = el.find(f"{tag}/value")
        if child is None:
            child = el.find(tag)
        return child.text if child is not None and child.text else ""

    def _get_float(self, el: ET.Element, tag: str) -> float:
        text = self._get_value_text(el, tag)
        try:
            return float(text)
        except (ValueError, TypeError):
            return 0.0

    def _get_bool(self, el: ET.Element, tag: str) -> bool:
        text = self._get_value_text(el, tag)
        return text == "1"

    def parse_submission(
        self, content: str, accession_number: str, form_type: str
    ) -> List[Dict[str, Any]]:
        """
        解析 SEC 原始 submission 文件内容 (SGML/XML)。
        """
        acceptance_dt = self._parse_acceptance_datetime(content)
        root = self._extract_xml_from_sgml(content)
        if root is None:
            return []
            
        return self._parse_form345_xml(root, accession_number, acceptance_dt, form_type)

    def _parse_form345_xml(
        self,
        root: ET.Element,
        accession_number: str,
        acceptance_dt: Optional[datetime],
        form_type: str,
    ) -> List[Dict[str, Any]]:
        results = []

        issuer = root.find("issuer")
        if issuer is None:
            return results

        issuer_cik = self._get_text(issuer, "issuerCik")
        issuer_ticker = self._get_text(issuer, "issuerTradingSymbol")
        filing_date = self._get_text(root, "periodOfReport")

        owners = root.findall("reportingOwner")
        if not owners:
            return results

        for owner_el in owners:
            owner_id = owner_el.find("reportingOwnerId")
            owner_rel = owner_el.find("reportingOwnerRelationship")

            owner_name = self._get_text(owner_id, "rptOwnerName")
            is_director = self._get_bool(owner_rel, "isDirector")
            is_officer = self._get_bool(owner_rel, "isOfficer")
            is_ten_pct = self._get_bool(owner_rel, "isTenPercentOwner")
            officer_title = self._get_text(owner_rel, "officerTitle")

            base = {
                "issuer_ticker": issuer_ticker,
                "issuer_cik": issuer_cik,
                "reporting_owner_name": owner_name,
                "is_director": is_director,
                "is_officer": is_officer,
                "is_ten_percent_owner": is_ten_pct,
                "officer_title": officer_title,
                "filing_date": filing_date,
                "transaction_date": None,
                "transaction_code": "",
                "security_title": "",
                "is_derivative": False,
                "transaction_shares": 0.0,
                "transaction_price_per_share": 0.0,
                "shares_owned_following_transaction": 0.0,
                "shares_owned": 0.0,
                "ownership_form": "",
                "acceptance_datetime": acceptance_dt,
                "accession_number": accession_number,
                "form_type": form_type,
            }

            nd_table = root.find("nonDerivativeTable")
            if nd_table is not None:
                for txn in nd_table.findall("nonDerivativeTransaction"):
                    row = {**base}
                    row["transaction_date"] = self._get_value_text(
                        txn, "transactionDate"
                    )
                    coding = txn.find("transactionCoding")
                    row["transaction_code"] = self._get_text(coding, "transactionCode")
                    amounts = txn.find("transactionAmounts")
                    row["transaction_shares"] = self._get_float(
                        amounts, "transactionShares"
                    )
                    row["transaction_price_per_share"] = self._get_float(
                        amounts, "transactionPricePerShare"
                    )
                    post = txn.find("postTransactionAmounts")
                    row["shares_owned_following_transaction"] = self._get_float(
                        post, "sharesOwnedFollowingTransaction"
                    )
                    row["security_title"] = self._get_value_text(txn, "securityTitle")
                    own_nat = txn.find("ownershipNature")
                    if own_nat is not None:
                        dio = own_nat.find("directOrIndirectOwnership")
                        if dio is not None:
                            row["ownership_form"] = self._get_value_text(dio, "value")
                    results.append(row)

                for holding in nd_table.findall("nonDerivativeHolding"):
                    row = {**base}
                    row["transaction_date"] = None
                    row["transaction_code"] = "H"
                    post = holding.find("postTransactionAmounts")
                    row["shares_owned"] = self._get_float(
                        post, "sharesOwnedFollowingTransaction"
                    )
                    row["security_title"] = self._get_value_text(
                        holding, "securityTitle"
                    )
                    own_nat = holding.find("ownershipNature")
                    if own_nat is not None:
                        dio = own_nat.find("directOrIndirectOwnership")
                        if dio is not None:
                            row["ownership_form"] = self._get_value_text(dio, "value")
                    results.append(row)

            d_table = root.find("derivativeTable")
            if d_table is not None:
                for txn in d_table.findall("derivativeTransaction"):
                    row = {**base, "is_derivative": True}
                    row["transaction_date"] = self._get_value_text(
                        txn, "transactionDate"
                    )
                    coding = txn.find("transactionCoding")
                    row["transaction_code"] = self._get_text(coding, "transactionCode")
                    amounts = txn.find("transactionAmounts")
                    row["transaction_shares"] = self._get_float(
                        amounts, "transactionShares"
                    )
                    row["transaction_price_per_share"] = self._get_float(
                        amounts, "transactionPricePerShare"
                    )
                    post = txn.find("postTransactionAmounts")
                    row["shares_owned_following_transaction"] = self._get_float(
                        post, "sharesOwnedFollowingTransaction"
                    )
                    row["security_title"] = self._get_value_text(txn, "securityTitle")
                    own_nat = txn.find("ownershipNature")
                    if own_nat is not None:
                        dio = own_nat.find("directOrIndirectOwnership")
                        if dio is not None:
                            row["ownership_form"] = self._get_value_text(dio, "value")
                    results.append(row)

                for holding in d_table.findall("derivativeHolding"):
                    row = {**base, "is_derivative": True}
                    row["transaction_date"] = None
                    row["transaction_code"] = "H"
                    underlying = holding.find("underlyingSecurity")
                    if underlying is not None:
                        row["shares_owned"] = self._get_float(
                            underlying, "underlyingSecurityShares"
                        )
                        row["security_title"] = self._get_value_text(
                            underlying, "underlyingSecurityTitle"
                        )
                    own_nat = holding.find("ownershipNature")
                    if own_nat is not None:
                        dio = own_nat.find("directOrIndirectOwnership")
                        if dio is not None:
                            row["ownership_form"] = self._get_value_text(dio, "value")
                    results.append(row)

        return results
