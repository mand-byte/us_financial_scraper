# -*- coding: utf-8 -*-
"""
Form 4 解析器 — 内幕交易 (Insider Transactions)
================================================
XML 根节点: <ownershipDocument>
核心表: nonDerivativeTable / derivativeTable

提取字段:
  - issuer_cik, issuer_ticker
  - owner_cik, owner_name, is_director, is_officer, is_ten_percent_owner, officer_title
  - transaction_date, transaction_code (P/S/A/D/M/G/...)
  - shares, price_per_share, acquired_or_disposed (A/D)
  - shares_owned_post_transaction
  - is_derivative (标识来自 derivativeTable)
  - filing_date (from metadata)

边界:
  1. 单个 Form 4 可能包含多条 transaction + 多个 holding
  2. derivativeTable 里的期权/RSU 交易会同时产生 derivativeTransaction 和 derivativeHolding
  3. 部分 Form 4 的 transactionAmounts 可能缺失 (仅 holding report)
"""

from typing import Any, Dict, List, Optional
from .base import BaseEdgarParser


class Form4Parser(BaseEdgarParser):
    FORM_TYPE = "4"

    def parse(self, xml_text: str, metadata: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        root = self.safe_parse_xml(xml_text)
        if root is None:
            return []

        meta = metadata or {}
        filing_date = meta.get("filingDate", "")
        accession = meta.get("accessionNumber", "")

        # Issuer
        issuer = root.find("issuer")
        issuer_cik = self.get_text(issuer, "issuerCik")
        issuer_ticker = self.get_text(issuer, "issuerTradingSymbol")

        # Owner (可能有多个 reportingOwner)
        owners = root.findall("reportingOwner")
        if not owners:
            return []

        results: List[Dict[str, Any]] = []

        for owner_el in owners:
            owner_id = owner_el.find("reportingOwnerId")
            owner_rel = owner_el.find("reportingOwnerRelationship")

            owner_cik = self.get_text(owner_id, "rptOwnerCik")
            owner_name = self.get_text(owner_id, "rptOwnerName")
            is_director = self.get_text(owner_rel, "isDirector") == "1"
            is_officer = self.get_text(owner_rel, "isOfficer") == "1"
            is_ten_pct = self.get_text(owner_rel, "isTenPercentOwner") == "1"
            officer_title = self.get_text(owner_rel, "officerTitle")

            owner_base = {
                "issuer_cik": issuer_cik,
                "issuer_ticker": issuer_ticker,
                "owner_cik": owner_cik,
                "owner_name": owner_name,
                "is_director": is_director,
                "is_officer": is_officer,
                "is_ten_percent_owner": is_ten_pct,
                "officer_title": officer_title,
                "filing_date": filing_date,
                "accession_number": accession,
            }

            # Non-derivative transactions
            nd_table = root.find("nonDerivativeTable")
            if nd_table is not None:
                for txn in nd_table.findall("nonDerivativeTransaction"):
                    row = {**owner_base, "is_derivative": False}
                    row["transaction_date"] = self.get_text(txn, "transactionDate")
                    coding = txn.find("transactionCoding")
                    row["transaction_code"] = self.get_text(coding, "transactionCode")
                    amounts = txn.find("transactionAmounts")
                    row["shares"] = self.get_float(amounts, "transactionShares")
                    row["price_per_share"] = self.get_float(amounts, "transactionPricePerShare")
                    row["acquired_or_disposed"] = self.get_text(amounts, "transactionAcquiredDisposedCode")
                    post = txn.find("postTransactionAmounts")
                    row["shares_owned_post"] = self.get_float(post, "sharesOwnedFollowingTransaction")
                    row["security_title"] = self.get_text(txn, "securityTitle")
                    results.append(row)

                # Non-derivative holdings (no transaction, just position report)
                for holding in nd_table.findall("nonDerivativeHolding"):
                    row = {**owner_base, "is_derivative": False}
                    row["transaction_date"] = ""
                    row["transaction_code"] = "H"  # Holding
                    row["shares"] = 0.0
                    row["price_per_share"] = 0.0
                    row["acquired_or_disposed"] = ""
                    post = holding.find("postTransactionAmounts")
                    row["shares_owned_post"] = self.get_float(post, "sharesOwnedFollowingTransaction")
                    row["security_title"] = self.get_text(holding, "securityTitle")
                    results.append(row)

            # Derivative transactions
            d_table = root.find("derivativeTable")
            if d_table is not None:
                for txn in d_table.findall("derivativeTransaction"):
                    row = {**owner_base, "is_derivative": True}
                    row["transaction_date"] = self.get_text(txn, "transactionDate")
                    coding = txn.find("transactionCoding")
                    row["transaction_code"] = self.get_text(coding, "transactionCode")
                    amounts = txn.find("transactionAmounts")
                    row["shares"] = self.get_float(amounts, "transactionShares")
                    row["price_per_share"] = self.get_float(amounts, "transactionPricePerShare")
                    row["acquired_or_disposed"] = self.get_text(amounts, "transactionAcquiredDisposedCode")
                    post = txn.find("postTransactionAmounts")
                    row["shares_owned_post"] = self.get_float(post, "sharesOwnedFollowingTransaction")
                    row["security_title"] = self.get_text(txn, "securityTitle")
                    # 衍生品特有字段
                    row["exercise_price"] = self.get_float(txn, "conversionOrExercisePrice")
                    row["exercise_date"] = self.get_text(txn, "exerciseDate")
                    row["expiration_date"] = self.get_text(txn, "expirationDate")
                    underlying = txn.find("underlyingSecurity")
                    row["underlying_title"] = self.get_text(underlying, "underlyingSecurityTitle")
                    row["underlying_shares"] = self.get_float(underlying, "underlyingSecurityShares")
                    results.append(row)

        return results
