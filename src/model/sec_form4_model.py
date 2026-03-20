# -*- coding: utf-8 -*-
"""
SEC Form 4 内幕交易 Model (SEC EDGAR 直接来源)
==============================================
比 OpenInsider 来源更全, 包含:
  - 衍生品交易 (RSU/Option)
  - owner_cik 级别追踪
  - 多 owner 同文档展开
"""
from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class SecForm4Model(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_form4_insider_transactions"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS sec_form4_insider_transactions
        (
            issuer_cik          String       COMMENT '发行人 CIK',
            issuer_ticker       String       COMMENT '申报时使用的 ticker',
            composite_figi      String       COMMENT 'PIT 映射后的 FIGI',

            owner_cik           String       COMMENT '内幕人 CIK (唯一标识)',
            owner_name          String       COMMENT '内幕人姓名',
            is_director         Bool         COMMENT '是否董事',
            is_officer          Bool         COMMENT '是否高管',
            is_ten_percent_owner Bool        COMMENT '是否 10%+ 股东',
            officer_title       String       COMMENT '职位名称',

            filing_date         Date         COMMENT 'SEC 申报日期',
            transaction_date    Date         COMMENT '交易执行日期',
            accession_number    String       COMMENT 'EDGAR Accession Number (唯一标识)',

            transaction_code    String       COMMENT 'P=买入 S=卖出 M=行权 A=授予 G=赠与 ...',
            security_title      String       COMMENT '证券名称 (Common Stock / RSU / Option)',
            is_derivative       Bool         COMMENT '是否衍生品交易',

            shares              Float64      COMMENT '交易股数',
            price_per_share     Float64      COMMENT '每股价格',
            acquired_or_disposed String      COMMENT 'A=获得 D=处置',
            shares_owned_post   Float64      COMMENT '交易后持有股数',

            -- 衍生品专有
            exercise_price      Nullable(Float64) COMMENT '行权价格',
            exercise_date       Nullable(Date)    COMMENT '行权日期',
            expiration_date     Nullable(Date)    COMMENT '到期日期',
            underlying_title    Nullable(String)  COMMENT '底层证券名称',
            underlying_shares   Nullable(Float64) COMMENT '底层证券股数',

            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, owner_cik, filing_date, transaction_date, transaction_code, shares)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "issuer_cik": {"type": "str"},
        "issuer_ticker": {"type": "str"},
        "composite_figi": {"type": "str"},
        "owner_cik": {"type": "str"},
        "owner_name": {"type": "str"},
        "is_director": {"type": "bool", "default": False},
        "is_officer": {"type": "bool", "default": False},
        "is_ten_percent_owner": {"type": "bool", "default": False},
        "officer_title": {"type": "str", "default": ""},
        "filing_date": {"type": "date"},
        "transaction_date": {"type": "date"},
        "accession_number": {"type": "str"},
        "transaction_code": {"type": "str"},
        "security_title": {"type": "str", "default": ""},
        "is_derivative": {"type": "bool", "default": False},
        "shares": {"type": "float64", "default": 0.0},
        "price_per_share": {"type": "float64", "default": 0.0},
        "acquired_or_disposed": {"type": "str", "default": ""},
        "shares_owned_post": {"type": "float64", "default": 0.0},
        "exercise_price": {"type": "float64", "default": None},
        "exercise_date": {"type": "date"},
        "expiration_date": {"type": "date"},
        "underlying_title": {"type": "str", "default": None},
        "underlying_shares": {"type": "float64", "default": None},
    }

    QUERY_LATEST_FILING_BY_ISSUER_CIK_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_form4_insider_transactions WHERE issuer_cik = '{issuer_cik}'"
    )
    QUERY_GLOBAL_LATEST_FILING_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_form4_insider_transactions"
    )

    @classmethod
    def format_dataframe(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        df = df.copy()

        for col, meta in cls.SCHEMA_CLEAN.items():
            if col not in df.columns:
                df[col] = meta.get("default", None)

        date_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

        str_cols = {k: v.get("len") for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "str"}
        for col, length in str_cols.items():
            if col in df.columns:
                df[col] = df[col].astype(str).replace("None", "")
                if length:
                    df[col] = df[col].str.slice(0, length)

        float_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "float64"]
        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        bool_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if v["type"] == "bool"]
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)

        return df[list(cls.SCHEMA_CLEAN.keys())]
