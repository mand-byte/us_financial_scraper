# -*- coding: utf-8 -*-
"""
SEC 13F-HR 机构持仓 Model
==========================
AUM > $100M 机构的季度持仓明细 (逐证券粒度)。
value 单位: 千美元 (API 原始值, 入库后保持原值便于下游换算)。
"""
from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class SecForm13FModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_13f_institutional_holdings"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS sec_13f_institutional_holdings
        (
            filer_cik               String       COMMENT '机构 CIK',
            filer_name              String       COMMENT '机构名称',
            report_period           Date         COMMENT '报告期末 (季度末)',
            filing_date             Date         COMMENT 'SEC 申报日期',
            accession_number        String       COMMENT 'EDGAR Accession Number',

            issuer_name             String       COMMENT '持仓证券发行人名称',
            class_title             String       COMMENT '证券类别 (COM/CL A/PRF/...)',
            cusip                   FixedString(9) COMMENT 'CUSIP (9位)',
            composite_figi          String       COMMENT 'CUSIP -> FIGI 映射',

            value_x1000             Float64      COMMENT '持仓市值 (千美元)',
            shares_or_principal     Float64      COMMENT '持仓股数或本金',
            shares_type             String       COMMENT 'SH=股 PRN=本金',
            put_call                Nullable(String) COMMENT 'PUT/CALL/空',
            investment_discretion   String       COMMENT 'SOLE/SHARED/DEFINED',

            voting_sole             Float64      COMMENT '独立投票权股数',
            voting_shared           Float64      COMMENT '共享投票权股数',
            voting_none             Float64      COMMENT '无投票权股数',

            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, filer_cik, report_period)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "filer_cik": {"type": "str"},
        "filer_name": {"type": "str"},
        "report_period": {"type": "date"},
        "filing_date": {"type": "date"},
        "accession_number": {"type": "str"},
        "issuer_name": {"type": "str"},
        "class_title": {"type": "str", "default": ""},
        "cusip": {"type": "str", "len": 9},
        "composite_figi": {"type": "str", "default": ""},
        "value_x1000": {"type": "float64", "default": 0.0},
        "shares_or_principal": {"type": "float64", "default": 0.0},
        "shares_type": {"type": "str", "default": "SH"},
        "put_call": {"type": "str", "default": ""},
        "investment_discretion": {"type": "str", "default": ""},
        "voting_sole": {"type": "float64", "default": 0.0},
        "voting_shared": {"type": "float64", "default": 0.0},
        "voting_none": {"type": "float64", "default": 0.0},
    }

    QUERY_LATEST_REPORT_PERIOD_BY_FILER_SQL: ClassVar[str] = (
        "SELECT max(report_period) as last_date FROM sec_13f_institutional_holdings WHERE filer_cik = '{filer_cik}'"
    )
    QUERY_GLOBAL_LATEST_FILING_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_13f_institutional_holdings"
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
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")

        return df[list(cls.SCHEMA_CLEAN.keys())]
