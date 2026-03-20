# -*- coding: utf-8 -*-
"""
SEC SC 13D 大股东申报 Model
============================
5%+ 股东变动申报, 激进投资者信号因子。
"""
from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class SecSC13DModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_sc13d_filings"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS sec_sc13d_filings
        (
            filer_cik               String       COMMENT '申报人 CIK',
            filer_name              String       COMMENT '申报人名称 (基金/个人)',
            composite_figi          String       COMMENT 'PIT 映射后的 FIGI',
            cusip                   String       COMMENT '标的证券 CUSIP',

            filing_date             Date         COMMENT 'SEC 申报日期',
            accession_number        String       COMMENT 'EDGAR Accession Number',
            date_of_event           Nullable(Date) COMMENT '触发事件日期',

            percent_of_class        Float64      COMMENT '持有百分比',
            shares_beneficially_owned UInt64     COMMENT '受益所有权股数',

            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, filer_cik, filing_date)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "filer_cik": {"type": "str"},
        "filer_name": {"type": "str"},
        "composite_figi": {"type": "str", "default": ""},
        "cusip": {"type": "str"},
        "filing_date": {"type": "date"},
        "accession_number": {"type": "str"},
        "date_of_event": {"type": "date"},
        "percent_of_class": {"type": "float64", "default": 0.0},
        "shares_beneficially_owned": {"type": "uint64", "default": 0},
    }

    QUERY_LATEST_FILING_BY_CUSIP_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_sc13d_filings WHERE cusip = '{cusip}'"
    )
    QUERY_GLOBAL_LATEST_FILING_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_sc13d_filings"
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

        int_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if "int" in v["type"]]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("uint64")

        return df[list(cls.SCHEMA_CLEAN.keys())]
