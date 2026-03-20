# -*- coding: utf-8 -*-
"""
SEC 10-Q 季报 XBRL Model
=========================
从 data.sec.gov companyfacts API 提取的标准化季报数据。
与 Massive API 的 income_statements/balance_sheets 互为补充:
  - Massive 按 CIK 拉取, 本表按 companyfacts 全量拉取
  - 本表始终包含 SEC 最权威的原始 XBRL 数值
"""
from src.model.base_clickhouse_model import BaseClickHouseModel
import pandas as pd
from typing import ClassVar, Dict, Any


class SecForm10QModel(BaseClickHouseModel):
    table_name: ClassVar[str] = "sec_form10q_xbrl"

    __DDL__: ClassVar[str] = """
        CREATE TABLE IF NOT EXISTS sec_form10q_xbrl
        (
            filer_cik               String       COMMENT '公司 CIK',
            filer_name              String       COMMENT '公司名称',
            composite_figi          String       COMMENT 'CIK -> FIGI 映射',

            filing_date             Date         COMMENT 'SEC 申报日期',
            period_of_report        Date         COMMENT '报告期末日期',
            accession_number        String       COMMENT 'EDGAR Accession Number',
            fiscal_year             UInt16       COMMENT '财年',
            fiscal_quarter          String       COMMENT '财季 (Q1/Q2/Q3)',

            revenue                 Nullable(Float64) COMMENT '营收',
            net_income              Nullable(Float64) COMMENT '净利润',
            eps_basic               Nullable(Float64) COMMENT '基本每股收益',
            eps_diluted             Nullable(Float64) COMMENT '摊薄每股收益',
            total_assets            Nullable(Float64) COMMENT '总资产',
            total_liabilities       Nullable(Float64) COMMENT '总负债',
            stockholders_equity     Nullable(Float64) COMMENT '股东权益',
            cash_and_equivalents    Nullable(Float64) COMMENT '现金及等价物',
            operating_cash_flow     Nullable(Float64) COMMENT '经营现金流',

            update_time DateTime64(3, 'UTC') DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(update_time)
        ORDER BY (composite_figi, period_of_report, accession_number)
    """

    SCHEMA_CLEAN: ClassVar[Dict[str, Any]] = {
        "filer_cik": {"type": "str"},
        "filer_name": {"type": "str"},
        "composite_figi": {"type": "str", "default": ""},
        "filing_date": {"type": "date"},
        "period_of_report": {"type": "date"},
        "accession_number": {"type": "str"},
        "fiscal_year": {"type": "uint16", "default": 0},
        "fiscal_quarter": {"type": "str", "default": ""},
        "revenue": {"type": "float64"},
        "net_income": {"type": "float64"},
        "eps_basic": {"type": "float64"},
        "eps_diluted": {"type": "float64"},
        "total_assets": {"type": "float64"},
        "total_liabilities": {"type": "float64"},
        "stockholders_equity": {"type": "float64"},
        "cash_and_equivalents": {"type": "float64"},
        "operating_cash_flow": {"type": "float64"},
    }

    QUERY_LATEST_PERIOD_BY_CIK_SQL: ClassVar[str] = (
        "SELECT max(period_of_report) as last_date FROM sec_form10q_xbrl WHERE filer_cik = '{filer_cik}'"
    )
    QUERY_GLOBAL_LATEST_FILING_SQL: ClassVar[str] = (
        "SELECT max(filing_date) as last_date FROM sec_form10q_xbrl"
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

        int_cols = [k for k, v in cls.SCHEMA_CLEAN.items() if "int" in v["type"]]
        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("uint64")

        return df[list(cls.SCHEMA_CLEAN.keys())]
